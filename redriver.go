package redriver

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/private/protocol/json/jsonutil"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/aws/aws-sdk-go/service/lambda/lambdaiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/tidwall/sjson"
)

type Redriver struct {
	queueUrl string
	sqs      sqsiface.SQSAPI
	streams  dynamodbstreamsiface.DynamoDBStreamsAPI
	lambda   lambdaiface.LambdaAPI
}

func New(queueUrl string, sqs sqsiface.SQSAPI, streams dynamodbstreamsiface.DynamoDBStreamsAPI, lambda lambdaiface.LambdaAPI) *Redriver {
	return &Redriver{queueUrl: queueUrl, sqs: sqs, streams: streams, lambda: lambda}
}

func (w *Redriver) Handle(ctx context.Context) error {
	received := 1
	total := 0
	var err error

	for received > 0 {
		received, err = w.receive(ctx)
		if err != nil {
			return err
		}

		total += received
		fmt.Printf("%d %d\n", received, total)
	}

	return nil
}

func (w *Redriver) receive(ctx context.Context) (int, error) {
	receive, err := w.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(10),
		WaitTimeSeconds:     aws.Int64(20),
		QueueUrl:            &w.queueUrl,
	})
	if err != nil {
		return 0, errors.WithStack(err)
	}

	deletes := []*sqs.DeleteMessageBatchRequestEntry{}

	for idx, message := range receive.Messages {
		item := &FailedItem{}
		err = json.Unmarshal([]byte(*message.Body), &item)
		if err != nil {
			return 0, errors.WithStack(err)
		}

		payload, err := w.payloadForItem(ctx, item)
		if err != nil {
			return 0, err
		}

		invoke, err := w.lambda.InvokeWithContext(ctx, &lambda.InvokeInput{
			FunctionName: &item.RequestContext.FunctionArn,
			Payload:      payload,
		})
		if err != nil {
			return 0, errors.WithStack(err)
		}

		if invoke.FunctionError != nil {
			return 0, errors.Errorf("lambda invoke error: %s", *invoke.FunctionError)
		}

		deletes = append(deletes, &sqs.DeleteMessageBatchRequestEntry{
			Id:            aws.String(fmt.Sprintf("idx%d", idx)),
			ReceiptHandle: message.ReceiptHandle,
		})
	}

	if len(deletes) > 0 {
		_, err = w.sqs.DeleteMessageBatchWithContext(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: &w.queueUrl,
			Entries:  deletes,
		})
		if err != nil {
			return 0, errors.WithStack(err)
		}
	}

	return len(receive.Messages), nil
}

func (w *Redriver) payloadForItem(ctx context.Context, item *FailedItem) ([]byte, error) {
	if item.DDBStreamBatchInfo != nil {
		return w.dynamodbPayloadForItem(ctx, item)
	} else if item.RequestPayload != nil {
		return item.RequestPayload, nil
	} else {
		return nil, errors.New("don't know how to deal with this item")
	}
}

func (w *Redriver) dynamodbPayloadForItem(ctx context.Context, item *FailedItem) ([]byte, error) {
	info := item.DDBStreamBatchInfo
	sequenceNumber := info.StartSequenceNumber

	getIterator, err := w.streams.GetShardIteratorWithContext(ctx, &dynamodbstreams.GetShardIteratorInput{
		SequenceNumber:    &sequenceNumber,
		ShardId:           &info.ShardId,
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAtSequenceNumber),
		StreamArn:         &info.StreamArn,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	iterator := getIterator.ShardIterator

	for {
		for sequenceNumber <= info.EndSequenceNumber {
			get, err := w.streams.GetRecordsWithContext(ctx, &dynamodbstreams.GetRecordsInput{
				Limit:         aws.Int64(1),
				ShardIterator: iterator,
			})
			if err != nil {
				return nil, errors.WithStack(err)
			}

			iterator = get.NextShardIterator

			for _, record := range get.Records {
				payload := &dynamodbstreams.GetRecordsOutput{Records: []*dynamodbstreams.Record{record}}
				payloadJson, _ := jsonutil.BuildJSON(payload)
				unix := record.Dynamodb.ApproximateCreationDateTime.Unix()
				payloadJson, err = sjson.SetBytes(payloadJson, "Records.0.Dynamodb.ApproximateCreationDateTime", unix)
				return payloadJson, errors.WithStack(err)
			}
		}
	}
}
