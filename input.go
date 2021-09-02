package redriver

import (
	"encoding/json"
	"time"
)

type FailedItem struct {
	Version         string          `json:"version"`
	Timestamp       time.Time       `json:"timestamp"`
	RequestContext  RequestContext  `json:"requestContext"`
	ResponseContext ResponseContext `json:"responseContext"`

	RequestPayload     json.RawMessage     `json:"requestPayload,omitempty"`
	ResponsePayload    json.RawMessage     `json:"responsePayload,omitempty"`
	DDBStreamBatchInfo *DDBStreamBatchInfo `json:"DDBStreamBatchInfo,omitempty"`
}

type RequestContext struct {
	RequestId              string `json:"requestId"`
	FunctionArn            string `json:"functionArn"`
	Condition              string `json:"condition"`
	ApproximateInvokeCount int    `json:"approximateInvokeCount"`
}

type ResponseContext struct {
	StatusCode      int    `json:"statusCode"`
	ExecutedVersion string `json:"executedVersion"`
	FunctionError   string `json:"functionError"`
}

type DDBStreamBatchInfo struct {
	ShardId                         string    `json:"shardId"`
	StartSequenceNumber             string    `json:"startSequenceNumber"`
	EndSequenceNumber               string    `json:"endSequenceNumber"`
	ApproximateArrivalOfFirstRecord time.Time `json:"approximateArrivalOfFirstRecord"`
	ApproximateArrivalOfLastRecord  time.Time `json:"approximateArrivalOfLastRecord"`
	BatchSize                       int       `json:"batchSize"`
	StreamArn                       string    `json:"streamArn"`
}
