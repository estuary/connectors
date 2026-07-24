package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/segmentio/encoding/json"
)

type documentMetadata struct {
	QueueURL                string                              `json:"queueUrl"`
	MessageID               string                              `json:"messageId"`
	SentTimestamp           string                              `json:"sentTimestamp,omitempty"`
	ApproximateReceiveCount int                                 `json:"approximateReceiveCount,omitempty"`
	MessageAttributes       map[string]documentMessageAttribute `json:"messageAttributes,omitempty"`
	MessageGroupID          string                              `json:"messageGroupId,omitempty"`
	SequenceNumber          string                              `json:"sequenceNumber,omitempty"`
	DeduplicationID         string                              `json:"deduplicationId,omitempty"`
}

// documentMessageAttribute mirrors the SQS message-attribute structure.
type documentMessageAttribute struct {
	DataType    *string `json:"dataType,omitempty"`
	StringValue *string `json:"stringValue,omitempty"`
	BinaryValue []byte  `json:"binaryValue,omitempty"`
}

// makeDocument converts an SQS message into a Flow document. A JSON-object
// body has its fields spread into the document root. Any other body (JSON
// scalar, JSON array, or non-JSON) is emitted as {"body": "<raw string>"}.
// The connector's _meta is written last and always wins over any _meta field
// the body happens to carry.
func makeDocument(queueURL string, m types.Message) (json.RawMessage, error) {
	body := aws.ToString(m.Body)
	doc, ok := parseObjectBody(body)
	if !ok {
		// SQS bodies are arbitrary text, and only a body that is exactly one
		// JSON object can be spread into the document root without loss.
		// Everything else is captured verbatim under "body".
		doc = map[string]any{"body": body}
	}

	meta := documentMetadata{
		QueueURL:  queueURL,
		MessageID: aws.ToString(m.MessageId),

		MessageGroupID:  m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)],
		SequenceNumber:  m.Attributes[string(types.MessageSystemAttributeNameSequenceNumber)],
		DeduplicationID: m.Attributes[string(types.MessageSystemAttributeNameMessageDeduplicationId)],
	}
	if ms, err := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64); err == nil {
		meta.SentTimestamp = time.UnixMilli(ms).UTC().Format(time.RFC3339Nano)
	}
	if n, err := strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]); err == nil {
		meta.ApproximateReceiveCount = n
	}
	if len(m.MessageAttributes) > 0 {
		meta.MessageAttributes = make(map[string]documentMessageAttribute, len(m.MessageAttributes))
		for name, attr := range m.MessageAttributes {
			meta.MessageAttributes[name] = documentMessageAttribute{
				DataType:    attr.DataType,
				StringValue: attr.StringValue,
				BinaryValue: attr.BinaryValue,
			}
		}
	}
	doc["_meta"] = meta

	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("serializing document for message %q: %w", aws.ToString(m.MessageId), err)
	}
	return out, nil
}

// parseObjectBody decodes a JSON-object body for spreading into the document
// root. It parses with UseNumber because a plain Unmarshal round-trips every
// number through float64, silently corrupting integers beyond 2^53 like
// snowflake IDs and nanosecond timestamps. UseNumber keeps each number's
// literal text intact through re-serialization. The leftover check
// replicates Unmarshal's trailing-content strictness - Parse consumes
// trailing whitespace and makes a body like `{"a":1} extra` capture
// as a raw string rather than a truncated object.
func parseObjectBody(body string) (map[string]any, bool) {
	var doc map[string]any
	leftover, err := json.Parse([]byte(body), &doc, json.UseNumber)
	if err != nil || doc == nil || len(leftover) != 0 {
		return nil, false
	}
	return doc, true
}
