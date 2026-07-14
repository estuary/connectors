package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

// TestMakeDocumentBodies pins down body-parsing behavior the capture
// snapshots can't express precisely. It covers number fidelity through the
// parse-and-respread path and the exact boundary between object and
// raw-string bodies.
func TestMakeDocumentBodies(t *testing.T) {
	const queueURL = "https://sqs.test.example/123456789012/test-queue"
	const meta = `"_meta":{"queueUrl":"` + queueURL + `","messageId":"m-1"}`

	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{
			// Integers beyond 2^53 must survive verbatim. A float64 round
			// trip would emit 1234567890123456800.
			name: "int64-precision",
			body: `{"id": 1234567890123456789}`,
			want: `{` + meta + `,"id":1234567890123456789}`,
		},
		{
			name: "number-literals-preserved",
			body: `{"nested": {"big": 9007199254740993}, "price": 10.10}`,
			want: `{` + meta + `,"nested":{"big":9007199254740993},"price":10.10}`,
		},
		{
			name: "object-with-trailing-whitespace",
			body: `{"a": 1}` + "\n  ",
			want: `{` + meta + `,"a":1}`,
		},
		{
			// Trailing non-whitespace content means the body is not a JSON
			// object, so it must capture whole as a raw string and never as
			// a silently truncated object.
			name: "trailing-content-is-raw",
			body: `{"a": 1} extra`,
			want: `{` + meta + `,"body":"{\"a\": 1} extra"}`,
		},
		{
			name: "concatenated-objects-are-raw",
			body: `{"a":1}{"b":2}`,
			want: `{` + meta + `,"body":"{\"a\":1}{\"b\":2}"}`,
		},
		{
			name: "null-is-raw",
			body: `null`,
			want: `{` + meta + `,"body":"null"}`,
		},
		{
			name: "array-is-raw",
			body: `[1, 2, 3]`,
			want: `{` + meta + `,"body":"[1, 2, 3]"}`,
		},
		{
			name: "spoofed-meta-is-overwritten",
			body: `{"_meta": {"spoofed": true}, "x": 1}`,
			want: `{` + meta + `,"x":1}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := makeDocument(queueURL, types.Message{
				MessageId: aws.String("m-1"),
				Body:      aws.String(tc.body),
			})
			require.NoError(t, err)
			require.Equal(t, tc.want, string(doc))
		})
	}
}

// TestMakeDocumentMessageAttributes pins the message-attribute passthrough:
// the producer's DataType (including custom labels) and value fields reach the
// document verbatim, with binary values base64-encoded by JSON serialization.
func TestMakeDocumentMessageAttributes(t *testing.T) {
	const queueURL = "https://sqs.test.example/123456789012/test-queue"

	doc, err := makeDocument(queueURL, types.Message{
		MessageId: aws.String("m-1"),
		Body:      aws.String(`{"x": 1}`),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"traceId": {
				DataType:    aws.String("String"),
				StringValue: aws.String("abc-123"),
			},
			"cents": {
				DataType:    aws.String("Number.cents"),
				StringValue: aws.String("1099"),
			},
			"payload": {
				DataType:    aws.String("Binary"),
				BinaryValue: []byte{0x01, 0x02, 0xff},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, `{"_meta":{"queueUrl":"`+queueURL+`","messageId":"m-1",`+
		`"messageAttributes":{`+
		`"cents":{"dataType":"Number.cents","stringValue":"1099"},`+
		`"payload":{"dataType":"Binary","binaryValue":"AQL/"},`+
		`"traceId":{"dataType":"String","stringValue":"abc-123"}}},"x":1}`, string(doc))
}
