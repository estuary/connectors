package connector

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransactorUnmarshalState(t *testing.T) {
	// Test Standard topic
	standardTransactor := &transactor{
		config: config{TopicType: "standard"},
	}
	err := standardTransactor.UnmarshalState(json.RawMessage(`{}`))
	assert.NoError(t, err)
	assert.NotNil(t, standardTransactor.idempotencyCache)

	// Test FIFO topic
	fifoTransactor := &transactor{
		config: config{TopicType: "fifo"},
	}
	err = fifoTransactor.UnmarshalState(json.RawMessage(`{}`))
	assert.NoError(t, err)
	assert.Nil(t, fifoTransactor.idempotencyCache)
}

func TestGenerateIdempotencyKey(t *testing.T) {
	tr := &transactor{
		config: config{
			IdempotencyKeyTemplate: "<source>:<ns>:<doc_id>:<ts_ms>",
		},
	}

	doc := map[string]interface{}{
		"_meta": map[string]interface{}{
			"source":    "test-source",
			"namespace": "test-namespace",
			"uuid":      "test-uuid",
		},
		"data": "test-data",
	}

	key := tr.generateIdempotencyKey(doc, nil)
	assert.Contains(t, key, "test-source")
	assert.Contains(t, key, "test-namespace")
	assert.Contains(t, key, "test-uuid")
}

func TestExtractPartitionKey(t *testing.T) {
	tr := &transactor{
		config: config{
			PartitionKeyField: "/_meta/partition_key",
		},
	}

	tests := []struct {
		name     string
		doc      map[string]interface{}
		expected string
	}{
		{
			name: "with partition key",
			doc: map[string]interface{}{
				"_meta": map[string]interface{}{
					"partition_key": "test-partition",
				},
			},
			expected: "test-partition",
		},
		{
			name: "fallback to namespace",
			doc: map[string]interface{}{
				"_meta": map[string]interface{}{
					"namespace": "test-namespace",
				},
			},
			expected: "test-namespace",
		},
		{
			name: "fallback to hash",
			doc: map[string]interface{}{
				"data": "test-data",
			},
			expected: "", // Will be a hash, we just check it's not empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tr.extractPartitionKey(tt.doc)
			if tt.expected != "" {
				assert.Equal(t, tt.expected, result)
			} else {
				assert.NotEmpty(t, result)
			}
		})
	}
}

func TestBuildMessageAttributes(t *testing.T) {
	tr := &transactor{}
	binding := &topicBinding{
		identifier: "test-identifier",
		topicName:  "test-topic",
	}

	doc := map[string]interface{}{
		"_meta": map[string]interface{}{
			"op":        "c",
			"namespace": "test-namespace",
			"source":    "test-source",
			"trace_id":  "test-trace-id",
		},
		"data": "test-data",
	}

	attrs := tr.buildMessageAttributes(doc, nil, binding, "test-idempotency-key")

	assert.Equal(t, "c", *attrs["op"].StringValue)
	assert.Equal(t, "test-namespace", *attrs["namespace"].StringValue)
	assert.Equal(t, "test-source", *attrs["source"].StringValue)
	assert.Equal(t, "test-idempotency-key", *attrs["idempotency_key"].StringValue)
	assert.Equal(t, "test-trace-id", *attrs["trace_id"].StringValue)
	assert.Equal(t, "test-identifier", *attrs["identifier"].StringValue)
}

func TestIsDuplicate(t *testing.T) {
	// Test Standard topic with cache
	standardTransactor := &transactor{
		config:           config{TopicType: "standard"},
		idempotencyCache: make(map[string]time.Time),
	}

	// Initially not a duplicate
	assert.False(t, standardTransactor.isDuplicate("test-key"))

	// Add to cache
	standardTransactor.idempotencyCache["test-key"] = time.Now()

	// Now it's a duplicate
	assert.True(t, standardTransactor.isDuplicate("test-key"))

	// Test FIFO topic (no cache)
	fifoTransactor := &transactor{
		config: config{TopicType: "fifo"},
	}

	// Never a duplicate for FIFO
	assert.False(t, fifoTransactor.isDuplicate("test-key"))
}

func TestPublishSingle(t *testing.T) {
	mockClient := &mockSNSClient{}
	tr := &transactor{
		config: config{
			TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
			TopicType: "standard",
		},
		client:           mockClient,
		idempotencyCache: make(map[string]time.Time),
	}

	msg := &messageToPublish{
		binding: &topicBinding{
			identifier: "test-id",
			topicName:  "test-topic",
		},
		data:           []byte(`{"test": "data"}`),
		attributes:     make(map[string]*sns.MessageAttributeValue),
		idempotencyKey: "test-key",
	}

	// Mock successful publish
	mockClient.On("Publish", mock.AnythingOfType("*sns.PublishInput")).Return(
		&sns.PublishOutput{MessageId: aws.String("test-message-id")}, nil)

	err := tr.publishSingle(context.Background(), msg)
	assert.NoError(t, err)

	// Verify the message was added to idempotency cache
	assert.True(t, tr.isDuplicate("test-key"))

	mockClient.AssertExpectations(t)
}

func TestPublishBatch(t *testing.T) {
	mockClient := &mockSNSClient{}
	tr := &transactor{
		config: config{
			TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
			TopicType: "standard",
			BatchSize: 2,
		},
		client:           mockClient,
		idempotencyCache: make(map[string]time.Time),
	}

	messages := []*messageToPublish{
		{
			binding: &topicBinding{
				identifier: "test-id-1",
				topicName:  "test-topic",
			},
			data:           []byte(`{"test": "data1"}`),
			attributes:     make(map[string]*sns.MessageAttributeValue),
			idempotencyKey: "test-key-1",
		},
		{
			binding: &topicBinding{
				identifier: "test-id-2",
				topicName:  "test-topic",
			},
			data:           []byte(`{"test": "data2"}`),
			attributes:     make(map[string]*sns.MessageAttributeValue),
			idempotencyKey: "test-key-2",
		},
	}

	// Mock successful batch publish
	mockClient.On("PublishBatch", mock.AnythingOfType("*sns.PublishBatchInput")).Return(
		&sns.PublishBatchOutput{
			Successful: []*sns.PublishBatchResultEntry{
				{Id: aws.String("msg-0"), MessageId: aws.String("test-message-id-1")},
				{Id: aws.String("msg-1"), MessageId: aws.String("test-message-id-2")},
			},
			Failed: []*sns.BatchResultErrorEntry{},
		}, nil)

	err := tr.publishBatch(context.Background(), messages)
	assert.NoError(t, err)

	// Verify both messages were added to idempotency cache
	assert.True(t, tr.isDuplicate("test-key-1"))
	assert.True(t, tr.isDuplicate("test-key-2"))

	mockClient.AssertExpectations(t)
}

func TestPublishBatchWithFailures(t *testing.T) {
	mockClient := &mockSNSClient{}
	tr := &transactor{
		config: config{
			TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
			TopicType: "standard",
			BatchSize: 2,
		},
		client:           mockClient,
		idempotencyCache: make(map[string]time.Time),
	}

	messages := []*messageToPublish{
		{
			binding: &topicBinding{
				identifier: "test-id-1",
				topicName:  "test-topic",
			},
			data:           []byte(`{"test": "data1"}`),
			attributes:     make(map[string]*sns.MessageAttributeValue),
			idempotencyKey: "test-key-1",
		},
		{
			binding: &topicBinding{
				identifier: "test-id-2",
				topicName:  "test-topic",
			},
			data:           []byte(`{"test": "data2"}`),
			attributes:     make(map[string]*sns.MessageAttributeValue),
			idempotencyKey: "test-key-2",
		},
	}

	// Mock batch publish with partial failure
	mockClient.On("PublishBatch", mock.AnythingOfType("*sns.PublishBatchInput")).Return(
		&sns.PublishBatchOutput{
			Successful: []*sns.PublishBatchResultEntry{
				{Id: aws.String("msg-0"), MessageId: aws.String("test-message-id-1")},
			},
			Failed: []*sns.BatchResultErrorEntry{
				{Id: aws.String("msg-1"), Code: aws.String("InternalError"), Message: aws.String("Internal error")},
			},
		}, nil)

	// Mock individual retry for failed message
	mockClient.On("Publish", mock.AnythingOfType("*sns.PublishInput")).Return(
		&sns.PublishOutput{MessageId: aws.String("test-message-id-2")}, nil)

	err := tr.publishBatch(context.Background(), messages)
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestFIFOMessageAttributes(t *testing.T) {
	tr := &transactor{
		config: config{
			TopicType:         "fifo",
			PartitionKeyField: "/_meta/partition_key",
		},
	}

	doc := map[string]interface{}{
		"_meta": map[string]interface{}{
			"partition_key": "test-partition",
		},
		"data": "test-data",
	}

	partitionKey := tr.extractPartitionKey(doc)
	assert.Equal(t, "test-partition", partitionKey)

	// Test message creation for FIFO
	idempotencyKey := tr.generateIdempotencyKey(doc, nil)
	
	// For FIFO topics, we should have MessageGroupId and MessageDeduplicationId
	messageGroupId := aws.String(partitionKey)
	messageDeduplicationId := aws.String(idempotencyKey)

	assert.Equal(t, "test-partition", *messageGroupId)
	assert.NotEmpty(t, *messageDeduplicationId)
}

func TestDestroy(t *testing.T) {
	tr := &transactor{
		config:           config{TopicType: "standard"},
		idempotencyCache: make(map[string]time.Time),
	}

	// Add some data to cache
	tr.idempotencyCache["test-key"] = time.Now()
	assert.NotNil(t, tr.idempotencyCache)

	// Destroy should clean up
	tr.Destroy()
	assert.Nil(t, tr.idempotencyCache)
}

