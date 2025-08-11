package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests require LocalStack to be running
// These tests will be skipped if LOCALSTACK_ENDPOINT is not set

func getLocalStackEndpoint() string {
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}
	return endpoint
}

func isLocalStackAvailable() bool {
	return os.Getenv("SKIP_INTEGRATION_TESTS") != "true"
}

func createLocalStackSession(t *testing.T) *session.Session {
	if !isLocalStackAvailable() {
		t.Skip("LocalStack integration tests are disabled")
	}

	endpoint := getLocalStackEndpoint()
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      aws.NewStaticCredentials("test", "test", ""),
	})
	require.NoError(t, err)
	return sess
}

func TestIntegrationStandardTopic(t *testing.T) {
	sess := createLocalStackSession(t)
	snsClient := sns.New(sess)
	
	// Create a standard topic
	topicName := fmt.Sprintf("test-standard-topic-%d", time.Now().Unix())
	createTopicOutput, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := *createTopicOutput.TopicArn

	// Clean up after test
	defer func() {
		snsClient.DeleteTopic(&sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	}()

	// Test configuration
	cfg := config{
		AWSRegion: "us-east-1",
		TopicARN:  topicARN,
		TopicType: "standard",
		BatchSize: 1,
	}

	// Create transactor
	tr := &transactor{
		config: cfg,
		client: snsClient,
		bindings: []*topicBinding{
			{
				identifier: "test-binding",
				topicName:  topicName,
			},
		},
		idempotencyCache: make(map[string]time.Time),
	}

	// Test publishing a message
	testDoc := map[string]interface{}{
		"_meta": map[string]interface{}{
			"op":        "c",
			"namespace": "test.collection",
			"source":    "test-source",
			"uuid":      "test-uuid-123",
		},
		"id":   1,
		"name": "test-record",
	}

	docBytes, err := json.Marshal(testDoc)
	require.NoError(t, err)

	// Create a mock store iterator
	ctx := context.Background()
	
	// Test message publishing
	msg := &messageToPublish{
		binding:        tr.bindings[0],
		data:          docBytes,
		attributes:    tr.buildMessageAttributes(testDoc, nil, tr.bindings[0], "test-key"),
		idempotencyKey: "test-key",
	}

	err = tr.publishSingle(ctx, msg)
	assert.NoError(t, err)

	// Verify idempotency cache
	assert.True(t, tr.isDuplicate("test-key"))
}

func TestIntegrationFIFOTopic(t *testing.T) {
	sess := createLocalStackSession(t)
	snsClient := sns.New(sess)
	
	// Create a FIFO topic
	topicName := fmt.Sprintf("test-fifo-topic-%d.fifo", time.Now().Unix())
	createTopicOutput, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicName),
		Attributes: map[string]*string{
			"FifoTopic": aws.String("true"),
		},
	})
	require.NoError(t, err)
	topicARN := *createTopicOutput.TopicArn

	// Clean up after test
	defer func() {
		snsClient.DeleteTopic(&sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	}()

	// Test configuration
	cfg := config{
		AWSRegion: "us-east-1",
		TopicARN:  topicARN,
		TopicType: "fifo",
		BatchSize: 1,
	}

	// Create transactor
	tr := &transactor{
		config: cfg,
		client: snsClient,
		bindings: []*topicBinding{
			{
				identifier: "test-binding",
				topicName:  topicName,
			},
		},
	}

	// Test publishing a message with FIFO attributes
	testDoc := map[string]interface{}{
		"_meta": map[string]interface{}{
			"op":            "c",
			"namespace":     "test.collection",
			"source":        "test-source",
			"uuid":          "test-uuid-123",
			"partition_key": "partition-1",
		},
		"id":   1,
		"name": "test-record",
	}

	docBytes, err := json.Marshal(testDoc)
	require.NoError(t, err)

	// Generate FIFO-specific attributes
	idempotencyKey := tr.generateIdempotencyKey(testDoc, nil)
	partitionKey := tr.extractPartitionKey(testDoc)

	msg := &messageToPublish{
		binding:                tr.bindings[0],
		data:                  docBytes,
		attributes:            tr.buildMessageAttributes(testDoc, nil, tr.bindings[0], idempotencyKey),
		messageGroupId:        aws.String(partitionKey),
		messageDeduplicationId: aws.String(idempotencyKey),
		idempotencyKey:        idempotencyKey,
	}

	err = tr.publishSingle(ctx, msg)
	assert.NoError(t, err)

	// Verify partition key
	assert.Equal(t, "partition-1", partitionKey)
}

func TestIntegrationBatchPublishing(t *testing.T) {
	sess := createLocalStackSession(t)
	snsClient := sns.New(sess)
	
	// Create a standard topic
	topicName := fmt.Sprintf("test-batch-topic-%d", time.Now().Unix())
	createTopicOutput, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := *createTopicOutput.TopicArn

	// Clean up after test
	defer func() {
		snsClient.DeleteTopic(&sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	}()

	// Test configuration with batch size > 1
	cfg := config{
		AWSRegion: "us-east-1",
		TopicARN:  topicARN,
		TopicType: "standard",
		BatchSize: 3,
	}

	// Create transactor
	tr := &transactor{
		config: cfg,
		client: snsClient,
		bindings: []*topicBinding{
			{
				identifier: "test-binding",
				topicName:  topicName,
			},
		},
		idempotencyCache: make(map[string]time.Time),
	}

	// Create multiple messages for batch publishing
	var messages []*messageToPublish
	for i := 0; i < 3; i++ {
		testDoc := map[string]interface{}{
			"_meta": map[string]interface{}{
				"op":        "c",
				"namespace": "test.collection",
				"source":    "test-source",
				"uuid":      fmt.Sprintf("test-uuid-%d", i),
			},
			"id":   i,
			"name": fmt.Sprintf("test-record-%d", i),
		}

		docBytes, err := json.Marshal(testDoc)
		require.NoError(t, err)

		idempotencyKey := fmt.Sprintf("test-key-%d", i)
		msg := &messageToPublish{
			binding:        tr.bindings[0],
			data:          docBytes,
			attributes:    tr.buildMessageAttributes(testDoc, nil, tr.bindings[0], idempotencyKey),
			idempotencyKey: idempotencyKey,
		}
		messages = append(messages, msg)
	}

	// Test batch publishing
	ctx := context.Background()
	err = tr.publishBatch(ctx, messages)
	assert.NoError(t, err)

	// Verify all messages are in idempotency cache
	for i := 0; i < 3; i++ {
		assert.True(t, tr.isDuplicate(fmt.Sprintf("test-key-%d", i)))
	}
}

func TestIntegrationDLQHandling(t *testing.T) {
	sess := createLocalStackSession(t)
	snsClient := sns.New(sess)
	sqsClient := sqs.New(sess)
	
	// Create a DLQ
	queueName := fmt.Sprintf("test-dlq-%d", time.Now().Unix())
	createQueueOutput, err := sqsClient.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := *createQueueOutput.QueueUrl

	// Get queue attributes to get the ARN
	queueAttrs, err := sqsClient.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []*string{aws.String("QueueArn")},
	})
	require.NoError(t, err)
	queueARN := *queueAttrs.Attributes["QueueArn"]

	// Clean up after test
	defer func() {
		sqsClient.DeleteQueue(&sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	}()

	// Create a topic (we'll simulate failure by using invalid topic ARN)
	invalidTopicARN := "arn:aws:sns:us-east-1:123456789012:non-existent-topic"

	// Test configuration with DLQ
	cfg := config{
		AWSRegion: "us-east-1",
		TopicARN:  invalidTopicARN,
		TopicType: "standard",
		BatchSize: 1,
		DLQSQSARN: queueARN,
	}

	// Create transactor
	tr := &transactor{
		config: cfg,
		client: snsClient,
		bindings: []*topicBinding{
			{
				identifier: "test-binding",
				topicName:  "non-existent-topic",
			},
		},
		idempotencyCache: make(map[string]time.Time),
	}

	// Test message that will fail to publish
	testDoc := map[string]interface{}{
		"_meta": map[string]interface{}{
			"op":        "c",
			"namespace": "test.collection",
			"source":    "test-source",
			"uuid":      "test-uuid-123",
		},
		"id":   1,
		"name": "test-record",
	}

	docBytes, err := json.Marshal(testDoc)
	require.NoError(t, err)

	msg := &messageToPublish{
		binding:        tr.bindings[0],
		data:          docBytes,
		attributes:    tr.buildMessageAttributes(testDoc, nil, tr.bindings[0], "test-key"),
		idempotencyKey: "test-key",
	}

	// This should fail to publish but succeed in sending to DLQ
	ctx := context.Background()
	err = tr.publishSingle(ctx, msg)
	// Should not return error because message was sent to DLQ
	assert.NoError(t, err)

	// Check that message was sent to DLQ
	receiveOutput, err := sqsClient.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: aws.Int64(1),
		WaitTimeSeconds:     aws.Int64(1),
	})
	require.NoError(t, err)
	assert.Len(t, receiveOutput.Messages, 1)

	// Verify DLQ message content
	dlqMessage := receiveOutput.Messages[0]
	var dlqContent map[string]interface{}
	err = json.Unmarshal([]byte(*dlqMessage.Body), &dlqContent)
	require.NoError(t, err)

	assert.Contains(t, dlqContent, "original_message")
	assert.Contains(t, dlqContent, "failure_reason")
	assert.Contains(t, dlqContent, "topic_arn")
	assert.Equal(t, invalidTopicARN, dlqContent["topic_arn"])
}

func TestIntegrationDriverValidation(t *testing.T) {
	sess := createLocalStackSession(t)
	snsClient := sns.New(sess)
	
	// Create a topic for validation
	topicName := fmt.Sprintf("test-validation-topic-%d", time.Now().Unix())
	createTopicOutput, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := *createTopicOutput.TopicArn

	// Clean up after test
	defer func() {
		snsClient.DeleteTopic(&sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	}()

	// Override the client creation for testing
	originalConfig := config{
		AWSRegion: "us-east-1",
		TopicARN:  topicARN,
	}

	// Test that validation works with a real topic
	// Note: This would require modifying the config.client method to accept LocalStack endpoint
	// For now, we just test the structure
	err = originalConfig.Validate()
	assert.NoError(t, err)
}

func TestIntegrationLoadTest(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	sess := createLocalStackSession(t)
	snsClient := sns.New(sess)
	
	// Create a topic for load testing
	topicName := fmt.Sprintf("test-load-topic-%d", time.Now().Unix())
	createTopicOutput, err := snsClient.CreateTopic(&sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := *createTopicOutput.TopicArn

	// Clean up after test
	defer func() {
		snsClient.DeleteTopic(&sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	}()

	// Test configuration
	cfg := config{
		AWSRegion:   "us-east-1",
		TopicARN:    topicARN,
		TopicType:   "standard",
		BatchSize:   10,
		MaxInFlight: 50,
	}

	// Create transactor
	tr := &transactor{
		config: cfg,
		client: snsClient,
		bindings: []*topicBinding{
			{
				identifier: "load-test-binding",
				topicName:  topicName,
			},
		},
		idempotencyCache: make(map[string]time.Time),
	}

	// Generate and publish 1000 messages
	ctx := context.Background()
	numMessages := 1000
	batchSize := 10

	start := time.Now()
	
	for i := 0; i < numMessages; i += batchSize {
		var batch []*messageToPublish
		
		for j := 0; j < batchSize && i+j < numMessages; j++ {
			msgNum := i + j
			testDoc := map[string]interface{}{
				"_meta": map[string]interface{}{
					"op":        "c",
					"namespace": "load.test",
					"source":    "load-test-source",
					"uuid":      fmt.Sprintf("load-test-uuid-%d", msgNum),
				},
				"id":      msgNum,
				"name":    fmt.Sprintf("load-test-record-%d", msgNum),
				"payload": fmt.Sprintf("This is test payload number %d with some additional data to make the message larger", msgNum),
			}

			docBytes, err := json.Marshal(testDoc)
			require.NoError(t, err)

			idempotencyKey := fmt.Sprintf("load-test-key-%d", msgNum)
			msg := &messageToPublish{
				binding:        tr.bindings[0],
				data:          docBytes,
				attributes:    tr.buildMessageAttributes(testDoc, nil, tr.bindings[0], idempotencyKey),
				idempotencyKey: idempotencyKey,
			}
			batch = append(batch, msg)
		}

		err = tr.publishBatch(ctx, batch)
		assert.NoError(t, err)
	}

	duration := time.Since(start)
	messagesPerSecond := float64(numMessages) / duration.Seconds()

	t.Logf("Published %d messages in %v (%.2f messages/second)", numMessages, duration, messagesPerSecond)
	
	// Verify we can handle reasonable throughput (at least 100 messages/second)
	assert.Greater(t, messagesPerSecond, 100.0, "Throughput should be at least 100 messages/second")
}

