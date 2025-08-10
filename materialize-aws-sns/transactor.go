package connector

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/sync/errgroup"
)

// Transactor handles the materialization of documents to SNS.
type Transactor struct {
	config   *Config
	client   snsiface.SNSAPI
	bindings []*TopicBinding

	// For idempotency tracking (in-memory for Standard topics)
	idempotencyCache map[string]time.Time
	cacheMutex       sync.RWMutex
}

// NewTransactor creates a new transactor.
func NewTransactor(config *Config, client snsiface.SNSAPI, bindings []*TopicBinding) *Transactor {
	t := &Transactor{
		config:   config,
		client:   client,
		bindings: bindings,
	}

	// Initialize idempotency cache for Standard topics
	if config.TopicType == "standard" {
		t.idempotencyCache = make(map[string]time.Time)
	}

	return t
}

// MessageToPublish represents a message ready for publishing.
type MessageToPublish struct {
	binding                *TopicBinding
	data                   []byte
	attributes             map[string]*sns.MessageAttributeValue
	messageGroupId         *string
	messageDeduplicationId *string
	idempotencyKey         string
}

// UnmarshalState initializes the transactor state.
func (t *Transactor) UnmarshalState(state json.RawMessage) error {
	// Initialize idempotency cache for Standard topics
	if t.config.TopicType == "standard" {
		t.idempotencyCache = make(map[string]time.Time)
	}
	return nil
}

// Acknowledge cleans up expired cache entries.
func (t *Transactor) Acknowledge(ctx context.Context) error {
	// Clean up expired idempotency cache entries (TTL: 5 minutes for Standard topics)
	if t.config.TopicType == "standard" && t.idempotencyCache != nil {
		t.cacheMutex.Lock()
		now := time.Now()
		for key, timestamp := range t.idempotencyCache {
			if now.Sub(timestamp) > 5*time.Minute {
				delete(t.idempotencyCache, key)
			}
		}
		t.cacheMutex.Unlock()
	}
	return nil
}

// PublishBatch publishes a batch of messages.
func (t *Transactor) PublishBatch(ctx context.Context, messages []*MessageToPublish) error {
	if len(messages) == 0 {
		return nil
	}

	// For single message or when batch size is 1, use individual Publish
	if len(messages) == 1 || t.config.BatchSize == 1 {
		return t.publishSingle(ctx, messages[0])
	}

	// Use PublishBatch for multiple messages (up to 10)
	var entries []*sns.PublishBatchRequestEntry
	for i, msg := range messages {
		entry := &sns.PublishBatchRequestEntry{
			Id:                aws.String(fmt.Sprintf("msg-%d", i)),
			Message:           aws.String(string(msg.data)),
			MessageAttributes: msg.attributes,
		}

		if msg.messageGroupId != nil {
			entry.MessageGroupId = msg.messageGroupId
		}
		if msg.messageDeduplicationId != nil {
			entry.MessageDeduplicationId = msg.messageDeduplicationId
		}

		entries = append(entries, entry)
	}

	input := &sns.PublishBatchInput{
		TopicArn:                   aws.String(t.config.TopicARN),
		PublishBatchRequestEntries: entries,
	}

	result, err := t.client.PublishBatch(input)
	if err != nil {
		// Handle batch failure - try individual publishes or send to DLQ
		return t.handleBatchFailure(ctx, messages, err)
	}

	// Handle partial failures
	if len(result.Failed) > 0 {
		return t.handlePartialFailure(ctx, messages, result.Failed)
	}

	// Mark messages as successfully sent for idempotency tracking
	if t.config.TopicType == "standard" {
		t.cacheMutex.Lock()
		now := time.Now()
		for _, msg := range messages {
			t.idempotencyCache[msg.idempotencyKey] = now
		}
		t.cacheMutex.Unlock()
	}

	return nil
}

func (t *Transactor) publishSingle(ctx context.Context, msg *MessageToPublish) error {
	input := &sns.PublishInput{
		TopicArn:          aws.String(t.config.TopicARN),
		Message:           aws.String(string(msg.data)),
		MessageAttributes: msg.attributes,
	}

	if msg.messageGroupId != nil {
		input.MessageGroupId = msg.messageGroupId
	}
	if msg.messageDeduplicationId != nil {
		input.MessageDeduplicationId = msg.messageDeduplicationId
	}

	_, err := t.client.Publish(input)
	if err != nil {
		return t.handlePublishFailure(ctx, msg, err)
	}

	// Mark message as successfully sent for idempotency tracking
	if t.config.TopicType == "standard" {
		t.cacheMutex.Lock()
		t.idempotencyCache[msg.idempotencyKey] = time.Now()
		t.cacheMutex.Unlock()
	}

	return nil
}

func (t *Transactor) handleBatchFailure(ctx context.Context, messages []*MessageToPublish, err error) error {
	// Try individual publishes for each message
	for _, msg := range messages {
		if publishErr := t.publishSingle(ctx, msg); publishErr != nil {
			// If individual publish also fails, send to DLQ
			if dlqErr := t.sendToDLQ(ctx, msg, publishErr); dlqErr != nil {
				return fmt.Errorf("failed to publish message and send to DLQ: publish error: %w, DLQ error: %v", publishErr, dlqErr)
			}
		}
	}
	return nil
}

func (t *Transactor) handlePartialFailure(ctx context.Context, messages []*MessageToPublish, failed []*sns.BatchResultErrorEntry) error {
	// Create a map of failed message IDs
	failedIds := make(map[string]*sns.BatchResultErrorEntry)
	for _, failure := range failed {
		failedIds[*failure.Id] = failure
	}

	// Retry failed messages individually or send to DLQ
	for i, msg := range messages {
		msgId := fmt.Sprintf("msg-%d", i)
		if _, exists := failedIds[msgId]; exists {
			if publishErr := t.publishSingle(ctx, msg); publishErr != nil {
				if dlqErr := t.sendToDLQ(ctx, msg, publishErr); dlqErr != nil {
					return fmt.Errorf("failed to retry message and send to DLQ: retry error: %w, DLQ error: %v", publishErr, dlqErr)
				}
			}
		}
	}

	return nil
}

func (t *Transactor) handlePublishFailure(ctx context.Context, msg *MessageToPublish, err error) error {
	// Send to DLQ if configured
	if dlqErr := t.sendToDLQ(ctx, msg, err); dlqErr != nil {
		return fmt.Errorf("failed to publish message and send to DLQ: publish error: %w, DLQ error: %v", err, dlqErr)
	}
	return nil
}

func (t *Transactor) sendToDLQ(ctx context.Context, msg *MessageToPublish, originalErr error) error {
	if t.config.DLQSQSARN == "" {
		return originalErr // No DLQ configured, return original error
	}

	// Create SQS client using the same session configuration as SNS
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(t.config.AWSRegion),
	})
	if err != nil {
		return fmt.Errorf("failed to create SQS session: %w", err)
	}
	
	sqsClient := sqs.New(sess)

	// Create DLQ message with failure metadata
	dlqMessage := map[string]interface{}{
		"original_message": string(msg.data),
		"failure_reason":   originalErr.Error(),
		"topic_arn":        t.config.TopicARN,
		"timestamp":        time.Now().UTC().Format(time.RFC3339),
		"binding":          msg.binding.TopicName,
		"identifier":       msg.binding.Identifier,
	}

	dlqMessageBytes, err := json.Marshal(dlqMessage)
	if err != nil {
		return fmt.Errorf("failed to marshal DLQ message: %w", err)
	}

	_, err = sqsClient.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(t.config.DLQSQSARN),
		MessageBody: aws.String(string(dlqMessageBytes)),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to DLQ: %w", err)
	}

	return nil // Successfully sent to DLQ, don't return original error
}

// GenerateIdempotencyKey generates an idempotency key for a document.
func (t *Transactor) GenerateIdempotencyKey(doc map[string]interface{}) string {
	template := t.config.IdempotencyKeyTemplate
	if template == "" {
		template = "<source>:<ns>:<doc_id>:<ts_ms>"
	}

	// Extract values from document
	source := "unknown"
	namespace := "unknown"
	docId := "unknown"
	tsMs := strconv.FormatInt(time.Now().UnixMilli(), 10)

	// Try to extract source from document metadata
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if s, ok := meta["source"].(string); ok {
			source = s
		}
		if ns, ok := meta["namespace"].(string); ok {
			namespace = ns
		}
		if id, ok := meta["uuid"].(string); ok {
			docId = id
		}
	}

	// Replace placeholders in template
	key := strings.ReplaceAll(template, "<source>", source)
	key = strings.ReplaceAll(key, "<ns>", namespace)
	key = strings.ReplaceAll(key, "<doc_id>", docId)
	key = strings.ReplaceAll(key, "<ts_ms>", tsMs)

	return key
}

// ExtractPartitionKey extracts the partition key from a document.
func (t *Transactor) ExtractPartitionKey(doc map[string]interface{}) string {
	field := t.config.PartitionKeyField
	if field == "" {
		field = "/_meta/partition_key"
	}

	// Navigate the field path
	parts := strings.Split(strings.TrimPrefix(field, "/"), "/")
	current := doc

	for _, part := range parts {
		if part == "" {
			continue
		}
		if next, ok := current[part]; ok {
			if nextMap, ok := next.(map[string]interface{}); ok {
				current = nextMap
			} else {
				// Found the value
				if str, ok := next.(string); ok {
					return str
				}
				return fmt.Sprintf("%v", next)
			}
		} else {
			break
		}
	}

	// Fallback to namespace or a hash of the document
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if ns, ok := meta["namespace"].(string); ok {
			return ns
		}
	}

	// Final fallback: hash of the entire document
	docBytes, _ := json.Marshal(doc)
	hash := md5.Sum(docBytes)
	return hex.EncodeToString(hash[:])[:8] // Use first 8 characters of hash
}

// BuildMessageAttributes builds SNS message attributes from a document.
func (t *Transactor) BuildMessageAttributes(doc map[string]interface{}, binding *TopicBinding, idempotencyKey string) map[string]*sns.MessageAttributeValue {
	attrs := make(map[string]*sns.MessageAttributeValue)

	// Operation type (create/update/delete)
	op := "c" // default to create
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if opVal, ok := meta["op"].(string); ok {
			op = opVal
		}
	}
	attrs["op"] = &sns.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(op),
	}

	// Namespace
	namespace := "unknown"
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if ns, ok := meta["namespace"].(string); ok {
			namespace = ns
		}
	}
	attrs["namespace"] = &sns.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(namespace),
	}

	// Source
	source := "unknown"
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if s, ok := meta["source"].(string); ok {
			source = s
		}
	}
	attrs["source"] = &sns.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(source),
	}

	// Idempotency key
	attrs["idempotency_key"] = &sns.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(idempotencyKey),
	}

	// Trace ID (if available)
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if traceId, ok := meta["trace_id"].(string); ok && traceId != "" {
			attrs["trace_id"] = &sns.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(traceId),
			}
		}
	}

	// Optional attributes
	if meta, ok := doc["_meta"].(map[string]interface{}); ok {
		if tenantId, ok := meta["tenant_id"].(string); ok && tenantId != "" {
			attrs["tenant_id"] = &sns.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(tenantId),
			}
		}
		if env, ok := meta["env"].(string); ok && env != "" {
			attrs["env"] = &sns.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(env),
			}
		}
		if stream, ok := meta["stream"].(string); ok && stream != "" {
			attrs["stream"] = &sns.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(stream),
			}
		}
	}

	// Identifier (if configured)
	if binding.Identifier != "" {
		attrs["identifier"] = &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(binding.Identifier),
		}
	}

	return attrs
}

// IsDuplicate checks if a message is a duplicate.
func (t *Transactor) IsDuplicate(idempotencyKey string) bool {
	if t.config.TopicType != "standard" || t.idempotencyCache == nil {
		return false
	}

	t.cacheMutex.RLock()
	_, exists := t.idempotencyCache[idempotencyKey]
	t.cacheMutex.RUnlock()

	return exists
}

// Destroy cleans up the transactor.
func (t *Transactor) Destroy() {
	// Clean up resources
	if t.idempotencyCache != nil {
		t.cacheMutex.Lock()
		t.idempotencyCache = nil
		t.cacheMutex.Unlock()
	}
}

// ProcessDocuments processes a batch of documents and publishes them to SNS.
func (t *Transactor) ProcessDocuments(ctx context.Context, documents []map[string]interface{}) error {
	var messageBatch []*MessageToPublish
	batchSize := t.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	errGroup, groupCtx := errgroup.WithContext(ctx)

	for i, doc := range documents {
		binding := t.bindings[i%len(t.bindings)] // Round-robin bindings

		// Extract document metadata
		docBytes, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		// Generate idempotency key
		idempotencyKey := t.GenerateIdempotencyKey(doc)

		// Check for duplicates (Standard topics only)
		if t.config.TopicType == "standard" && t.IsDuplicate(idempotencyKey) {
			continue // Skip duplicate message
		}

		// Generate partition key for FIFO topics
		var messageGroupId *string
		var messageDeduplicationId *string
		if t.config.TopicType == "fifo" {
			partitionKey := t.ExtractPartitionKey(doc)
			messageGroupId = aws.String(partitionKey)
			messageDeduplicationId = aws.String(idempotencyKey)
		}

		// Build message attributes
		attrs := t.BuildMessageAttributes(doc, binding, idempotencyKey)

		msg := &MessageToPublish{
			binding:                binding,
			data:                   docBytes,
			attributes:             attrs,
			messageGroupId:         messageGroupId,
			messageDeduplicationId: messageDeduplicationId,
			idempotencyKey:         idempotencyKey,
		}

		messageBatch = append(messageBatch, msg)

		// Publish batch when it reaches the configured size
		if len(messageBatch) >= batchSize {
			batch := messageBatch
			messageBatch = nil

			errGroup.Go(func() error {
				return t.PublishBatch(groupCtx, batch)
			})
		}
	}

	// Publish remaining messages in the batch
	if len(messageBatch) > 0 {
		batch := messageBatch
		errGroup.Go(func() error {
			return t.PublishBatch(groupCtx, batch)
		})
	}

	// Wait for all messages to be delivered
	return errGroup.Wait()
}

