# AWS SNS Materializer

A Flow materialization connector that publishes documents from Flow collections to Amazon SNS (Simple Notification Service) topics.

## Features

- **Topic Types**: Supports both Standard and FIFO SNS topics
- **At-least-once delivery**: For Standard topics with in-memory deduplication
- **Exactly-once delivery**: For FIFO topics using MessageDeduplicationId and MessageGroupId
- **Batching**: Efficient publishing using SNS PublishBatch (up to 10 messages per batch)
- **Retry Logic**: Configurable exponential backoff with jitter
- **Dead Letter Queue**: Failed messages can be sent to SQS DLQ after retry exhaustion
- **Security**: Support for IAM role assumption and KMS encryption
- **Observability**: Structured logging and metrics

## Configuration

### Endpoint Configuration

```json
{
  "aws_region": "us-east-1",
  "access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
  "secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
  "topic_arn": "arn:aws:sns:us-east-1:123456789012:my-topic",
  "topic_type": "standard",
  "partition_key_field": "/_meta/partition_key",
  "idempotency_key_template": "<source>:<ns>:<doc_id>:<ts_ms>",
  "delivery_mode": "async",
  "batch_size": 10,
  "max_in_flight": 100,
  "retry_base_interval": "100ms",
  "retry_max_interval": "30s",
  "retry_max_attempts": 3,
  "retry_jitter": 0.1,
  "dlq_sqs_arn": "arn:aws:sqs:us-east-1:123456789012:my-dlq",
  "kms_key_arn": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
  "assume_role_arn": "arn:aws:iam::123456789012:role/FlowSNSRole",
  "log_level": "info",
  "metrics_namespace": "materialize_aws_sns"
}
```

### Resource Configuration

```json
{
  "topic": "my-topic-name",
  "identifier": "optional-binding-identifier"
}
```

## Message Format

### Message Body
The full Flow document/envelope as JSON.

### Message Attributes
- `op`: Operation type (c/u/d for create/update/delete)
- `namespace`: Flow collection namespace
- `source`: Data source identifier
- `idempotency_key`: Generated idempotency key
- `trace_id`: Optional trace identifier
- `tenant_id`: Optional tenant identifier
- `env`: Optional environment identifier
- `stream`: Optional stream identifier
- `identifier`: Optional binding identifier

### FIFO Topics
- `MessageGroupId`: Derived from partition key field
- `MessageDeduplicationId`: Uses the idempotency key

## AWS Permissions

The connector requires the following IAM permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sns:Publish",
        "sns:GetTopicAttributes"
      ],
      "Resource": "arn:aws:sns:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage"
      ],
      "Resource": "arn:aws:sqs:*:*:*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "us-east-1"
        }
      }
    },
    {
      "Effect": "Allow",
      "Action": [
        "kms:Encrypt",
        "kms:Decrypt",
        "kms:ReEncrypt*",
        "kms:GenerateDataKey*",
        "kms:DescribeKey"
      ],
      "Resource": "arn:aws:kms:*:*:key/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sts:AssumeRole"
      ],
      "Resource": "arn:aws:iam::*:role/*"
    }
  ]
}
```

## Terraform Examples

### Standard SNS Topic

```hcl
resource "aws_sns_topic" "standard_topic" {
  name = "flow-materialization-standard"
  
  tags = {
    Environment = "production"
    Purpose     = "flow-materialization"
  }
}

resource "aws_sns_topic_policy" "standard_topic_policy" {
  arn = aws_sns_topic.standard_topic.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/FlowSNSRole"
        }
        Action = [
          "sns:Publish",
          "sns:GetTopicAttributes"
        ]
        Resource = aws_sns_topic.standard_topic.arn
      }
    ]
  })
}
```

### FIFO SNS Topic

```hcl
resource "aws_sns_topic" "fifo_topic" {
  name                        = "flow-materialization-fifo.fifo"
  fifo_topic                  = true
  content_based_deduplication = false  # We provide explicit deduplication IDs
  
  tags = {
    Environment = "production"
    Purpose     = "flow-materialization"
  }
}

resource "aws_sns_topic_policy" "fifo_topic_policy" {
  arn = aws_sns_topic.fifo_topic.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/FlowSNSRole"
        }
        Action = [
          "sns:Publish",
          "sns:GetTopicAttributes"
        ]
        Resource = aws_sns_topic.fifo_topic.arn
      }
    ]
  })
}
```

### Dead Letter Queue

```hcl
resource "aws_sqs_queue" "dlq" {
  name = "flow-materialization-dlq"
  
  message_retention_seconds = 1209600  # 14 days
  
  tags = {
    Environment = "production"
    Purpose     = "flow-materialization-dlq"
  }
}

resource "aws_sqs_queue_policy" "dlq_policy" {
  queue_url = aws_sqs_queue.dlq.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/FlowSNSRole"
        }
        Action = [
          "sqs:SendMessage"
        ]
        Resource = aws_sqs_queue.dlq.arn
      }
    ]
  })
}
```

### IAM Role

```hcl
resource "aws_iam_role" "flow_sns_role" {
  name = "FlowSNSRole"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"  # Adjust based on your Flow deployment
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "flow_sns_policy" {
  name = "FlowSNSPolicy"
  role = aws_iam_role.flow_sns_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "sns:GetTopicAttributes"
        ]
        Resource = [
          aws_sns_topic.standard_topic.arn,
          aws_sns_topic.fifo_topic.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = aws_sqs_queue.dlq.arn
      }
    ]
  })
}
```

## Demo

A simple demo that publishes Flow collection documents to SNS and prints message attributes:

### LocalStack Setup

```bash
# Start LocalStack
docker run --rm -it -p 4566:4566 -p 4571:4571 localstack/localstack

# Create SNS topic
aws --endpoint-url=http://localhost:4566 sns create-topic --name demo-topic --region us-east-1

# Create SQS queue for testing
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name demo-queue --region us-east-1

# Subscribe queue to topic
aws --endpoint-url=http://localhost:4566 sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:demo-topic \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:demo-queue \
  --region us-east-1
```

### Flow Configuration

```yaml
# flow.yaml
collections:
  demo/events:
    schema:
      type: object
      properties:
        id: { type: integer }
        name: { type: string }
        timestamp: { type: string, format: date-time }
      required: [id, name, timestamp]
    key: [/id]

materializations:
  demo/sns-materialization:
    endpoint:
      connector:
        image: ghcr.io/estuary/materialize-aws-sns:dev
        config:
          aws_region: us-east-1
          topic_arn: arn:aws:sns:us-east-1:000000000000:demo-topic
          topic_type: standard
          batch_size: 5
    bindings:
      - resource:
          topic: demo-topic
          identifier: demo-binding
        source: demo/events
```

### Subscriber Script

```python
#!/usr/bin/env python3
import boto3
import json
import time

# Configure for LocalStack
sqs = boto3.client(
    'sqs',
    endpoint_url='http://localhost:4566',
    region_name='us-east-1',
    aws_access_key_id='test',
    aws_secret_access_key='test'
)

queue_url = 'http://localhost:4566/000000000000/demo-queue'

print("Listening for SNS messages...")
while True:
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=5
    )
    
    if 'Messages' in response:
        for message in response['Messages']:
            # Parse SNS message
            sns_message = json.loads(message['Body'])
            flow_document = json.loads(sns_message['Message'])
            
            print(f"Received Flow document: {flow_document}")
            print(f"Message attributes: {sns_message.get('MessageAttributes', {})}")
            
            # Delete processed message
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
    
    time.sleep(1)
```

## Testing

### Unit Tests

```bash
cd materialize-aws-sns
go test -v ./...
```

### Integration Tests with LocalStack

```bash
# Start LocalStack
docker run --rm -d -p 4566:4566 --name localstack localstack/localstack

# Run integration tests
LOCALSTACK_ENDPOINT=http://localhost:4566 go test -v -tags=integration ./...

# Stop LocalStack
docker stop localstack
```

### Load Testing

```bash
# Run load tests (publishes ~10k documents)
go test -v -run TestIntegrationLoadTest ./...
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure the IAM role has the required SNS and SQS permissions
2. **Topic Not Found**: Verify the topic ARN is correct and the topic exists
3. **FIFO Topic Errors**: Ensure MessageGroupId and MessageDeduplicationId are provided for FIFO topics
4. **Batch Size Errors**: SNS PublishBatch supports maximum 10 messages per batch

### Monitoring

The connector emits the following metrics:
- `published_total`: Total number of messages published
- `failed_total`: Total number of failed publish attempts
- `latency_ms`: Publishing latency in milliseconds
- `retries_total`: Total number of retry attempts
- `dlq_total`: Total number of messages sent to DLQ

### Logging

Set `log_level` to `debug` for detailed logging:

```json
{
  "log_level": "debug"
}
```

## FIFO Topic Considerations

When using FIFO topics:

1. **Ordering**: Messages with the same MessageGroupId are delivered in order
2. **Deduplication**: Messages with the same MessageDeduplicationId within the deduplication interval (5 minutes) are deduplicated
3. **Throughput**: FIFO topics have lower throughput limits compared to Standard topics
4. **Naming**: FIFO topic names must end with `.fifo`

## Performance Tuning

- **Batch Size**: Increase `batch_size` (up to 10) for higher throughput
- **Max In Flight**: Increase `max_in_flight` for better concurrency
- **Delivery Mode**: Use `async` for better performance, `sync` for immediate error handling
- **Retry Configuration**: Adjust retry parameters based on your error tolerance

## Security Best Practices

1. Use IAM roles instead of access keys
2. Enable KMS encryption for sensitive data
3. Use least-privilege IAM policies
4. Monitor CloudTrail logs for SNS API calls
5. Use VPC endpoints for private network communication

