package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock SNS client for testing
type mockSNSClient struct {
	snsiface.SNSAPI
	mock.Mock
}

func (m *mockSNSClient) GetTopicAttributes(input *sns.GetTopicAttributesInput) (*sns.GetTopicAttributesOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sns.GetTopicAttributesOutput), args.Error(1)
}

func (m *mockSNSClient) Publish(input *sns.PublishInput) (*sns.PublishOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sns.PublishOutput), args.Error(1)
}

func (m *mockSNSClient) PublishBatch(input *sns.PublishBatchInput) (*sns.PublishBatchOutput, error) {
	args := m.Called(input)
	return args.Get(0).(*sns.PublishBatchOutput), args.Error(1)
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      config
		expectError bool
	}{
		{
			name: "valid config",
			config: config{
				AWSRegion: "us-east-1",
				TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
			},
			expectError: false,
		},
		{
			name: "missing region",
			config: config{
				TopicARN: "arn:aws:sns:us-east-1:123456789012:test-topic",
			},
			expectError: true,
		},
		{
			name: "missing topic ARN",
			config: config{
				AWSRegion: "us-east-1",
			},
			expectError: true,
		},
		{
			name: "invalid topic type",
			config: config{
				AWSRegion: "us-east-1",
				TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
				TopicType: "invalid",
			},
			expectError: true,
		},
		{
			name: "invalid delivery mode",
			config: config{
				AWSRegion:    "us-east-1",
				TopicARN:     "arn:aws:sns:us-east-1:123456789012:test-topic",
				DeliveryMode: "invalid",
			},
			expectError: true,
		},
		{
			name: "invalid batch size",
			config: config{
				AWSRegion: "us-east-1",
				TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
				BatchSize: 15, // Max is 10
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestResourceValidation(t *testing.T) {
	tests := []struct {
		name        string
		resource    resource
		expectError bool
	}{
		{
			name: "valid resource",
			resource: resource{
				TopicName: "test-topic",
			},
			expectError: false,
		},
		{
			name: "missing topic name",
			resource: resource{
				Identifier: "test-id",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.resource.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDriverSpec(t *testing.T) {
	d := Driver()
	ctx := context.Background()

	req := &pm.Request_Spec{}
	resp, err := d.Spec(ctx, req)

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.NotEmpty(t, resp.ConfigSchemaJson)
	assert.NotEmpty(t, resp.ResourceConfigSchemaJson)
	assert.Equal(t, "https://go.estuary.dev/materialize-aws-sns", resp.DocumentationUrl)
}

func TestDriverValidate(t *testing.T) {
	d := Driver()
	ctx := context.Background()

	// Test with valid config
	cfg := config{
		AWSRegion: "us-east-1",
		TopicARN:  "arn:aws:sns:us-east-1:123456789012:test-topic",
	}
	cfgBytes, _ := json.Marshal(cfg)

	res := resource{
		TopicName: "test-topic",
	}
	resBytes, _ := json.Marshal(res)

	req := &pm.Request_Validate{
		ConfigJson: cfgBytes,
		Bindings: []*pm.Request_Validate_Binding{
			{
				ResourceConfigJson: resBytes,
				Collection: &pm.CollectionSpec{
					Projections: []*pm.Projection{
						{
							Field: "",
							Ptr:   "",
						},
					},
				},
			},
		},
	}

	// This will fail without a real AWS connection, but we can test the config parsing
	_, err := d.Validate(ctx, req)
	// We expect an error here because we don't have real AWS credentials
	assert.Error(t, err)
}

func TestResolveEndpointConfig(t *testing.T) {
	tests := []struct {
		name        string
		configJson  string
		expectError bool
		expected    config
	}{
		{
			name:       "valid config with defaults",
			configJson: `{"aws_region": "us-east-1", "topic_arn": "arn:aws:sns:us-east-1:123456789012:test-topic"}`,
			expectError: false,
			expected: config{
				AWSRegion:              "us-east-1",
				TopicARN:               "arn:aws:sns:us-east-1:123456789012:test-topic",
				TopicType:              "standard",
				PartitionKeyField:      "/_meta/partition_key",
				IdempotencyKeyTemplate: "<source>:<ns>:<doc_id>:<ts_ms>",
				DeliveryMode:           "async",
				BatchSize:              1,
				MaxInFlight:            100,
				LogLevel:               "info",
				MetricsNamespace:       "materialize_aws_sns",
				RetryBaseInterval:      "100ms",
				RetryMaxInterval:       "30s",
				RetryMaxAttempts:       3,
				RetryJitter:            0.1,
			},
		},
		{
			name:       "config with custom values",
			configJson: `{"aws_region": "us-west-2", "topic_arn": "arn:aws:sns:us-west-2:123456789012:test-topic.fifo", "topic_type": "fifo", "batch_size": 5}`,
			expectError: false,
			expected: config{
				AWSRegion:              "us-west-2",
				TopicARN:               "arn:aws:sns:us-west-2:123456789012:test-topic.fifo",
				TopicType:              "fifo",
				PartitionKeyField:      "/_meta/partition_key",
				IdempotencyKeyTemplate: "<source>:<ns>:<doc_id>:<ts_ms>",
				DeliveryMode:           "async",
				BatchSize:              5,
				MaxInFlight:            100,
				LogLevel:               "info",
				MetricsNamespace:       "materialize_aws_sns",
				RetryBaseInterval:      "100ms",
				RetryMaxInterval:       "30s",
				RetryMaxAttempts:       3,
				RetryJitter:            0.1,
			},
		},
		{
			name:        "invalid JSON",
			configJson:  `{"aws_region": "us-east-1"`,
			expectError: true,
		},
		{
			name:        "missing required field",
			configJson:  `{"aws_region": "us-east-1"}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := resolveEndpointConfig(json.RawMessage(tt.configJson))
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, cfg)
			}
		})
	}
}

func TestResolveResourceConfig(t *testing.T) {
	tests := []struct {
		name         string
		resourceJson string
		expectError  bool
		expected     resource
	}{
		{
			name:         "valid resource",
			resourceJson: `{"topic": "test-topic", "identifier": "test-id"}`,
			expectError:  false,
			expected: resource{
				TopicName:  "test-topic",
				Identifier: "test-id",
			},
		},
		{
			name:         "resource without identifier",
			resourceJson: `{"topic": "test-topic"}`,
			expectError:  false,
			expected: resource{
				TopicName: "test-topic",
			},
		},
		{
			name:         "invalid JSON",
			resourceJson: `{"topic": "test-topic"`,
			expectError:  true,
		},
		{
			name:         "missing required field",
			resourceJson: `{"identifier": "test-id"}`,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := resolveResourceConfig(json.RawMessage(tt.resourceJson))
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, res)
			}
		})
	}
}

