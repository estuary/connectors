package connector

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sts"
)

// Config holds the configuration for the AWS SNS materializer.
type Config struct {
	AWSRegion                string  `json:"aws_region"`
	AccessKeyID              string  `json:"access_key_id,omitempty"`
	SecretAccessKey          string  `json:"secret_access_key,omitempty"`
	TopicARN                 string  `json:"topic_arn"`
	TopicType                string  `json:"topic_type,omitempty"`
	PartitionKeyField        string  `json:"partition_key_field,omitempty"`
	IdempotencyKeyTemplate   string  `json:"idempotency_key_template,omitempty"`
	DeliveryMode             string  `json:"delivery_mode,omitempty"`
	BatchSize                int     `json:"batch_size,omitempty"`
	MaxInFlight              int     `json:"max_in_flight,omitempty"`
	DLQSQSARN                string  `json:"dlq_sqs_arn,omitempty"`
	KMSKeyARN                string  `json:"kms_key_arn,omitempty"`
	AssumeRoleARN            string  `json:"assume_role_arn,omitempty"`
	LogLevel                 string  `json:"log_level,omitempty"`
	MetricsNamespace         string  `json:"metrics_namespace,omitempty"`
	RetryBaseInterval        string  `json:"retry_base_interval,omitempty"`
	RetryMaxInterval         string  `json:"retry_max_interval,omitempty"`
	RetryMaxAttempts         int     `json:"retry_max_attempts,omitempty"`
	RetryJitter              float64 `json:"retry_jitter,omitempty"`
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.AWSRegion == "" {
		return fmt.Errorf("aws_region is required")
	}
	if c.TopicARN == "" {
		return fmt.Errorf("topic_arn is required")
	}
	if c.TopicType != "" && c.TopicType != "standard" && c.TopicType != "fifo" {
		return fmt.Errorf("topic_type must be 'standard' or 'fifo'")
	}
	if c.DeliveryMode != "" && c.DeliveryMode != "async" && c.DeliveryMode != "sync" {
		return fmt.Errorf("delivery_mode must be 'async' or 'sync'")
	}
	if c.BatchSize < 1 || c.BatchSize > 10 {
		return fmt.Errorf("batch_size must be between 1 and 10")
	}
	if c.MaxInFlight < 1 {
		return fmt.Errorf("max_in_flight must be at least 1")
	}
	return nil
}

// Client creates an AWS SNS client.
func (c *Config) Client(ctx context.Context) (snsiface.SNSAPI, error) {
	awsConfig := &aws.Config{
		Region: aws.String(c.AWSRegion),
	}

	if c.AccessKeyID != "" && c.SecretAccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(c.AccessKeyID, c.SecretAccessKey, "")
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("creating AWS session: %w", err)
	}

	// Handle role assumption if configured
	if c.AssumeRoleARN != "" {
		stsClient := sts.New(sess)
		result, err := stsClient.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         aws.String(c.AssumeRoleARN),
			RoleSessionName: aws.String("materialize-aws-sns"),
		})
		if err != nil {
			return nil, fmt.Errorf("assuming role %s: %w", c.AssumeRoleARN, err)
		}

		sess, err = session.NewSession(&aws.Config{
			Region:      aws.String(c.AWSRegion),
			Credentials: credentials.NewStaticCredentials(*result.Credentials.AccessKeyId, *result.Credentials.SecretAccessKey, *result.Credentials.SessionToken),
		})
		if err != nil {
			return nil, fmt.Errorf("creating session with assumed role: %w", err)
		}

		_ = result // Use the result to avoid unused variable error
	}

	return sns.New(sess), nil
}

// Resource represents a binding resource configuration.
type Resource struct {
	TopicName  string `json:"topic"`
	Identifier string `json:"identifier,omitempty"`
}

// Validate validates the resource configuration.
func (r Resource) Validate() error {
	if r.TopicName == "" {
		return fmt.Errorf("missing topic name")
	}
	return nil
}

// TopicBinding represents a topic binding.
type TopicBinding struct {
	Identifier string
	TopicName  string
}

// Driver represents the AWS SNS driver.
type Driver struct{}

// NewDriver creates a new AWS SNS driver.
func NewDriver() *Driver {
	return &Driver{}
}

// ResolveEndpointConfig parses and validates endpoint configuration.
func ResolveEndpointConfig(configJSON json.RawMessage) (*Config, error) {
	cfg := &Config{
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
	}

	if err := json.Unmarshal(configJSON, cfg); err != nil {
		return nil, fmt.Errorf("parsing SNS config: %w", err)
	}

	return cfg, cfg.Validate()
}

// ResolveResourceConfig parses and validates resource configuration.
func ResolveResourceConfig(resourceJSON json.RawMessage) (*Resource, error) {
	var res Resource
	if err := json.Unmarshal(resourceJSON, &res); err != nil {
		return nil, fmt.Errorf("parsing resource config: %w", err)
	}

	return &res, res.Validate()
}
