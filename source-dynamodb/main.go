package main

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
)

type config struct {
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for capturing from the DynamoDB table." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for capturing from the DynamoDB table." jsonschema_extras:"secret=true,order=2"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the DynamoDB table." jsonschema_extras:"order=3"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	BackfillSegments int    `json:"backfillSegments,omitempty" jsonschema:"title=Backfill Table Segments,description=Number of segments to use for backfill table scans. Has no effect if changed after the backfill has started."`
	ScanLimit        int    `json:"scanLimit,omitempty" jsonschema:"title=Scan Limit,description=Limit the number of items to evaluate for each table backfill scan request."`
	Endpoint         string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=The AWS endpoint URI to connect to. Use if you're capturing from a compatible API that isn't provided by AWS."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Advanced.BackfillSegments < 0 {
		return fmt.Errorf("backfillSegments cannot be negative")
	}
	if c.Advanced.ScanLimit < 0 {
		return fmt.Errorf("scanLimit cannot be negative")
	}

	return nil
}

type resource struct {
	Table         string `json:"table" jsonschema:"title=Table Name,description=The name of the table to be captured."`
	RcuAllocation int    `json:"rcuAllocation,omitempty" jsonschema:"title=RCU Allocation,description=Read capacity units the capture will attempt to consume during the table backfill. Leave blank to automatically determine based on the provisioned capacity of the table."`
}

func (r *resource) Validate() error {
	var requiredProperties = [][]string{
		{"table", r.Table},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if r.RcuAllocation < 0 {
		return fmt.Errorf("rcuAllocation cannot be negative")
	}

	return nil
}

func (c *config) toClient(ctx context.Context) (*client, error) {
	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Region),
	}

	if c.Advanced.Endpoint != "" {
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{URL: c.Advanced.Endpoint}, nil
		})

		opts = append(opts, awsConfig.WithEndpointResolverWithOptions(customResolver))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	return &client{
		db:     dynamodb.NewFromConfig(awsCfg),
		stream: dynamodbstreams.NewFromConfig(awsCfg),
	}, nil
}

type client struct {
	db     *dynamodb.Client
	stream *dynamodbstreams.Client
}

func main() {
	boilerplate.RunMain(new(driver))
}
