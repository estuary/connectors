package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsHTTP "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/estuary/connectors/go/auth/iam"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/invopop/jsonschema"
)

type AuthType string

const (
	AWSAccessKey AuthType = "AWSAccessKey"
	AWSIAM       AuthType = "AWSIAM"
)

type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	AccessKeyCredentials
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Access Key", AccessKeyCredentials{}, string(AWSAccessKey)),
	}
	subSchemas = append(subSchemas,
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)))

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(AWSAccessKey), subSchemas...)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case AWSAccessKey:
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'aws_access_key_id'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'aws_secret_access_key'")
		}
		return nil
	case AWSIAM:
		if err := c.ValidateIAM(); err != nil {
			return err
		}
		return nil
	}
	return fmt.Errorf("unknown 'auth_type'")
}

// Since the AccessKeyCredentials and IAMConfig have conflicting JSON field names, only parse
// the embedded struct of interest.
func (c *CredentialsConfig) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		AuthType AuthType `json:"auth_type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	c.AuthType = discriminator.AuthType

	switch c.AuthType {
	case AWSAccessKey:
		return json.Unmarshal(data, &c.AccessKeyCredentials)
	case AWSIAM:
		return json.Unmarshal(data, &c.IAMConfig)
	}
	return fmt.Errorf("unexpected auth_type: %s", c.AuthType)
}

type AccessKeyCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id" jsonschema:"title=AWS Access Key ID,description=AWS Access Key ID for capturing from the DynamoDB table." jsonschema_extras:"order=1"`
	AWSSecretAccessKey string `json:"aws_secret_access_key" jsonschema:"title=AWS Secret Access key,description=AWS Access Key ID for capturing from the DynamoDB table." jsonschema_extras:"secret=true,order=2"`
}

type config struct {
	AWSAccessKeyID     string             `json:"awsAccessKeyId" jsonschema:"-"`
	AWSSecretAccessKey string             `json:"awsSecretAccessKey" jsonschema:"-" jsonschema_extras:"secret=true"`
	Region             string             `json:"region" jsonschema:"title=Region,description=Region of the DynamoDB table." jsonschema_extras:"order=1"`
	Credentials        *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-iam-auth=true,order=2"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	BackfillSegments int    `json:"backfillSegments,omitempty" jsonschema:"title=Backfill Table Segments,description=Number of segments to use for backfill table scans. Has no effect if changed after the backfill has started."`
	ScanLimit        int    `json:"scanLimit,omitempty" jsonschema:"title=Scan Limit,description=Limit the number of items to evaluate for each table backfill scan request."`
	Endpoint         string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=The AWS endpoint URI to connect to. Use if you're capturing from a compatible API that isn't provided by AWS."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Credentials == nil {
		if c.AWSAccessKeyID == "" {
			return errors.New("missing 'awsAccessKeyId'")
		}
		if c.AWSSecretAccessKey == "" {
			return errors.New("missing 'awsSecretAccessKey'")
		}
	} else if err := c.Credentials.Validate(); err != nil {
		return err
	}

	if c.Advanced.BackfillSegments < 0 {
		return fmt.Errorf("backfillSegments cannot be negative")
	}
	if c.Advanced.ScanLimit < 0 {
		return fmt.Errorf("scanLimit cannot be negative")
	}

	return nil
}

func (c *config) CredentialsProvider(ctx context.Context) (aws.CredentialsProvider, error) {
	if c.Credentials == nil {
		return credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""), nil
	}

	switch c.Credentials.AuthType {
	case AWSAccessKey:
		return credentials.NewStaticCredentialsProvider(
			c.Credentials.AWSAccessKeyID, c.Credentials.AWSSecretAccessKey, ""), nil
	case AWSIAM:
		return c.Credentials.IAMTokens.AWSCredentialsProvider()
	}
	return nil, errors.New("unknown 'auth_type'")
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
	credProvider, err := c.CredentialsProvider(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(credProvider),
		awsConfig.WithRegion(c.Region),
		awsConfig.WithRetryer(func() aws.Retryer {
			// Bump up the number of retry maximum attempts from the default of 3. The maximum retry
			// duration is 20 seconds, so this gives us around 5 minutes of retrying retryable
			// errors before giving up and crashing the connector.
			//
			// Ref: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/
			return retry.AddWithMaxAttempts(retry.NewStandard(), 20)
		}),
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
	awsHTTP.DefaultHTTPTransportMaxIdleConns = 100        // From 100.
	awsHTTP.DefaultHTTPTransportMaxIdleConnsPerHost = 100 // From 10.
	boilerplate.RunMain(new(driver))
}
