package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/estuary/connectors/go-types/parser"
)

type Config struct {
	Endpoint           string         `json:"endpoint"`
	Bucket             string         `json:"bucket"`
	Prefix             string         `json:"prefix"`
	MatchKeys          string         `json:"matchKeys"`
	Region             string         `json:"region"`
	AWSAccessKeyID     string         `json:"awsAccessKeyId"`
	AWSSecretAccessKey string         `json:"awsSecretAccessKey"`
	Parser             *parser.Config `json:"parser"`
}

func (c *Config) Validate() error {
	if c.Region == "" && c.Endpoint == "" {
		return fmt.Errorf("must supply one of 'region' or 'endpoint'")
	}
	if c.AWSAccessKeyID == "" {
		return fmt.Errorf("missing awsAccessKeyId")
	}
	if c.AWSSecretAccessKey == "" {
		return fmt.Errorf("missing awsSecretAccessKey")
	}
	return nil
}

func configJSONSchema(parserSchema json.RawMessage) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title":   "S3 Source Spec",
		"type":    "object",
		"required": {
			"awsAccessKeyId",
			"awsSecretAccessKey"
		},
		"properties": {
			"bucket": {
				"type":        "string",
				"title":       "Bucket",
				"description": "Name of the S3 bucket"
			},
			"prefix": {
				"type":        "string",
				"title":       "Prefix",
				"description": "Prefix within the bucket to capture from."
			},
			"matchKeys": {
				"type":        "string",
				"title":       "Match Keys",
				"format":      "regex",
				"description": "Filter applied to all object keys under the prefix. If provided, only objects whose key (relative to the prefix) matches this regex will be read. For example, you can use \".*\\.json\" to only capture json files."
			},
			"parser": parserSchema,
			"region": {
				"type":        "string",
				"title":       "AWS Region",
				"description": "The name of the AWS region where the S3 stream is located",
				"default":     "us-east-1"
			},
			"endpoint": {
				"type":        "string",
				"title":       "AWS Endpoint",
				"description": "The AWS endpoint URI to connect to, useful if you're capturing from a S3-compatible API that isn't provided by AWS"
			},
			"awsAccessKeyId": {
				"type":        "string",
				"title":       "AWS Access Key ID",
				"description": "Part of the AWS credentials that will be used to connect to S3",
				"default":     "example-aws-access-key-id"
			},
			"awsSecretAccessKey": {
				"type":        "string",
				"title":       "AWS Secret Access Key",
				"description": "Part of the AWS credentials that will be used to connect to S3",
				"default":     "example-aws-secret-access-key"
			},
			"shardRange": {
				"type": "object",
				"properties": {
					"end": {
						"type":        "string",
						"pattern":     "^[0-9a-fA-F]{8}$",
						"title":       "Partition range begin",
						"description": "Unsigned 32 bit integer represented as a hexidecimal string, which is used to determine which partitions this instance will be responsible for"
					}
					"begin": {
						"type":        "string",
						"pattern":     "^[0-9a-fA-F]{8}$",
						"title":       "Partition range begin",
						"description": "Unsigned 32 bit integer represented as a hexidecimal string, which is used to determine which partitions this instance will be responsible for"
					}
				}
			}
		}
    `))
}

func connect(ctx context.Context, config *Config) (*s3.S3, error) {
	var err = config.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	var creds = credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecretAccessKey, "")
	var c = aws.NewConfig().WithCredentials(creds)
	if config.Region != "" {
		c = c.WithRegion(config.Region)
	}
	if config.Endpoint != "" {
		c = c.WithEndpoint(config.Endpoint)
	}

	awsSession, err := session.NewSession(c)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	return s3.New(awsSession), nil
}

func parseConfigAndConnect(ctx context.Context, configFile airbyte.ConfigFile) (config Config, client *s3.S3, err error) {
	err = configFile.ConfigFile.Parse(&config)
	if err != nil {
		err = fmt.Errorf("parsing config file: %w", err)
		return
	}
	client, err = connect(ctx, &config)
	if err != nil {
		err = fmt.Errorf("failed to connect: %w", err)
	}
	return
}
