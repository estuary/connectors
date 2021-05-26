package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/estuary/connectors/go/protocol"
	log "github.com/sirupsen/logrus"
)

// Config represents the fully merged endpoint configuration for Kinesis.
// It matches the `KinesisConfig` struct in `crates/sources/src/specs.rs`
type Config struct {
	PartitionRange     *protocol.PartitionRange `json:"partitionRange"`
	Endpoint           string                   `json:"endpoint"`
	Region             string                   `json:"region"`
	AWSAccessKeyID     string                   `json:"awsAccessKeyId"`
	AWSSecretAccessKey string                   `json:"awsSecretAccessKey"`
}

func (c *Config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("missing region")
	}
	if c.AWSAccessKeyID == "" {
		return fmt.Errorf("missing awsAccessKeyId")
	}
	if c.AWSSecretAccessKey == "" {
		return fmt.Errorf("missing awsSecretAccessKey")
	}
	return nil
}

var configJSONSchema = map[string]interface{}{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title":   "Kinesis Source Spec",
	"type":    "object",
	"required": []string{
		"stream",
		"region",
		"awsAccessKeyId",
		"awsSecretAccessKey",
	},
	"properties": map[string]interface{}{
		"region": map[string]interface{}{
			"type":        "string",
			"title":       "AWS Region",
			"description": "The name of the AWS region where the Kinesis stream is located",
			"default":     "us-east-1",
		},
		"awsAccessKeyId": map[string]interface{}{
			"type":        "string",
			"title":       "AWS Access Key ID",
			"description": "Part of the AWS credentials that will be used to connect to Kinesis",
			"default":     "example-aws-access-key-id",
		},
		"awsSecretAccessKey": map[string]interface{}{
			"type":        "string",
			"title":       "AWS Secret Access Key",
			"description": "Part of the AWS credentials that will be used to connect to Kinesis",
			"default":     "example-aws-secret-access-key",
		},
		"partitionRange": map[string]interface{}{
			"type": "object",
			"properties": map[string]interface{}{
				"end": map[string]interface{}{
					"type":        "string",
					"pattern":     "^[0-9a-fA-F]{8}$",
					"title":       "Partition range begin",
					"description": "Unsigned 32 bit integer represented as a hexidecimal string, which is used to determine which partitions this instance will be responsible for",
				},
				"begin": map[string]interface{}{
					"type":        "string",
					"pattern":     "^[0-9a-fA-F]{8}$",
					"title":       "Partition range begin",
					"description": "Unsigned 32 bit integer represented as a hexidecimal string, which is used to determine which partitions this instance will be responsible for",
				},
			},
		},
	},
}

func connect(config *Config) (*kinesis.Kinesis, error) {
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
	return kinesis.New(awsSession), nil
}

func listAllStreams(ctx context.Context, client *kinesis.Kinesis) ([]string, error) {
	var streams []string
	var lastStream *string = nil
	var limit = int64(100)
	var errBackoff = backoff{
		initialMillis: 200,
		maxMillis:     1000,
		multiplier:    1.5,
	}
	var reqNum int
	for {
		reqNum++
		log.WithField("requestNumber", reqNum).Debug("sending ListStreams request")
		var req = kinesis.ListStreamsInput{
			Limit:                    &limit,
			ExclusiveStartStreamName: lastStream,
		}
		resp, err := client.ListStreamsWithContext(ctx, &req)
		if err != nil {
			if isRetryable(err) {
				log.WithField("error", err).Warnf("error while listing streams (will retry): %T %#v", err, err)
				select {
				case <-errBackoff.nextBackoff():
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			} else {
				return nil, err
			}
		} else {
			log.WithField("responseStreamCount", len(resp.StreamNames)).Debug("got ListStreams response")
			for _, name := range resp.StreamNames {
				streams = append(streams, *name)
			}
			if resp.HasMoreStreams != nil && *resp.HasMoreStreams {
				lastStream = resp.StreamNames[len(resp.StreamNames)-1]
			} else {
				break
			}
		}
	}
	log.WithField("streamCount", len(streams)).Debug("finished listing streams successfully")
	return streams, nil
}

type backoff struct {
	initialMillis int64
	maxMillis     int64
	multiplier    float64
	currentMillis int64
}

func (b *backoff) nextBackoff() <-chan time.Time {
	if b.currentMillis == 0 {
		b.reset()
	}
	var ch = time.After(time.Duration(b.currentMillis) * time.Millisecond)
	var nextMillis = int64(float64(b.currentMillis) * b.multiplier)
	if nextMillis > b.maxMillis {
		nextMillis = b.maxMillis
	}
	b.currentMillis = nextMillis
	return ch
}
func (b *backoff) reset() {
	b.currentMillis = b.initialMillis
}

var terminalErrors = map[string]bool{
	"ResourceNotFoundException": true,
	// This means we sent an invalid request, which is likely indicative of a bug in our code.
	"InvalidArgumentException": true,
	// The kinesis client wraps context.Canceled errors. This is fine :/
	"RequestCanceled": true,
}

// Returns true if this error represents something that ought to be retried.
// Basically, this is any error except those that we're sure are terminal.
func isRetryable(err error) bool {
	switch typed := err.(type) {
	case awserr.RequestFailure:
		// Don't consider authentication or authorization errors retryable since they will require
		// human intervention.
		// It's not at all clear from their docs what error `Code` this would map to, which is why
		// this case is handled separately.
		return typed.StatusCode() != 403
	case awserr.Error:
		return !terminalErrors[typed.Code()]
	default:
		return true
	}
}
