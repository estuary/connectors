package main

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"golang.org/x/sync/errgroup"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Config represents the fully merged endpoint configuration for Kinesis.
type Config struct {
	Region             string `json:"region" jsonschema:"title=AWS Region,description=The name of the AWS region where the Kinesis stream is located"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=AWS Access Key ID,description=Part of the AWS credentials that will be used to connect to Kinesis"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=AWS Secret Access Key,description=Part of the AWS credentials that will be used to connect to Kinesis" jsonschema_extras:"secret=true"`

	Advanced advancedConfig `json:"advanced,omitempty"`
}

type advancedConfig struct {
	Endpoint string `json:"endpoint,omitempty" jsonschema:"title=AWS Endpoint,description=The AWS endpoint URI to connect to (useful if you're capturing from a kinesis-compatible API that isn't provided by AWS)"`
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

func getAwsCfg(ctx context.Context, cfg *Config) (*aws.Config, error) {
	var err = cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create HTTP client with explicit timeouts
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 20 * time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
		},
	}

	// Session token can be provided via environment variable for testing with temporary credentials
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, sessionToken),
		),
		awsConfig.WithRegion(cfg.Region),
		awsConfig.WithHTTPClient(httpClient),
		awsConfig.WithRetryer(func() aws.Retryer {
			// Bump up the number of retry maximum attempts from the default of 3. The maximum retry
			// duration is 20 seconds, so this gives us around 5 minutes of retrying retryable
			// errors before giving up and crashing the connector.
			//
			// Ref: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None // rely on the standard error backoff for rate limiting
				o.MaxAttempts = 3              // Reduce retries for testing
			})
		}),
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating aws config: %w", err)
	}

	return &awsCfg, nil
}

func connect(ctx context.Context, cfg *Config) (*kinesis.Client, error) {
	awsCfg, err := getAwsCfg(ctx, cfg)
	if err != nil {
		return nil, err
	}

	var clientOpts []func(*kinesis.Options)
	if cfg.Advanced.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *kinesis.Options) {
			o.BaseEndpoint = &cfg.Advanced.Endpoint
		})
	}

	return kinesis.NewFromConfig(*awsCfg, clientOpts...), nil
}

func connectGlue(ctx context.Context, cfg *Config) (*glue.Client, error) {
	awsCfg, err := getAwsCfg(ctx, cfg)
	if err != nil {
		return nil, err
	}

	var clientOpts []func(*glue.Options)
	if cfg.Advanced.Endpoint != "" {
		clientOpts = append(clientOpts, func(o *glue.Options) {
			o.BaseEndpoint = &cfg.Advanced.Endpoint
		})
	}

	return glue.NewFromConfig(*awsCfg, clientOpts...), nil
}

type kinesisStream struct {
	name string
	arn  string
}

func listStreams(ctx context.Context, client *kinesis.Client) ([]kinesisStream, error) {
	var out []kinesisStream
	var streamNames []string

	input := &kinesis.ListStreamsInput{}
	for {
		res, err := client.ListStreams(ctx, input)
		if err != nil {
			return nil, err
		}

		streamNames = append(streamNames, res.StreamNames...)

		if !*res.HasMoreStreams {
			break
		}
		input.NextToken = res.NextToken
	}

	// ListStreams only includes stream names and no ARNs, so the
	// DescribeStreamSummary API is used to get the ARN for each individual
	// stream.
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, n := range streamNames {
		n := n
		group.Go(func() error {
			desc, err := client.DescribeStreamSummary(groupCtx, &kinesis.DescribeStreamSummaryInput{
				StreamName: &n,
			})
			if err != nil {
				return err
			}

			mu.Lock()
			defer mu.Unlock()

			out = append(out, kinesisStream{
				name: n,
				arn:  *desc.StreamDescriptionSummary.StreamARN,
			})

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	slices.SortFunc(out, func(a, b kinesisStream) int { return cmp.Compare(a.name, b.name) })

	return out, nil
}
