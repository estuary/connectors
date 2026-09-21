package main

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

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

	// Session token can be provided via environment variable for testing with temporary credentials
	sessionToken := os.Getenv("AWS_SESSION_TOKEN")

	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, sessionToken),
		),
		awsConfig.WithRegion(cfg.Region),
		awsConfig.WithRetryer(func() aws.Retryer {
			// Bump up the number of retry maximum attempts from the default of 3. The maximum retry
			// duration is 20 seconds, so this gives us around 5 minutes of retrying retryable
			// errors before giving up and crashing the connector.
			//
			// Ref: https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts/
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None // rely on the standard error backoff for rate limiting
				o.MaxAttempts = 20
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
	var streamNames []string
	var streamARNs = make(map[string]string)

	input := &kinesis.ListStreamsInput{}
	for {
		res, err := client.ListStreams(ctx, input)
		if err != nil {
			return nil, err
		}

		streamNames = append(streamNames, res.StreamNames...)

		// ListStreams reports a summary for each stream which includes its ARN.
		// StreamSummaries is not a required field of the response
		// though, so any stream that doesn't have one is described via
		// DescribeStreamSummary
		for _, s := range res.StreamSummaries {
			if s.StreamName != nil && s.StreamARN != nil {
				streamARNs[*s.StreamName] = *s.StreamARN
			}
		}

		if !aws.ToBool(res.HasMoreStreams) {
			break
		} else if res.NextToken != nil {
			input.NextToken = res.NextToken
		} else if len(streamNames) > 0 {
			// Older versions of the API paginate by asking for streams
			// following the last one that was returned.
			lastName := streamNames[len(streamNames)-1]
			input.NextToken = nil
			input.ExclusiveStartStreamName = &lastName
		} else {
			// There's no way to ask for the next page, so stop
			// here rather than requesting the same page forever.
			break
		}
	}

	// DescribeStreamSummary is limited to 20 transactions per second for the
	// entire AWS account, and that budget is shared with everything else the
	// account is doing, so these requests are rate limited to well under it.
	// They are also usually not needed at all since ListStreams almost always
	// reports the ARN of every stream it returns.
	var mu sync.Mutex
	limiter := rate.NewLimiter(10, 10)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, n := range streamNames {
		if _, ok := streamARNs[n]; ok {
			continue
		}

		group.Go(func() error {
			if err := limiter.Wait(groupCtx); err != nil {
				return fmt.Errorf("waiting for rate limiter: %w", err)
			}

			desc, err := client.DescribeStreamSummary(groupCtx, &kinesis.DescribeStreamSummaryInput{
				StreamName: &n,
			})
			if err != nil {
				return fmt.Errorf("describing stream %q: %w", n, err)
			} else if desc.StreamDescriptionSummary == nil || desc.StreamDescriptionSummary.StreamARN == nil {
				return fmt.Errorf("stream %q was described without an ARN", n)
			}

			mu.Lock()
			defer mu.Unlock()
			streamARNs[n] = *desc.StreamDescriptionSummary.StreamARN

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	var out = make([]kinesisStream, 0, len(streamNames))
	for _, n := range streamNames {
		out = append(out, kinesisStream{name: n, arn: streamARNs[n]})
	}

	slices.SortFunc(out, func(a, b kinesisStream) int { return cmp.Compare(a.name, b.name) })

	return out, nil
}
