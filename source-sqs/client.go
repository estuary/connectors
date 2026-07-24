package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// minHTTPPoolSize is the floor for idle HTTP connections to SQS. With the
// SDK's default of 10 idle connections per host, most calls from the
// receiver and delete-worker fleets would pay a fresh TLS handshake, which
// wrecks throughput.
const minHTTPPoolSize = 128

// buildClient constructs an SQS client. concurrencyHint sizes the HTTP
// connection pool and should approximate the number of concurrent API calls
// (receivers plus delete workers). Unary RPCs like Spec/Validate/Discover
// pass zero to get the floor.
func buildClient(ctx context.Context, cfg *config, concurrencyHint int) (*sqs.Client, error) {
	creds, err := cfg.CredentialsProvider()
	if err != nil {
		return nil, fmt.Errorf("creating credentials provider: %w", err)
	}

	poolSize := max(minHTTPPoolSize, concurrencyHint)
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConns = poolSize
		t.MaxIdleConnsPerHost = poolSize
	})

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(creds),
		awsConfig.WithRegion(cfg.Region),
		awsConfig.WithHTTPClient(httpClient),
		awsConfig.WithRetryer(func() aws.Retryer {
			// 5 attempts vs the SDK default of 3 for resilience against
			// transient throttling. No client-side rate limiter, since
			// receive and delete volume is already bounded by the worker
			// fleets and the standard error backoff handles throttling
			// responses.
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.RateLimiter = ratelimit.None
				o.MaxAttempts = 5
			})
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("creating AWS config: %w", err)
	}

	return sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if cfg.Advanced.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Advanced.Endpoint)
		}
	}), nil
}
