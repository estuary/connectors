package connector

import (
	"errors"
	"fmt"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// unprocessedRetry re-sends the requests of a batch that DynamoDB left
// unprocessed because they would have exceeded the table's throughput. Errors
// from the call itself have already been retried by the AWS SDK's retryer.
var unprocessedRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    30,
	InitialBackoff: 20 * time.Millisecond,
	MaxBackoff:     time.Second,
	Jitter:         0.5,
	QuietRetries:   14,
}

// unprocessedError reports the requests of a batch that DynamoDB did not
// process.
type unprocessedError struct {
	operation string
	count     int
}

func (e *unprocessedError) Error() string {
	return fmt.Sprintf("%s left %d requests unprocessed", e.operation, e.count)
}

func classify(err error) retry.Outcome {
	if _, ok := errors.AsType[*unprocessedError](err); ok {
		return retry.Retryable
	}
	return retry.Fatal
}
