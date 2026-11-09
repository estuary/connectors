package connector

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// sendRetry repeats webhook requests, including ones the receiver may already
// have processed. A failed transaction is replayed in full, so retrying the
// one request re-sends less than failing would.
var sendRetry = retry.Policy{
	Classify:       classify,
	RetryAmbiguous: true,
	MaxAttempts:    11,
	InitialBackoff: 500 * time.Millisecond,
	MaxBackoff:     10 * time.Second,
	Jitter:         0.5,
}

// statusError is a response with a non-2xx status.
type statusError struct {
	code    int
	address string
}

func (e *statusError) Error() string {
	return fmt.Sprintf("unexpected webhook response code %d from %s", e.code, e.address)
}

func classify(err error) retry.Outcome {
	if e, ok := errors.AsType[*statusError](err); ok {
		if e.code == http.StatusTooManyRequests {
			return retry.Retryable
		}
		// The receiver may have acted on the request whatever status it
		// returned.
		return retry.Ambiguous
	}
	return retry.Network(err)
}
