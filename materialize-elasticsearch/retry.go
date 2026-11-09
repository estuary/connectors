package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/estuary/connectors/go/retry"
)

// requestRetry repeats data requests, including ones that may already have
// taken effect. Loads and keyed stores are idempotent. Delta stores create
// documents with generated IDs, but a failed transaction is replayed in full,
// so retrying the one request duplicates less than failing would.
var requestRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    11,
	InitialBackoff: 2 * time.Second,
	MaxBackoff:     5 * time.Second,
	Jitter:         0.5,
}

// responseError is an error response to a whole request.
type responseError struct {
	status int
	msg    string
}

func newResponseError(res *esapi.Response) *responseError {
	return &responseError{status: res.StatusCode, msg: res.String()}
}

func (e *responseError) Error() string { return e.msg }

// transportError is a request that failed without a response.
type transportError struct{ err error }

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

// bulkItemsError reports bulk items that failed with a transient per-item
// error. Those items did not land and can be re-sent.
type bulkItemsError struct {
	count int
	err   error
}

func (e *bulkItemsError) Error() string {
	return fmt.Sprintf("%d items failed: %s", e.count, e.err)
}

func (e *bulkItemsError) Unwrap() error { return e.err }

func classify(err error) retry.Outcome {
	if e, ok := errors.AsType[*responseError](err); ok {
		switch e.status {
		case http.StatusTooManyRequests, http.StatusBadGateway,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return retry.Retryable
		}
		return retry.Fatal
	} else if _, ok := errors.AsType[*bulkItemsError](err); ok {
		return retry.Retryable
	} else if _, ok := errors.AsType[*transportError](err); ok && !errors.Is(err, context.Canceled) {
		return retry.Retryable
	}
	return retry.Fatal
}
