package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
	RetryAmbiguous: true,
	MaxAttempts:    11,
	InitialBackoff: 2 * time.Second,
	MaxBackoff:     5 * time.Second,
	Jitter:         0.5,
}

// taskPollRetry polls a background task until it completes, retrying each
// check that finds it still running as well as transient failures of the
// check itself. Checks start often so that small tasks return promptly, and
// back off for tasks that run long. Every retry is logged at debug level,
// since a running task is routine and its caller logs its progress. Each
// check has its own deadline so that a hung check is retried rather than
// stalling the wait.
var taskPollRetry = retry.Policy{
	Classify:       classifyTaskPoll,
	RetryAmbiguous: true,
	MaxElapsed:     4 * time.Hour,
	InitialBackoff: 500 * time.Millisecond,
	MaxBackoff:     10 * time.Second,
	QuietRetries:   math.MaxInt,
	AttemptTimeout: time.Minute,
}

// taskRunningError is a background task that hasn't completed yet, with the
// progress it reported.
type taskRunningError struct {
	taskID  string
	total   int64
	deleted int64
}

func (e *taskRunningError) Error() string {
	return fmt.Sprintf("task %s is still running", e.taskID)
}

func classifyTaskPoll(err error) retry.Outcome {
	if _, ok := errors.AsType[*taskRunningError](err); ok {
		return retry.Retryable
	}
	return classify(err)
}

// responseError is an error response to a whole request.
type responseError struct {
	status int
	msg    string
}

// newResponseError reads the error response res. Its message is the response
// status and the error the body reports, or the whole body if it doesn't
// report one.
func newResponseError(res *esapi.Response) *responseError {
	var body []byte
	if res.Body != nil {
		body, _ = io.ReadAll(res.Body)
	}
	return &responseError{
		status: res.StatusCode,
		msg:    fmt.Sprintf("[%s] %s", res.Status(), errorBodyMessage(body)),
	}
}

// errorBodyMessage returns the error reported by the error response body as
// its type and reason. The root cause is added when it differs from the
// error, since an error that wraps others, such as a search that failed on
// every shard, describes only the failure as a whole. A body that doesn't
// report an error this way is returned as is.
func errorBodyMessage(body []byte) string {
	var parsed struct {
		Error struct {
			esError
			RootCause []esError `json:"root_cause"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil || parsed.Error.Type == "" {
		return string(body)
	}

	msg := fmt.Sprintf("%s: %s", parsed.Error.Type, parsed.Error.Reason)
	if len(parsed.Error.RootCause) > 0 && parsed.Error.RootCause[0] != parsed.Error.esError {
		cause := parsed.Error.RootCause[0]
		msg += fmt.Sprintf(" (root cause %s: %s)", cause.Type, cause.Reason)
	}
	return msg
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
		case http.StatusTooManyRequests:
			return retry.Retryable
		case http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return retry.Ambiguous
		}
		return retry.Fatal
	} else if _, ok := errors.AsType[*bulkItemsError](err); ok {
		return retry.Retryable
	} else if _, ok := errors.AsType[*transportError](err); ok {
		// The request may have been received, whatever the failure.
		if outcome := retry.Network(err); outcome == retry.Retryable || errors.Is(err, context.Canceled) {
			return outcome
		}
		return retry.Ambiguous
	}
	return retry.Fatal
}
