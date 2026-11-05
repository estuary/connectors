package connector

import (
	"errors"
	"fmt"
	"time"

	"github.com/estuary/connectors/go/retry"
)

// putEventsRetry re-sends the entries of a PutEvents call that EventBridge
// rejected individually with a transient error code; rejected entries were
// not published. Errors from the call itself have already been retried by the
// AWS SDK's retryer. Entries still failing after its bounds fail the
// transaction, which the runtime retries in full.
var putEventsRetry = retry.Policy{
	Classify:       classify,
	MaxAttempts:    4,
	InitialBackoff: 200 * time.Millisecond,
	Jitter:         0.5,
}

// failedEntriesError reports the entries of a PutEvents call that EventBridge
// rejected with a transient error code, and the first of their errors.
type failedEntriesError struct {
	count         int
	code, message string
}

func (e *failedEntriesError) Error() string {
	return fmt.Sprintf("%d entries failed, first with %s: %s", e.count, e.code, e.message)
}

func classify(err error) retry.Outcome {
	if _, ok := errors.AsType[*failedEntriesError](err); ok {
		return retry.Retryable
	}
	return retry.Fatal
}

// retryableCode returns true for EventBridge per-entry error codes that
// indicate transient failures. These codes are surfaced on
// PutEventsResultEntry.ErrorCode and are stable across SDK versions.
func retryableCode(code string) bool {
	switch code {
	case "ThrottlingException", "Throttling",
		"InternalFailure", "ServiceUnavailable":
		return true
	}
	return false
}
