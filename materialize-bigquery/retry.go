package connector

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/retry"
	"google.golang.org/api/googleapi"
)

// jobRetry runs a query again, as a new job, after its job failed on a
// conflict with a concurrent update or on a rate limit. A failed job took no
// effect, so re-running it is safe for any query.
var jobRetry = retry.Policy{
	Classify:       classifyJob,
	MaxAttempts:    30,
	InitialBackoff: 500 * time.Millisecond,
	MaxBackoff:     time.Minute,
	Jitter:         0.5,
	QuietRetries:   10,
}

// metadataRetry rides out BigQuery's eventually consistent metadata, which
// can answer 404 for a table shortly after the DDL that created or altered it.
var metadataRetry = retry.Policy{
	Classify:       classifyMetadata,
	MaxAttempts:    5,
	InitialBackoff: time.Second,
	MaxBackoff:     4 * time.Second,
	Jitter:         0.5,
	QuietRetries:   4,
}

// jobFailedError is the error a job finished with. Only these are retried:
// after an error submitting a job or fetching its status, the job may exist
// and still be running.
type jobFailedError struct{ err error }

func (e *jobFailedError) Error() string { return e.err.Error() }
func (e *jobFailedError) Unwrap() error { return e.err }

// retryableJobMessages identify failed jobs that succeed when run again.
// Concurrent update conflicts and per-table metadata rate limits fail with a
// 400 "invalidQuery", which also covers many errors a retry won't fix, so
// they are told apart by message.
var retryableJobMessages = []string{
	"Transaction is aborted due to concurrent update against table",
	"Could not serialize access to table",
	"The job encountered an error during execution. Retrying the job may solve the problem.",
	"Exceeded rate limits",
}

func classifyJob(err error) retry.Outcome {
	if _, ok := errors.AsType[*jobFailedError](err); !ok {
		return retry.Fatal
	}
	if e, ok := errors.AsType[*googleapi.Error](err); ok {
		var text = e.Error()
		if slices.ContainsFunc(retryableJobMessages, func(msg string) bool { return strings.Contains(text, msg) }) ||
			(len(e.Errors) == 1 && e.Errors[0].Reason == "jobRateLimitExceeded") {
			return retry.Retryable
		}
	}
	// A job that failed as soon as it was submitted reports a *bigquery.Error.
	if e, ok := errors.AsType[*bigquery.Error](err); ok && e.Reason == "rateLimitExceeded" {
		return retry.Retryable
	}
	return retry.Fatal
}

func classifyMetadata(err error) retry.Outcome {
	if e, ok := errors.AsType[*googleapi.Error](err); ok && e.Code == http.StatusNotFound {
		return retry.Retryable
	}
	return retry.Fatal
}
