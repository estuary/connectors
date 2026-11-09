package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"testing/synctest"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
)

func invalidQuery(message string) *googleapi.Error {
	return &googleapi.Error{
		Code:    http.StatusBadRequest,
		Message: message,
		Errors:  []googleapi.ErrorItem{{Reason: "invalidQuery", Message: message}},
	}
}

func TestClassifyJob(t *testing.T) {
	var concurrentUpdate = invalidQuery("Transaction is aborted due to concurrent update against table project.dataset.table.")

	for _, tt := range []struct {
		name string
		err  error
		want retry.Outcome
	}{
		{
			name: "concurrent update",
			err:  &jobFailedError{concurrentUpdate},
			want: retry.Retryable,
		},
		{
			name: "serialization conflict",
			err:  &jobFailedError{invalidQuery("Could not serialize access to table project.dataset.table due to concurrent update")},
			want: retry.Retryable,
		},
		{
			name: "transient execution failure",
			err:  &jobFailedError{invalidQuery("The job encountered an error during execution. Retrying the job may solve the problem.")},
			want: retry.Retryable,
		},
		{
			name: "table update rate limit",
			err:  &jobFailedError{invalidQuery("Exceeded rate limits: too many table update operations for this table.")},
			want: retry.Retryable,
		},
		{
			name: "job rate limit",
			err: &jobFailedError{&googleapi.Error{
				Code:   http.StatusForbidden,
				Errors: []googleapi.ErrorItem{{Reason: "jobRateLimitExceeded"}},
			}},
			want: retry.Retryable,
		},
		{
			name: "job failed on submission with a rate limit",
			err:  &jobFailedError{&bigquery.Error{Reason: "rateLimitExceeded", Message: "Exceeded rate limits"}},
			want: retry.Retryable,
		},
		{
			name: "job failed on submission with another error",
			err:  &jobFailedError{&bigquery.Error{Reason: "invalidQuery", Message: "Syntax error"}},
			want: retry.Fatal,
		},
		{
			name: "other failed job",
			err:  &jobFailedError{invalidQuery("Not found: Table project:dataset.table was not found")},
			want: retry.Fatal,
		},
		{
			// The job may exist and still be running.
			name: "error submitting the job",
			err:  fmt.Errorf("run: %w", concurrentUpdate),
			want: retry.Fatal,
		},
		{
			name: "cancelled while waiting for the job",
			err:  &jobFailedError{context.Canceled},
			want: retry.Fatal,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, classifyJob(tt.err))
		})
	}
}

func TestClassifyMetadata(t *testing.T) {
	require.Equal(t, retry.Retryable, classifyMetadata(fmt.Errorf("getting: %w", &googleapi.Error{Code: http.StatusNotFound})))
	require.Equal(t, retry.Fatal, classifyMetadata(&googleapi.Error{Code: http.StatusForbidden}))
	require.Equal(t, retry.Fatal, classifyMetadata(errors.New("something else")))
}

func TestRetryPolicies(t *testing.T) {
	var notFound = &googleapi.Error{Code: http.StatusNotFound}
	var conflict = &jobFailedError{invalidQuery("Transaction is aborted due to concurrent update against table t.")}

	for _, tt := range []struct {
		name   string
		policy retry.Policy
		err    error
		calls  int
	}{
		{"job conflict", jobRetry, conflict, jobRetry.MaxAttempts},
		{"job submission error", jobRetry, fmt.Errorf("run: %w", conflict.err), 1},
		{"metadata not found", metadataRetry, notFound, metadataRetry.MaxAttempts},
		{"metadata forbidden", metadataRetry, &googleapi.Error{Code: http.StatusForbidden}, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls int
				var err = tt.policy.Do(t.Context(), "op", func(context.Context) error {
					calls++
					return tt.err
				})
				require.ErrorIs(t, err, tt.err)
				require.Equal(t, tt.calls, calls)
			})
		})
	}
}
