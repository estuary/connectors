package connector

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

var (
	errNotFound = errors.New("not found")
)

// query executes a query against BigQuery and returns the completed job.
func (c client) query(ctx context.Context, queryString string, parameters ...interface{}) (*bigquery.Job, error) {
	return c.runQuery(ctx, c.newQuery(queryString, parameters...))
}

// newQuery returns a preconfigured *bigquery.Query.
func (c client) newQuery(queryString string, parameters ...interface{}) *bigquery.Query {

	// Create the query
	query := c.bigqueryClient.Query(queryString)
	query.Location = c.cfg.Region
	// Add parameters
	for _, p := range parameters {
		query.Parameters = append(query.Parameters, bigquery.QueryParameter{Value: p})
	}

	return query
}

const (
	maxAttempts            = 30
	initialBackoff float64 = 200       // Milliseconds
	maxBackoff             = 60 * 1000 // Milliseconds
)

// runQuery will run a query and return the completed job.
func (c client) runQuery(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
	var backoff = initialBackoff
	var job *bigquery.Job
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		backoff *= math.Pow(2, 1+rand.Float64())
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		retryDelay := time.Duration(backoff * float64(time.Millisecond))

		job, err = query.Run(ctx)
		if err != nil {
			return nil, fmt.Errorf("run: %w", err)
		}

		// Some queries may immediately fail, such as table alteration rate
		// limits. For these, `job.Wait` will hang forever, so we must check to
		// see if the job has already failed.
		if initialStatus, err := job.Status(ctx); err != nil {
			return nil, fmt.Errorf("getting initialStatus for job: %w", err)
		} else if err := initialStatus.Err(); err != nil {
			if err := maybeRetry(ctx, err, attempt, retryDelay, initialStatus); err != nil {
				return nil, err
			}
			continue
		}

		// Weirdness ahead: if `err != nil`, then `status` might be nil. But if `err == nil`, then
		// there might still have been an error reported by `status.Err()`. We always want both the
		// err and the status so that we can check both. When `err != nil`, the status may still
		// have some helpful info to log.
		var status *bigquery.JobStatus
		status, err = job.Wait(ctx)
		if status == nil {
			status = job.LastStatus()
		}
		if err == nil {
			err = status.Err()
		}
		if err != nil {
			if err := maybeRetry(ctx, err, attempt, retryDelay, status); err != nil {
				return nil, err
			}
			continue
		}

		// I think this is just documenting the assumption that the job must always be Done after
		// Wait returns. I don't think we should ever hit this condition.
		if !status.Done() {
			return job, fmt.Errorf("query not done")
		}

		return job, nil
	}

	return job, fmt.Errorf("exhausted retries: %w", err)
}

// maybeRetry will return a `nil` error after a period of time if the provided
// `err` is retryable. Otherwise it will return the provided error immediately.
func maybeRetry(ctx context.Context, err error, attempt int, delay time.Duration, status *bigquery.JobStatus) error {
	doDelay := func() error {
		ll := log.WithFields(log.Fields{
			"attempt":   attempt,
			"jobStatus": status,
			"error":     err,
			"delay":     delay.String(),
		})

		if attempt > 10 {
			ll.Info("job failed (will retry)")
		} else {
			ll.Debug("job failed (will retry)")
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			return nil
		}
	}

	// We need to retry errors due to concurrent updates to the same table, but
	// unfortunately there's no good way to identify such errors. The status code of that
	// error is 400 and the status is "invalidQuery" (see
	// https://cloud.google.com/bigquery/docs/error-messages), which also applies to several
	// other scenarios like the instance being fenced off (from our use of RAISE), a table
	// being referenced by the query not existing (which is for some reason not a 404), etc.

	// Because of this we match on substrings in the error message to determine if a retry
	// should be attempted. The two errors that may be encountered from concurrent shards
	// operating in the same dataset have the error strings "Transaction is aborted due to
	// concurrent update against table ..." (most common, seemingly), or "Could not
	// serialize access to table ...". We retry only if the the error string contains these
	// strings.

	// Short term rate limit errors can also be retried using the same exponential backoff
	// strategy. These kinds of errors can always be identified by their "Reason" being
	// "jobRateLimitExceeded".
	if e, ok := err.(*googleapi.Error); ok {
		if strings.Contains(err.Error(), "Transaction is aborted due to concurrent update against table") ||
			strings.Contains(err.Error(), "Could not serialize access to table") ||
			strings.Contains(err.Error(), "The job encountered an error during execution. Retrying the job may solve the problem.") ||
			(len(e.Errors) == 1 && e.Errors[0].Reason == "jobRateLimitExceeded") {
			return doDelay()
		}
	}

	// A *bigquery.Error is returned if the job immediately has an error in its
	// status. The only way I have found this to happen from our usage is from
	// table alterations exceeding a rate limit.
	if e, ok := err.(*bigquery.Error); ok {
		if e.Reason == "rateLimitExceeded" {
			return doDelay()
		}
	}

	// Not a retryable error.
	return err
}

// fetchOne will fetch one row of data from a job and scan it into dest.
func (c client) fetchOne(ctx context.Context, job *bigquery.Job, dest interface{}) error {

	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	if err = it.Next(dest); err == iterator.Done {
		return errNotFound
	} else if err != nil {
		return fmt.Errorf("next: %w", err)
	}

	return nil

}
