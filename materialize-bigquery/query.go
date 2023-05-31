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
	query.Location = c.config.Region
	// Add parameters
	for _, p := range parameters {
		query.Parameters = append(query.Parameters, bigquery.QueryParameter{Value: p})
	}

	return query
}

const (
	maxAttempts            = 10
	initialBackoff float64 = 200 // Milliseconds
	maxBackoff             = time.Duration(60 * time.Second)
)

// runQuery will run a query and return the completed job.
func (c client) runQuery(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
	var backoff = initialBackoff
	var job *bigquery.Job
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		job, err := query.Run(ctx)
		if err != nil {
			return nil, fmt.Errorf("run: %w", err)
		}

		// Weirdness ahead: if `err != nil`, then `status` might be nil. But if `err == nil`, then
		// there might still have been an error reported by `status.Err()`. We always want both the
		// err and the status so that we can check both. When `err != nil`, the status may still
		// have some helpful info to log.
		status, err := job.Wait(ctx)
		if status == nil {
			status = job.LastStatus()
		}
		if err == nil {
			err = status.Err()
		}

		if err != nil {
			// Is this a terminal error?

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
			if _, ok := err.(*googleapi.Error); ok { // Sanity check that this is actually an error from the googleapi package
				if strings.Contains(err.Error(), "Transaction is aborted due to concurrent update against table") || strings.Contains(err.Error(), "Could not serialize access to table") {
					backoff *= math.Pow(2, 1+rand.Float64())
					delay := time.Duration(backoff * float64(time.Millisecond))
					if delay > maxBackoff {
						delay = maxBackoff
					}

					log.WithFields(log.Fields{
						"attempt":   attempt,
						"jobStatus": status,
						"error":     err,
						"delay":     delay.String(),
					}).Warn("job failed (will retry)")

					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After(delay):
						continue
					}
				}
			}

			return nil, err
		}

		// I think this is just documenting the assumption that the job must always be Done after
		// Wait returns. I don't think we should ever hit this condition.
		if !status.Done() {
			return job, fmt.Errorf("query not done")
		}

		return job, nil
	}
	log.WithField("error", err).Error("job failed, exhausted retries")
	return job, fmt.Errorf("exhausted retries: %w", err)
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
