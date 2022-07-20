package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
func (ep *Endpoint) query(ctx context.Context, queryString string, parameters ...interface{}) (*bigquery.Job, error) {
	return ep.runQuery(ctx, ep.newQuery(queryString, parameters...))

}

// newQuery returns a preconfigured *bigquery.Query.
func (ep *Endpoint) newQuery(queryString string, parameters ...interface{}) *bigquery.Query {

	// Create the query
	query := ep.bigQueryClient.Query(queryString)
	query.Location = ep.config.Region
	// Add parameters
	for _, p := range parameters {
		query.Parameters = append(query.Parameters, bigquery.QueryParameter{Value: p})
	}

	return query

}

// runQuery will run a query and return the completed job.
func (ep *Endpoint) runQuery(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
	var maxAttempts = 5
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

		var logEntry = log.WithFields(log.Fields{
			"attempt":   attempt,
			"jobStatus": status,
			"error":     err,
		})
		if err != nil {
			// Is this a terminal error?
			// We need to retry errors due to concurrent updates to the same table, but
			// unfortunately there's no good way to identify such errors. The status code of
			// that error is 400, but that also applies to many other errors. So we'll retry
			// anything with a 400 status, but with a fairly low limit on the number of attempts,
			// since these errors may actually be terminal.
			if e, ok := err.(*googleapi.Error); ok {
				if e.Code == 404 {
					logEntry.Warn("project, dataset, or table was not found")
					return nil, errNotFound
				} else if e.Code != 400 {
					logEntry.Error("job failed")
					return nil, err // The error isn't considered retryable
				}
			}
			logEntry.Warn("job failed (will retry)")
			// Wait between 50-150 milliseconds before continuing, so we have at least a little bit
			// of jitter applied to retries.
			time.Sleep(time.Duration(rand.Intn(100)+50) * time.Millisecond)
			continue
		}

		// I think this is just documenting the assumption that the job must always be Done after
		// Wait returns. I don't think we should ever hit this condition.
		if !status.Done() {
			return job, fmt.Errorf("query not done")
		}

		return job, nil
	}
	log.WithField("error", err).Error("job failed, exausted retries")
	return job, fmt.Errorf("exausted retries: %w", err)
}

// fetchOne will fetch one row of data from a job and scan it into dest.
func (ep *Endpoint) fetchOne(ctx context.Context, job *bigquery.Job, dest interface{}) error {

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
