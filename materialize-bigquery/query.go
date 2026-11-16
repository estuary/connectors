package connector

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	m "github.com/estuary/connectors/go/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
)

var (
	errNotFound = errors.New("not found")
)

// nextJobID computes the job ID for the given attempt.
// IMPORTANT: This must never change, otherwise some data may be duplicated in
// materializations.
func nextJobID(prefix string, attempt uint32) string {
	return fmt.Sprintf("flow-%s-%010d", prefix, attempt)
}

// queryIdempotent runs the provided query with its associated files and ensures
// that it is only run a single time. Job IDs are computed deterministically
// from the prefix, and once one has succeeded it will not be run again.
func (c client) queryIdempotent(
	ctx context.Context,
	schema bigquery.Schema,
	query string,
	jobPrefix string,
	sourceURIs []string,
	tempTableName string,
) (*bigquery.JobStatus, error) {
	q := c.bigqueryClient.Query(query)
	q.Location = c.cfg.Region
	if tempTableName != "" {
		q.TableDefinitions = map[string]bigquery.ExternalData{
			tempTableName: edc(sourceURIs, schema),
		}
	}

	var attempt uint32 = 1
	for ; ; attempt++ {
		jobID := nextJobID(jobPrefix, attempt)
		q.JobID = jobID

		job, err := q.Run(ctx)
		if err != nil {
			if e, ok := errors.AsType[*googleapi.Error](err); ok && e.Code == 409 {
				// A 409 error code means that a job with the given ID already
				// exists. If a prior job with the same ID failed, we must try
				// again with a new job ID that is 1 higher than the last one.
				// If it is still running, we can wait for it to finish.
				j, err := c.bigqueryClient.JobFromIDLocation(ctx, jobID, c.cfg.Region)
				if err != nil {
					return nil, fmt.Errorf("getting job metadata for %q: %w", jobID, err)
				}

				if stat, err := j.Status(ctx); err != nil {
					return nil, fmt.Errorf("getting job status of previously submitted job %q: %w", jobID, err)
				} else if stat.Err() != nil {
					log.WithFields(log.Fields{
						"error": err,
						"jobID": jobID,
						"query": query,
					}).Warn("previously submitted job failed, will retry with new job ID")
					continue
				} else if stat.Done() {
					// This would be somewhat unusual, but could happen if the
					// connector exited for some reason after submitting a job
					// but before it could acknowledge the commit to the Flow
					// runtime.
					log.WithField("jobID", jobID).Info("previously submitted job completed successfully")
					return stat, nil
				}

				log.WithFields(log.Fields{
					"jobID": jobID,
				}).Info("waiting for previously submitted job to finish")

				stat, err := j.Wait(ctx)
				if err != nil {
					return nil, fmt.Errorf("getting job status of previously submitted job %q while waiting for it to finish: %w", jobID, err)
				} else if stat.Err() != nil {
					return nil, fmt.Errorf("waited for previously submitted job %q to finish but it resulted in an error: %w", jobID, err)
				} else if !stat.Done() {
					return nil, fmt.Errorf("interal application error: previously submitted job not done after waiting with no error")
				}

				log.WithFields(log.Fields{
					"jobID": jobID,
				}).Info("previously submitted job finished successfully after waiting")
				return stat, nil
			}

			// Other errors will crash the connector and automatically be
			// retried when it restarts.
			return nil, err
		}

		// Check the job status to see if it failed immediately, which has been
		// observed to happen under some circumstances and can cause `job.Wait`
		// to hang.
		if stat, err := job.Status(ctx); err != nil {
			return nil, fmt.Errorf("getting job status: %w", err)
		} else if stat.Err() != nil {
			return nil, fmt.Errorf("job failed: %w", stat.Err())
		}

		// Wait for the job to complete, ensuring that it does so without error.
		stat, err := job.Wait(ctx)
		if err != nil {
			return nil, fmt.Errorf("waiting for job: %w", err)
		} else if stat.Err() != nil {
			return nil, fmt.Errorf("job failed after waiting: %w", stat.Err())
		} else if !stat.Done() {
			return nil, fmt.Errorf("interal application error: job not done after waiting with no error")
		}

		return stat, nil
	}
}

// dmlRowStats extracts the row counts of a completed DML job's statistics.
// The zero RowStats is returned when the status carries none.
func dmlRowStats(stat *bigquery.JobStatus) m.RowStats {
	if stat == nil || stat.Statistics == nil {
		return m.RowStats{}
	}
	qs, ok := stat.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok || qs.DMLStats == nil {
		return m.RowStats{}
	}
	return m.ExactRowStats(qs.DMLStats.InsertedRowCount, qs.DMLStats.UpdatedRowCount, qs.DMLStats.DeletedRowCount).
		WithTotal(qs.NumDMLAffectedRows)
}

// query executes a query against BigQuery and returns the completed job.
// TODO(whb): This is currently only used for executing metadata changes, ex.
// creating tables, adding columns, etc. It would probably be more efficient to
// use the HTTP metadata API for doing these operations instead.
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

// runQuery runs a query and returns its completed job.
func (c client) runQuery(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {
	var job *bigquery.Job
	if err := jobRetry.Do(ctx, "bigquery query", func(ctx context.Context) error {
		var err error
		if job, err = query.Run(ctx); err != nil {
			return fmt.Errorf("run: %w", err)
		}

		// Some queries may immediately fail, such as table alteration rate
		// limits. For these, `job.Wait` will hang forever, so we must check to
		// see if the job has already failed.
		if initialStatus, err := job.Status(ctx); err != nil {
			return fmt.Errorf("getting initialStatus for job: %w", err)
		} else if err := initialStatus.Err(); err != nil {
			return &jobFailedError{err}
		}

		// Wait reports a failed job either as its error or in status.Err().
		status, err := job.Wait(ctx)
		if err == nil {
			err = status.Err()
		}
		if err != nil {
			return &jobFailedError{err}
		} else if !status.Done() {
			return fmt.Errorf("query not done")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return job, nil
}
