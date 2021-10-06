package main

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
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

// runQuery will run a query and return the compelted job.
func (ep *Endpoint) runQuery(ctx context.Context, query *bigquery.Query) (*bigquery.Job, error) {

	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			return nil, errNotFound // The table wasn't found
		}
		return job, fmt.Errorf("wait: %w", err)
	}
	if err := status.Err(); err != nil {
		return job, fmt.Errorf("status: %w", err)
	}
	if !status.Done() {
		return job, fmt.Errorf("query not done")
	}

	return job, nil

}

// fetchOne will fetch one row of data from a job and scan it into dest.
func (ep *Endpoint) fetchOne(ctx context.Context, job *bigquery.Job, dest interface{}) error {

	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("read: %w", err)
	}

	if err = it.Next(dest); err != nil {
		return fmt.Errorf("next: %w", err)
	}

	return nil

}
