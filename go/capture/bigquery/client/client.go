package client

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// Connect builds a BigQuery client for the given project, applying the EstuaryFlow
// user agent and verifying connectivity with a no-op query before returning.
func Connect(ctx context.Context, projectID string, credOption option.ClientOption) (*bigquery.Client, error) {
	log.WithFields(log.Fields{
		"project_id": projectID,
	}).Info("connecting to database")

	var clientOpts = []option.ClientOption{
		credOption,
		option.WithUserAgent("EstuaryFlow (GPN:Estuary;)"),
	}
	bq, err := bigquery.NewClient(ctx, projectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}

	if err := executeQuery(ctx, bq, "SELECT true;"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return bq, nil
}

func executeQuery(ctx context.Context, client *bigquery.Client, query string) error {
	if job, err := client.Query(query).Run(ctx); err != nil {
		return err
	} else if js, err := job.Wait(ctx); err != nil {
		return err
	} else if js.Err() != nil {
		return js.Err()
	}
	return nil
}
