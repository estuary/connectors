package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

type config struct {
	BillingProjectID string `json:"billing_project_id,omitempty"`
	ProjectID        string `json:"project_id"`
	Dataset          string `json:"dataset"`
	Region           string `json:"region,omitempty"`
	Bucket           string `json:"bucket"`
	BucketPath       string `json:"bucket_path"`
	CredentialsJSON  []byte `json:"credentials_json,omitempty"`

	ClientOpts []option.ClientOption
}

func NewConfig(data json.RawMessage) (*config, error) {
	var cfg config

	if err := pf.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing BigQuery configuration: %w", err)
	}

	// Bucket path can be a problematic value if it starts or ends
	// with a slash(/) because the connector will format a finalized path
	// with this as part of it. Because double slash are problematic in some cases
	// with Cloudstorage, it's better for the connector to trim the slashes
	// if they are provided so path can be constructed the same way.
	cfg.BucketPath = strings.Trim(cfg.BucketPath, "/")

	log.WithFields(log.Fields{
		"project_id":  cfg.ProjectID,
		"dataset":     cfg.Dataset,
		"region":      cfg.Region,
		"bucket":      cfg.Bucket,
		"bucket_path": cfg.BucketPath,
	}).Info("opening bigquery")

	if len(cfg.CredentialsJSON) != 0 {
		cfg.ClientOpts = append(cfg.ClientOpts, option.WithCredentialsJSON(cfg.CredentialsJSON))
	} else {
		return nil, errors.New("JSON credentials required")
	}

	if cfg.BillingProjectID == "" {
		cfg.BillingProjectID = cfg.ProjectID
	}

	return &cfg, nil
}

func (c *config) BigQueryClient(ctx context.Context) (*bigquery.Client, error) {
	bigQueryClient, err := bigquery.NewClient(ctx, c.BillingProjectID, c.ClientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}

	return bigQueryClient, nil
}

func (c *config) StorageClient(ctx context.Context) (*storage.Client, error) {
	cloudStorageClient, err := storage.NewClient(ctx, c.ClientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating cloud storage client: %w", err)
	}

	return cloudStorageClient, nil
}

func (c *config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("expected project_id")
	}
	if c.Dataset == "" {
		return fmt.Errorf("expected dataset")
	}
	if c.Region == "" {
		return fmt.Errorf("expected region")
	}
	if c.Bucket == "" {
		return fmt.Errorf("expected bucket")
	}
	return nil
}

type bindingResource struct {
	Table string `json:"table"`
	Delta bool   `json:"delta_updates,omitempty"`
}

func (br *bindingResource) Validate() error {
	if br.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}
