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
	BillingProjectID string `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID"`
	ProjectID        string `json:"project_id" jsonschema:"title=Project ID"`
	Dataset          string `json:"dataset" jsonschema:"title=BigQuery Dataset"`
	Region           string `json:"region,omitempty" jsonschema:"title=Region,example="us-central1"`
	Bucket           string `json:"bucket" jsonschema:"title: Bucket"`
	BucketPath       string `json:"bucket_path" jsonschema:"title=Bucket Prefix,example=my/new/materialization"`
	CredentialsJSON  []byte `json:"credentials_json,omitempty" jsonschema:"title=Credentials`

	ClientOpts []option.ClientOption `json:"-"`
}

func NewConfig(data json.RawMessage) (*config, error) {
	var cfg config

	if err := pf.UnmarshalStrict(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing BigQuery configuration: %w", err)
	}

	if len(cfg.BucketPath) > 0 && (strings.HasPrefix(cfg.BucketPath, "/") || strings.HasSuffix(cfg.BucketPath, "/")) {
		return nil, fmt.Errorf("bucket_path cannot start or end with a slash (/), you can use a multi-level path using slash, ie. 'multi/level/bucket/path")
	}

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

func (*config) GetFieldDocString(fieldname string) string {
	switch fieldname {
	case "BillingProjectID":
		return "Billing Project ID connected to the BigQuery dataset. It can be the same value as Project ID."
	case "ProjectID":
		return "Google Cloud Project ID that owns the BigQuery dataset."
	case "Dataset":
		return "BigQuery dataset that will be used to store the materialization output."
	case "Region":
		return "Region where both the Bucket and the BigQuery dataset is located. They both need to be within the same region."
	case "Bucket":
		return "Google Cloud Storage bucket that is going to be used to store specfications & temporary data before merging into BigQuery."
	case "BucketPath":
		return "A prefix that will be used to store objects to Google Cloud Storage's bucket."
	case "CredentialsJSON":
		return "Google Cloud Service Account JSON credentials in base64 format."
	default:
		return ""
	}
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

func (*bindingResource) GetFieldDocString(fieldname string) string {
	switch fieldname {
	case "Table":
		return "Table in the BigQuery dataset to store materialized resutl in."
	case "Delta":
		return "Should updates to this table be done via delta updates. Defaults is false."
	default:
		return ""
	}
}
