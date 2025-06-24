package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	"github.com/estuary/connectors/go/dbt"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql-v2"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

var featureFlagDefaults = map[string]bool{
	// When set, object and array field types will be materialized as JSON
	// columns, instead of the historical behavior of strings.
	"objects_and_arrays_as_json": true,
}

type config struct {
	ProjectID        string                     `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset." jsonschema_extras:"order=0"`
	CredentialsJSON  string                     `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=1"`
	Region           string                     `json:"region" jsonschema:"title=Region,description=Region where both the Bucket and the BigQuery dataset is located. They both need to be within the same region." jsonschema_extras:"order=2"`
	Dataset          string                     `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=3"`
	Bucket           string                     `json:"bucket" jsonschema:"title=Bucket,description=Google Cloud Storage bucket that is going to be used to store specfications & temporary data before merging into BigQuery." jsonschema_extras:"order=4"`
	BucketPath       string                     `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects to Google Cloud Storage's bucket." jsonschema_extras:"order=5"`
	BillingProjectID string                     `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID,description=Billing Project ID connected to the BigQuery dataset. Defaults to Project ID if not specified." jsonschema_extras:"order=6"`
	HardDelete       bool                       `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=7"`
	Schedule         boilerplate.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger    dbt.JobConfig              `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	DisableFieldTruncation bool   `json:"disableFieldTruncation,omitempty" jsonschema:"title=Disable Field Truncation,description=Disables truncation of materialized fields. May result in errors for documents with extremely large values or complex nested structures."`
	FeatureFlags           string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"project_id", c.ProjectID},
		{"credentials_json", c.CredentialsJSON},
		{"dataset", c.Dataset},
		{"region", c.Region},
		{"bucket", c.Bucket},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Sanity check: Are the provided credentials valid JSON? A common error is to upload
	// credentials that are not valid JSON, and the resulting error is fairly cryptic if fed
	// directly to bigquery.NewClient.
	if !json.Valid([]byte(c.CredentialsJSON)) {
		return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
	}

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return nil
}

func (c config) client(ctx context.Context, ep *sql.Endpoint[config]) (*client, error) {
	var clientOpts []option.ClientOption

	clientOpts = append(clientOpts,
		option.WithCredentialsJSON([]byte(c.CredentialsJSON)),
		option.WithUserAgent("Estuary Technologies"))

	// Allow overriding the main 'project_id' with 'billing_project_id' for client operation billing.
	var billingProjectID = c.BillingProjectID
	if billingProjectID == "" {
		billingProjectID = c.ProjectID
	}
	bigqueryClient, err := bigquery.NewClient(ctx, billingProjectID, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating bigquery client: %w", err)
	}
	// Use the much faster storage read API for reading query results if the
	// authorization provides sufficient access, typically via the "BigQuery
	// Read Session User" role. Result iterators will automatically fall back to
	// the standard but much slower job/table read API if the storage API can't
	// be accessed.
	if err := bigqueryClient.EnableStorageReadClient(ctx, clientOpts...); err != nil {
		return nil, fmt.Errorf("enabling storage read client: %w", err)
	}

	cloudStorageClient, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating cloud storage client: %w", err)
	}

	return &client{
		bigqueryClient:     bigqueryClient,
		cloudStorageClient: cloudStorageClient,
		cfg:                c,
		ep:                 ep,
	}, nil
}

func (c config) DefaultNamespace() string {
	return c.Dataset
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type tableConfig struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Table in the BigQuery dataset to store materialized result in." jsonschema_extras:"x-collection-name=true"`
	Dataset   string `json:"dataset,omitempty" jsonschema:"title=Alternative Dataset,description=Alternative dataset for this table (optional). Must be located in the region set in the endpoint configuration." jsonschema_extras:"x-schema-name=true"`
	Delta     bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Defaults is false." jsonschema_extras:"x-delta-updates=true"`
	projectID string
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	if c.Dataset == "" {
		c.Dataset = cfg.Dataset
	}
	c.projectID = cfg.ProjectID

	return c
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func (c tableConfig) Parameters() ([]string, bool, error) {
	return []string{c.projectID, c.Dataset, c.Table}, c.Delta, nil
}

func Driver() *sql.Driver[config, tableConfig] {
	return newBigQueryDriver()
}

func newBigQueryDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-bigquery",
		StartTunnel:      func(ctx context.Context, cfg config) error { return nil },
		NewEndpoint: func(ctx context.Context, cfg config, tenant string, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"project_id":  cfg.ProjectID,
				"dataset":     cfg.Dataset,
				"region":      cfg.Region,
				"bucket":      cfg.Bucket,
				"bucket_path": cfg.BucketPath,
			}).Info("creating bigquery endpoint")

			objAndArrayAsJson := featureFlags["objects_and_arrays_as_json"]
			dialect := bqDialect(objAndArrayAsJson)
			templates := renderTemplates(dialect)

			// BigQuery's default SerPolicy has historically had limits of 1500
			// for truncation, rather than the more common 1000.
			serPolicy := &pf.SerPolicy{
				StrTruncateAfter:       1 << 16, // 64 KiB
				NestedObjTruncateAfter: 1500,
				ArrayTruncateAfter:     1500,
			}
			if cfg.Advanced.DisableFieldTruncation {
				serPolicy = boilerplate.SerPolicyDisabled
			}

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				SerPolicy:           serPolicy,
				MetaCheckpoints:     sql.FlowCheckpointsTable([]string{cfg.ProjectID, cfg.Dataset}),
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       prepareNewTransactor(dialect, templates, objAndArrayAsJson),
				Tenant:              tenant,
				ConcurrentApply:     true,
				Options: boilerplate.MaterializeOptions{
					ExtendedLogging: true,
					AckSchedule: &boilerplate.AckScheduleOption{
						Config: cfg.Schedule,
						Jitter: []byte(cfg.ProjectID + cfg.Dataset),
					},
					DBTJobTrigger: &cfg.DBTJobTrigger,
				},
			}, nil
		},
		PreReqs: preReqs,
	}
}
