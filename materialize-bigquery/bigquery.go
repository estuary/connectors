package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	m "github.com/estuary/connectors/go/protocols/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// Config represents the endpoint configuration for BigQuery.
type config struct {
	ProjectID        string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset." jsonschema_extras:"order=0"`
	CredentialsJSON  string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=1"`
	Region           string `json:"region" jsonschema:"title=Region,description=Region where both the Bucket and the BigQuery dataset is located. They both need to be within the same region." jsonschema_extras:"order=2"`
	Dataset          string `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=3"`
	Bucket           string `json:"bucket" jsonschema:"title=Bucket,description=Google Cloud Storage bucket that is going to be used to store specfications & temporary data before merging into BigQuery." jsonschema_extras:"order=4"`
	BucketPath       string `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects to Google Cloud Storage's bucket." jsonschema_extras:"order=5"`
	BillingProjectID string `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID,description=Billing Project ID connected to the BigQuery dataset. Defaults to Project ID if not specified." jsonschema_extras:"order=6"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce compute time by increasing the delay between updates. Defaults to 30 minutes if unset.,enum=0s,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`

	HardDelete bool `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false"`
}

func (c *config) Validate() error {
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
	if !json.Valid(decodeCredentials(c.CredentialsJSON)) {
		return fmt.Errorf("service account credentials must be valid JSON, and the provided credentials were not")
	}

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
	}

	if _, err := m.ParseDelay(c.Advanced.UpdateDelay); err != nil {
		return err
	}

	return nil
}

func (c *config) client(ctx context.Context) (*client, error) {
	var clientOpts []option.ClientOption

	clientOpts = append(clientOpts,
		option.WithCredentialsJSON(decodeCredentials(c.CredentialsJSON)),
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

	cloudStorageClient, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating cloud storage client: %w", err)
	}

	return &client{
		bigqueryClient:     bigqueryClient,
		cloudStorageClient: cloudStorageClient,
		cfg:                *c,
	}, nil
}

type tableConfig struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Table in the BigQuery dataset to store materialized result in." jsonschema_extras:"x-collection-name=true"`
	Dataset   string `json:"dataset,omitempty" jsonschema:"title=Alternative Dataset,description=Alternative dataset for this table (optional). Must be located in the region set in the endpoint configuration."`
	Delta     bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Defaults is false."`
	projectID string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to the explicit endpoint configuration dataset. This may be over-written by a
		// present `dataset` property within `raw` for the resource.
		Dataset:   ep.Config.(*config).Dataset,
		projectID: ep.Config.(*config).ProjectID,
	}
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

// Path returns the sqlDriver.ResourcePath for a table.
func (c tableConfig) Path() sql.TablePath {
	return []string{c.projectID, c.Dataset, c.Table}
}

// DeltaUpdates returns if BigQuery is in DeltaUpdates mode or not.
func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

// decodeCredentials allows support for legacy credentials that were base64 encoded. Previously, the
// connector required base64 encoding of JSON service account credentials. In the future, this
// fallback can be removed when base64 encoded credentials are no longer supported and only
// unencoded JSON is acceptable.
func decodeCredentials(credentialString string) []byte {
	decoded, err := base64.StdEncoding.DecodeString(credentialString)
	if err == nil {
		// If the provided credentials string was a valid base64 encoding, assume that it was base64
		// encoded JSON and return the result of successfully decoding that.
		return decoded
	}

	// Otherwise, assume that the credentials string was not base64 encoded.
	return []byte(credentialString)
}

func Driver() *sql.Driver {
	return newBigQueryDriver()
}

func newBigQueryDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-bigquery",
		EndpointSpecType: config{},
		ResourceSpecType: tableConfig{},
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"project_id":  cfg.ProjectID,
				"dataset":     cfg.Dataset,
				"region":      cfg.Region,
				"bucket":      cfg.Bucket,
				"bucket_path": cfg.BucketPath,
			}).Info("creating bigquery endpoint")

			var metaBase sql.TablePath = []string{cfg.ProjectID, cfg.Dataset}
			var metaSpecs, metaCheckpoints = sql.MetaTables(metaBase)

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             bqDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				NewClient:           newClient,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
				ConcurrentApply:     true,
			}, nil
		},
	}
}
