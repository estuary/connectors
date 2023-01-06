package connector

import (
	"context"
	dbSql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

// Config represents the endpoint configuration for BigQuery.
type config struct {
	ProjectID        string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID that owns the BigQuery dataset." jsonschema_extras:"order=0"`
	CredentialsJSON  string `json:"credentials_json" jsonschema:"title=Service Account JSON,description=The JSON credentials of the service account to use for authorization." jsonschema_extras:"secret=true,multiline=true,order=1"`
	Region           string `json:"region" jsonschema:"title=Region,description=Region where both the Bucket and the BigQuery dataset is located. They both need to be within the same region." jsonschema_extras:"order=2"`
	Dataset          string `json:"dataset" jsonschema:"title=Dataset,description=BigQuery dataset that will be used to store the materialization output." jsonschema_extras:"order=3"`
	Bucket           string `json:"bucket" jsonschema:"title=Bucket,description=Google Cloud Storage bucket that is going to be used to store specfications & temporary data before merging into BigQuery." jsonschema_extras:"order=4"`
	BucketPath       string `json:"bucket_path,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects to Google Cloud Storage's bucket." jsonschema_extras:"order=5"`
	BillingProjectID string `json:"billing_project_id,omitempty" jsonschema:"title=Billing Project ID,description=Billing Project ID connected to the BigQuery dataset. Defaults to Project ID if not specified." jsonschema_extras:"order=6"`
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

func (c *config) client(ctx context.Context) (*client, error) {
	var clientOpts []option.ClientOption

	clientOpts = append(clientOpts, option.WithCredentialsJSON(decodeCredentials(c.CredentialsJSON)))

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

	if _, err := bigqueryClient.DatasetInProject(c.ProjectID, c.Dataset).Metadata(ctx); err != nil {
		return nil, fmt.Errorf("validating dataset & project: %w", err)
	}

	if _, err := cloudStorageClient.Bucket(c.Bucket).Attrs(ctx); err != nil {
		return nil, fmt.Errorf("validating connection to bucket %q: %w", c.Bucket, err)
	}

	return &client{
		bigqueryClient:     bigqueryClient,
		cloudStorageClient: cloudStorageClient,
		config:             *c,
	}, nil
}

// DatasetPath returns the sqlDriver.ResourcePath including the dataset.
func (c *config) DatasetPath(path ...string) sql.TablePath {
	return append([]string{c.ProjectID, c.Dataset}, path...)
}

type tableConfig struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Table in the BigQuery dataset to store materialized result in." jsonschema_extras:"x-collection-name=true"`
	Delta     bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Defaults is false."`
	projectID string
	dataset   string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		projectID: ep.Config.(config).ProjectID,
		dataset:   ep.Config.(config).Dataset,
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
	return []string{c.projectID, c.dataset, c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return ""
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

func Driver() pm.DriverServer {
	return newBigQueryDriver()
}

func newBigQueryDriver() pm.DriverServer {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-bigquery",
		EndpointSpecType: config{},
		ResourceSpecType: tableConfig{},
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (*sql.Endpoint, error) {
			cfg := config{}
			if err := pf.UnmarshalStrict(raw, &cfg); err != nil {
				return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"project_id":  cfg.ProjectID,
				"dataset":     cfg.Dataset,
				"region":      cfg.Region,
				"bucket":      cfg.Bucket,
				"bucket_path": cfg.BucketPath,
			}).Info("opening bigquery")

			var metaSpecs, metaCheckpoints = sql.MetaTables(cfg.DatasetPath())

			client, err := cfg.client(ctx)
			if err != nil {
				return nil, fmt.Errorf("creating client: %w", err)
			}

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             bqDialect,
				MetaSpecs:           metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
			}, nil
		},
	}
}

// client implements the sql.Client interface.
type client struct {
	bigqueryClient     *bigquery.Client
	cloudStorageClient *storage.Client
	config             config
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	job, err := c.query(ctx, fmt.Sprintf(
		"SELECT version, spec FROM %s WHERE materialization=%s;",
		specs.Identifier,
		specs.Keys[0].Placeholder,
	), materialization.String())

	if err == errNotFound {
		return "", "", dbSql.ErrNoRows
	} else if err != nil {
		return "", "", err
	}

	var data struct {
		Version string `bigquery:"version"`
		SpecB64 string `bigquery:"spec"`
	}

	if err := c.fetchOne(ctx, job, &data); err == errNotFound {
		return "", "", dbSql.ErrNoRows
	} else if err != nil {
		return "", "", err
	}

	return data.SpecB64, data.Version, nil
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	// We don't explicitly wrap `statements` in a transaction, as not all databases support
	// transactional DDL statements, but we do run them through a single connection. This allows a
	// driver to explicitly run `BEGIN;` and `COMMIT;` statements around a transactional operation.
	_, err := c.query(ctx, strings.Join(statements, "\n"))
	return err
}

func (c client) InstallFence(ctx context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	var query strings.Builder
	if err := tplInstallFence.Execute(&query, fence); err != nil {
		return fence, fmt.Errorf("evaluating fence template: %w", err)
	}

	job, err := c.query(ctx, query.String())
	if err != nil {
		return fence, err
	}
	var bqFence struct {
		Fence      int64  `bigquery:"fence"`
		Checkpoint string `bigquery:"checkpoint"`
	}
	if err = c.fetchOne(ctx, job, &bqFence); err != nil {
		return fence, fmt.Errorf("read fence: %w", err)
	}

	checkpoint, err := base64.StdEncoding.DecodeString(bqFence.Checkpoint)
	if err != nil {
		return fence, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	fence.Fence = bqFence.Fence
	fence.Checkpoint = checkpoint

	return fence, nil
}
