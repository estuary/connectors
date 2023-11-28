package connector

import (
	"context"
	dbSql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
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

	if _, err := sql.ParseDelay(c.Advanced.UpdateDelay); err != nil {
		return err
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

	return &client{
		bigqueryClient:     bigqueryClient,
		cloudStorageClient: cloudStorageClient,
		config:             *c,
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

			client, err := cfg.client(ctx)
			if err != nil {
				return nil, fmt.Errorf("creating client: %w", err)
			}

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             bqDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				CheckPrerequisites:  prereqs,
				Tenant:              tenant,
			}, nil
		},
	}
}

func prereqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	client := ep.Client.(*client)
	errs := &sql.PrereqErr{}

	var googleErr *googleapi.Error

	if meta, err := client.bigqueryClient.DatasetInProject(cfg.ProjectID, cfg.Dataset).Metadata(ctx); err != nil {
		if errors.As(err, &googleErr) {
			// The raw error message returned if the dataset or project can't be found can be pretty
			// vague, but a 404 code always means that one of those two things couldn't be found.
			if googleErr.Code == http.StatusNotFound {
				err = fmt.Errorf("the ProjectID %q or BigQuery Dataset %q could not be found: %s (code %v)", cfg.ProjectID, cfg.Dataset, googleErr.Message, googleErr.Code)
			}
		}
		errs.Err(err)
	} else if meta.Location != cfg.Region {
		errs.Err(fmt.Errorf("dataset %q is actually in region %q, which is different than the configured region %q", cfg.Dataset, meta.Location, cfg.Region))
	}

	// Verify cloud storage abilities.
	data := []byte("test")

	objectDir := path.Join(cfg.Bucket, cfg.BucketPath)
	objectKey := path.Join(objectDir, uuid.NewString())
	objectHandle := client.cloudStorageClient.Bucket(cfg.Bucket).Object(objectKey)

	writer := objectHandle.NewWriter(ctx)
	if _, err := writer.Write(data); err != nil {
		// This won't err until the writer is closed in the case of having the wrong bucket or
		// authorization configured, and would most likely be caused by a logic error in the
		// connector code.
		errs.Err(err)
	} else if err := writer.Close(); err != nil {
		// Handling for the two most common cases: The bucket doesn't exist, or the bucket does
		// exist but the configured credentials aren't authorized to write to it.
		if errors.As(err, &googleErr) {
			if googleErr.Code == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", cfg.Bucket)
			} else if googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to bucket %q", objectDir)
			}
		}
		errs.Err(err)
	} else {
		// Verify that the created object can be read and deleted. The unauthorized case is handled
		// & formatted specifically in these checks since the existence of the bucket has already
		// been verified by creating the temporary test object.
		if reader, err := objectHandle.NewReader(ctx); err != nil {
			errs.Err(err)
		} else if _, err := reader.Read(make([]byte, len(data))); err != nil {
			if errors.As(err, &googleErr) && googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf("not authorized to read from bucket %q", objectDir)
			}
			errs.Err(err)
		} else {
			reader.Close()
		}

		// It's technically possible to be able to delete but not read, so delete is checked even if
		// read failed.
		if err := objectHandle.Delete(ctx); err != nil {
			if errors.As(err, &googleErr) && googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf("not authorized to delete from bucket %q", objectDir)
			}
			errs.Err(err)
		}
	}

	return errs
}

// client implements the sql.Client interface.
type client struct {
	bigqueryClient     *bigquery.Client
	cloudStorageClient *storage.Client
	config             config
}

func (c client) AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string, columnDDL string) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;",
		tableIdentifier,
		columnIdentifier,
		columnDDL,
	)

	if !dryRun {
		if _, err := c.query(ctx, query); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) DropNotNullForColumn(ctx context.Context, dryRun bool, table sql.Table, column sql.Column) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
		table.Identifier,
		column.Identifier,
	)

	if !dryRun {
		if _, err := c.query(ctx, query); err != nil {
			// BigQuery will return an error if the column did not have a NOT NULL constraint and we try
			// to drop it, so match on that error as best we can here.
			var googleErr *googleapi.Error
			if errors.As(err, &googleErr) &&
				googleErr.Code == http.StatusBadRequest &&
				strings.HasSuffix(googleErr.Message, "which does not have a NOT NULL constraint.") {
				log.WithFields(log.Fields{
					"table":  table.Identifier,
					"column": column.Identifier,
					"err":    err.Error(),
				}).Debug("column was already nullable")
			} else {
				return "", err
			}
		}
	}

	return query, nil
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	job, err := c.query(ctx, fmt.Sprintf(
		"SELECT version, spec FROM %s WHERE materialization=%s;",
		specs.Identifier,
		specs.Keys[0].Placeholder,
	), materialization.String())
	if err != nil {
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

	log.WithFields(log.Fields{
		"table":           specs.Identifier,
		"materialization": materialization.String(),
		"version":         data.Version,
	}).Info("existing materialization spec loaded")

	return data.SpecB64, data.Version, nil
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	// We don't explicitly wrap `statements` in a transaction, as not all databases support
	// transactional DDL statements, but we do run them through a single connection. This allows a
	// driver to explicitly run `BEGIN;` and `COMMIT;` statements around a transactional operation.

	// The last statement in the list is always the "spec update" during normal operation. BigQuery
	// will error if the total size of the query is greater than 1 MiB, so we must use a
	// parameterized query here to prevent that from happening.
	// TODO(whb): This current implementation is a hack, and I plan to fix it shortly in a larger
	// revamp of how SQL materialization applies work.

	updateSpecStmt := statements[len(statements)-1]
	if !strings.HasPrefix(updateSpecStmt, "INSERT INTO") && !strings.HasPrefix(updateSpecStmt, "UPDATE") {
		// Run all the statements as one single multi-statement query if the final statement is NOT
		// a "spec update" statement. This only happens for the `TestFencingCases` tests.
		_, err := c.query(ctx, strings.Join(statements, "\n"))
		return err
	}

	// Run all the statements except the final "spec update" statement as a single multi-statement query.
	if _, err := c.query(ctx, strings.Join(statements[:len(statements)-1], "\n")); err != nil {
		return err
	}

	args := ctx.Value(sql.ExecStatementsContextKeyArgs).([]interface{})
	if args == nil {
		return fmt.Errorf("could not find SPEC_ARGS in context")
	}
	// args are in the order of:
	//  - Identifier of the MetaSpecs table
	//  - Literal of the materialization version
	//  - Literal of the base64 encoded spec
	//  - Literal of the materialization name
	// The MetaSpecs table identifier will be included directly in the query string.
	params := []string{}
	for _, a := range args[1:] {
		params = append(params, strings.Trim(a.(string), "'")) // Don't include Literal quotes when using values as parameters.
	}

	var metaSpecParameterizedQuery string
	if strings.HasPrefix(updateSpecStmt, "INSERT INTO") {
		metaSpecParameterizedQuery = fmt.Sprintf(
			"INSERT INTO %s (version, spec, materialization) VALUES (%s, %s, %s);",
			args[0].(string),
			bqDialect.Placeholder(0),
			bqDialect.Placeholder(1),
			bqDialect.Placeholder(2),
		)
	} else {
		metaSpecParameterizedQuery = fmt.Sprintf(
			"UPDATE %[1]s SET version = %[2]s, spec = %[3]s WHERE materialization = %[4]s;",
			args[0].(string),
			bqDialect.Placeholder(0),
			bqDialect.Placeholder(1),
			bqDialect.Placeholder(2),
		)
	}

	_, err := c.query(ctx, metaSpecParameterizedQuery, params[0], params[1], params[2])
	return err
}

func (c client) InstallFence(ctx context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	var query strings.Builder
	if err := tplInstallFence.Execute(&query, fence); err != nil {
		return fence, fmt.Errorf("evaluating fence template: %w", err)
	}

	log.Info("installing fence")
	job, err := c.query(ctx, query.String())
	if err != nil {
		return fence, err
	}
	var bqFence struct {
		Fence      int64  `bigquery:"fence"`
		Checkpoint string `bigquery:"checkpoint"`
	}
	log.Info("reading installed fence")
	if err = c.fetchOne(ctx, job, &bqFence); err != nil {
		return fence, fmt.Errorf("read fence: %w", err)
	}

	checkpoint, err := base64.StdEncoding.DecodeString(bqFence.Checkpoint)
	if err != nil {
		return fence, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	fence.Fence = bqFence.Fence
	fence.Checkpoint = checkpoint

	log.WithFields(log.Fields{
		"fence":            fence.Fence,
		"keyBegin":         fence.KeyBegin,
		"keyEnd":           fence.KeyEnd,
		"materialization":  fence.Materialization.String(),
		"checkpointsTable": fence.TablePath,
	}).Info("fence installed successfully")

	return fence, nil
}
