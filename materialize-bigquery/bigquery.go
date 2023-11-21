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
	"slices"
	"strings"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
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
				Tenant:              tenant,
			}, nil
		},
	}
}

type client struct {
	bigqueryClient     *bigquery.Client
	cloudStorageClient *storage.Client
	config             config
}

type columnRow struct {
	TableName  string `bigquery:"table_name"`
	ColumnName string `bigquery:"column_name"`
	IsNullable string `bigquery:"is_nullable"` // string YES or NO
	DataType   string `bigquery:"data_Type"`
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, actions sql.ApplyActions, updateSpecStatement string, dryRun bool) (string, error) {
	client := ep.Client.(*client)
	cfg := ep.Config.(*config)

	// Query the information schema for all the datasets we care about to build up a list of
	// existing tables and columns. The datasets we care about are the dataset configured for the
	// overall endpoint, as well as any distinct datasets configured for any of the bindings.
	datasets := []string{cfg.Dataset}
	for _, t := range actions.CreateTables {
		ds := t.Path[1]
		if !slices.Contains(datasets, ds) {
			datasets = append(datasets, ds)
		}
	}
	for _, t := range actions.AlterTables {
		ds := t.Path[1]
		if !slices.Contains(datasets, ds) {
			datasets = append(datasets, ds)
		}
	}
	for idx := range datasets {
		datasets[idx] = bqDialect.Identifier(datasets[idx])
	}

	existing := &sql.ExistingColumns{}
	for _, ds := range datasets {
		job, err := client.query(ctx, fmt.Sprintf(
			"select table_name, column_name, is_nullable, data_type from %s.INFORMATION_SCHEMA.COLUMNS;",
			bqDialect.Identifier(ds),
		))
		if err != nil {
			return "", fmt.Errorf("querying INFORMATION_SCHEMA.COLUMNS for dataset %q: %w", ds, err)
		}

		it, err := job.Read(ctx)
		if err != nil {
			return "", fmt.Errorf("reading job: %w", err)
		}

		for {
			var c columnRow
			if err = it.Next(&c); err == iterator.Done {
				break
			} else if err != nil {
				return "", fmt.Errorf("columnRow read: %w", err)
			}

			existing.PushColumn(
				ds,
				c.TableName,
				c.ColumnName,
				strings.EqualFold(c.IsNullable, "yes"),
				c.DataType,
				0, // BigQuery does not have a character_maximum_length in its INFORMATION_SCHEMA.COLUMNS view.
			)
		}
	}

	filtered, err := sql.FilterActions(actions, bqDialect, existing)
	if err != nil {
		return "", err
	}

	statements := []string{}
	for _, tc := range filtered.CreateTables {
		statements = append(statements, tc.TableCreateSql)
	}

	for _, ta := range filtered.AlterTables {
		var alterColumnStmt strings.Builder
		if err := tplAlterTableColumns.Execute(&alterColumnStmt, ta); err != nil {
			return "", fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		statements = append(statements, alterColumnStmt.String())
	}

	// The spec will get updated last, after all the other actions are complete, but include it in
	// the description of actions.
	action := strings.Join(append(statements, updateSpecStatement), "\n")
	if dryRun {
		return action, nil
	}

	// Execute statements in parallel for efficiency. Each statement acts on a single table, and
	// everything that needs to be done for a given table is contained in that statement.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, s := range statements {
		s := s
		group.Go(func() error {
			if _, err := c.query(groupCtx, s); err != nil {
				return fmt.Errorf("executing apply statement: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := c.query(ctx, updateSpecStatement); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return action, nil
}

func (c client) PreReqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
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
	_, err := c.query(ctx, strings.Join(statements, "\n"))
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
