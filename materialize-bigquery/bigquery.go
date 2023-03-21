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
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
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

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
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
	log.WithField("projectID", billingProjectID).Info("bigquery client successfully created")

	cloudStorageClient, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating cloud storage client: %w", err)
	}
	log.Info("cloud storage client successfully created")

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
			}).Info("creating bigquery endpoint")

			var metaSpecs, metaCheckpoints = sql.MetaTables(cfg.DatasetPath())

			client, err := cfg.client(ctx)
			if err != nil {
				return nil, fmt.Errorf("creating client: %w", err)
			}

			return &sql.Endpoint{
				Config:                      cfg,
				Dialect:                     bqDialect,
				MetaSpecs:                   &metaSpecs,
				MetaCheckpoints:             &metaCheckpoints,
				Client:                      client,
				CreateTableTemplate:         tplCreateTargetTable,
				AlterColumnNullableTemplate: tplAlterColumnNullable,
				AlterTableAddColumnTemplate: tplAlterTableAddColumn,
				NewResource:                 newTableConfig,
				NewTransactor:               newTransactor,
				CheckPrerequisites:          prereqs,
			}, nil
		},
	}
}

func prereqs(ctx context.Context, raw json.RawMessage) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	var cfg = new(config)
	if err := pf.UnmarshalStrict(raw, cfg); err != nil {
		// An error here is a logic error in the connector code.
		errs.Err(fmt.Errorf("cannot parse endpoint configuration: %w", err))
		return errs
	}

	client, err := cfg.client(ctx)
	if err != nil {
		// The user-provided JSON credentials could not be parsed and no further checks are
		// possible.
		errs.Err(fmt.Errorf("cannot parse JSON credentials"))
		return errs
	}

	var googleErr *googleapi.Error

	if _, err := client.bigqueryClient.DatasetInProject(client.config.ProjectID, client.config.Dataset).Metadata(ctx); err != nil {
		if errors.As(err, &googleErr) {
			// Not found or forbidden means that either the dataset or project is not found or the
			// credentials are not authorized for them. In these cases the returned error message
			// contains the differentiation between "was it the project or dataset" as unstructured
			// text and we can remove some of the error wrapping to make the message returned to the
			// user more clear.
			if googleErr.Code == http.StatusNotFound || googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf(googleErr.Message)
			}
		}
		errs.Err(err)
	}

	// Verification that the client can perform queries in the configured region. There's not a
	// whole lot we can do for interpreting this error as far as I can tell, but we can at least
	// check before moving on further with the upsert process.
	query := client.newQuery("SELECT 1")
	query.Location = cfg.Region
	if _, err := query.Run(ctx); err != nil {
		errs.Err(err)
	}

	// Verify cloud storage abilities.
	data := []byte("test")

	objectDir := path.Join(client.config.Bucket, client.config.BucketPath)
	objectKey := path.Join(objectDir, tmpFileName())
	objectHandle := client.cloudStorageClient.Bucket(client.config.Bucket).Object(objectKey)

	writer := objectHandle.NewWriter(ctx)
	if _, err := writer.Write(data); err != nil {
		// This won't err until the writer is closed in the case of having the wrong bucket or
		// authorization configured, and would most likely be caused by a logic error in the
		// connector code.
		errs.Err(err)
	} else if err := writer.Close(); err != nil {
		// Handling for the two most comman cases: The bucket doesn't exist, or the bucket does
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

	log.WithFields(log.Fields{
		"table":           specs.Identifier,
		"materialization": materialization.String(),
		"version":         data.Version,
	}).Info("existing materialization spec loaded")

	return data.SpecB64, data.Version, nil
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	// BigQuery does not support transactional DDL statements and has a maximum limit of 1024k
	// characters in a single multi-statement query. It is possible for a materialization created
	// with a large number of bindings to exceed this limit. Because of this we process large lists
	// of statements in batches that are comfortably below the character limit.
	return batchStatements(1024*1000/2, statements, func(batch []string) error {
		_, err := c.query(ctx, strings.Join(batch, "\n"))
		return err
	})
}

func batchStatements(charLimit int, statements []string, flush func([]string) error) error {
	for {
		chars := 0
		for idx := 0; idx < len(statements); idx++ {
			chars += len(statements[idx])
			if chars > charLimit || idx == len(statements)-1 {
				if err := flush(statements[:idx+1]); err != nil {
					return err
				}
				statements = statements[idx+1:]
				break
			}
		}

		if len(statements) == 0 {
			break
		}
	}

	return nil
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
