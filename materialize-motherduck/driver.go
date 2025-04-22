package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/klauspost/compress/gzip"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type config struct {
	Token              string `json:"token" jsonschema:"title=Motherduck Service Token,description=Service token for authenticating with MotherDuck." jsonschema_extras:"secret=true,order=0"`
	Database           string `json:"database" jsonschema:"title=Database,description=The database to materialize to." jsonschema_extras:"order=1"`
	Schema             string `json:"schema" jsonschema:"title=Database Schema,default=main,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads. Must not contain dots (.)" jsonschema_extras:"order=3,pattern=^[^.]*$"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=4"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=5"`
	Region             string `json:"region" jsonschema:"title=S3 Bucket Region,description=Region of the S3 staging bucket." jsonschema_extras:"order=6"`
	BucketPath         string `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in S3." jsonschema_extras:"order=7"`
	HardDelete         bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=8"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"token", c.Token},
		{"database", c.Database},
		{"schema", c.Schema},
		{"bucket", c.Bucket},
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Sanity check that the provided authentication token is a well-formed JWT. It it's not, the
	// sql.Open function used elsewhere will return an error string that is difficult to comprehend.
	// This check isn't perfect but it should catch most of the blatantly obvious error cases.
	for _, part := range strings.Split(c.Token, ".") {
		if _, err := base64.RawURLEncoding.DecodeString(part); err != nil {
			return fmt.Errorf("invalid token: must be a base64 encoded JWT")
		}
	}

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
	}

	// The bucket name must not contain any dots, since this breaks server
	// certificate validation on MotherDuck's side.
	if strings.Contains(c.Bucket, ".") {
		return fmt.Errorf("bucket name must not contain '.'")
	}

	return nil
}

func (c *config) db(ctx context.Context) (*stdsql.DB, error) {
	var userAgent = "Estuary"

	db, err := stdsql.Open("duckdb", fmt.Sprintf("md:%s?motherduck_token=%s&custom_user_agent=%s", c.Database, c.Token, userAgent))
	if err != nil {
		if strings.Contains(err.Error(), "Jwt header is an invalid JSON") {
			return nil, fmt.Errorf("invalid token: unauthenticated")
		}
		return nil, err
	}

	for idx, c := range []string{
		"SET autoinstall_known_extensions=1;",
		"SET autoload_known_extensions=1;",
		fmt.Sprintf("SET s3_access_key_id='%s'", c.AWSAccessKeyID),
		fmt.Sprintf("SET s3_secret_access_key='%s';", c.AWSSecretAccessKey),
		fmt.Sprintf("SET s3_region='%s';", c.Region),
	} {
		if _, err := db.ExecContext(ctx, c); err != nil {
			return nil, fmt.Errorf("executing setup command %d: %w", idx, err)
		}
	}

	return db, err
}

func (c *config) toS3Client(ctx context.Context) (*s3.Client, error) {
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Region),
	)
	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg), nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)." jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Update,description=Should updates to this table be done via delta updates." jsonschema_extras:"x-delta-updates=true"`

	database string
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to the endpoint schema. This will be over-written by a present `schema` property
		// within `raw`.
		Schema:   ep.Config.(*config).Schema,
		database: ep.Config.(*config).Database,
	}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}

	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.database, c.Schema, c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newDuckDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-motherduck",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		StartTunnel:      func(ctx context.Context, conf any) error { return nil },
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("could not parse endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": cfg.Database,
			}).Info("opening database")

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             duckDialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable([]string{cfg.Database, cfg.Schema}),
				NewClient:           newClient,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
				ConcurrentApply:     false,
			}, nil
		},
		PreReqs: preReqs,
	}
}

type transactor struct {
	cfg *config

	fence    sql.Fence
	conn     *stdsql.Conn
	s3client *s3.Client

	bindings []*binding
	be       *boilerplate.BindingEvents
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	is *boilerplate.InfoSchema,
	be *boilerplate.BindingEvents,
) (_ m.Transactor, _ *boilerplate.MaterializeOptions, err error) {
	cfg := ep.Config.(*config)

	s3client, err := cfg.toS3Client(ctx)
	if err != nil {
		return nil, nil, err
	}

	db, err := cfg.db(ctx)
	if err != nil {
		return nil, nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("creating connection: %w", err)
	}

	t := &transactor{
		cfg:      cfg,
		conn:     conn,
		s3client: s3client,
		fence:    fence,
		be:       be,
	}

	for _, b := range bindings {
		t.bindings = append(t.bindings, &binding{
			target:    b,
			storeFile: newStagedFile(s3client, cfg.Bucket, cfg.BucketPath, b.ColumnNames()),
			loadFile:  newStagedFile(s3client, cfg.Bucket, cfg.BucketPath, b.KeyNames()),
		})
	}

	return t, nil, nil
}

type binding struct {
	target    sql.Table
	storeFile *stagedFile
	loadFile  *stagedFile
	mustMerge bool
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		b := d.bindings[it.Binding]
		b.loadFile.start()

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = b.loadFile.encodeRow(ctx, converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	var subqueries []string
	for _, b := range d.bindings {
		if !b.loadFile.started {
			// Pass.
		} else if delete, err := b.loadFile.flush(); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			// Clean up staged files by calling the `delete` function returned
			// by each binding that had keys to load.
			defer delete(ctx)

			var loadQuery strings.Builder
			if err := tplLoadQuery.Execute(&loadQuery, &queryParams{
				Table: b.target,
				Files: b.loadFile.allFiles(),
			}); err != nil {
				return fmt.Errorf("rendering load query: %w", err)
			}

			subqueries = append(subqueries, loadQuery.String())
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}
	loadAllSql := strings.Join(subqueries, "\nUNION ALL\n")

	// The results of the load query will be written to an S3 object, which
	// we'll then read directly from S3. This is done instead of running the
	// query in-process to avoid blowing out the memory usage of the connector,
	// since iterating over the returned rows directly results in the entire
	// query being materialized in-memory.
	loadResKey := path.Join(d.cfg.BucketPath, "loaded_"+uuid.NewString()+".json.gz")

	// TODO(whb): In the future it may be useful to persist the load query
	// results in the driver checkpoint via the `Flushed` response and use them
	// for re-application of transactions. This will require idempotent runtime
	// transactions though.
	defer d.s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(loadResKey),
	})

	d.be.StartedEvaluatingLoads()
	rows, err := d.conn.QueryContext(ctx, fmt.Sprintf("COPY (%s) to '%s';", loadAllSql, fmt.Sprintf("s3://%s/%s", d.cfg.Bucket, loadResKey)))
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()
	d.be.FinishedEvaluatingLoads()

	var loadQueryCount int
	for rows.Next() {
		if err := rows.Scan(&loadQueryCount); err != nil {
			return fmt.Errorf("scanning loadQueryCount: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	// Now read these results back from the s3 file.
	r, err := d.s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(loadResKey),
	})
	if err != nil {
		return fmt.Errorf("get load results object reader: %w", err)
	}
	defer r.Body.Close()

	gzr, err := gzip.NewReader(r.Body)
	if err != nil {
		return fmt.Errorf("get gzip reader for load results: %w", err)
	}
	defer gzr.Close()

	type bindingDoc struct {
		Binding int
		Doc     json.RawMessage
	}

	dec := json.NewDecoder(gzr)
	loadedCount := 0
	for {
		var doc bindingDoc
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("scanning loaded document from file: %w", err)
		}
		if err = loaded(doc.Binding, doc.Doc); err != nil {
			return fmt.Errorf("sending loaded document: %w", err)
		}
		loadedCount += 1
	}

	// These counts should always be equal.
	if loadedCount != loadQueryCount {
		return fmt.Errorf("mismatched loadedCount vs loadQueryCount: %d vs %d", loadedCount, loadQueryCount)
	}

	return nil
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	for it.Next() {
		if d.cfg.HardDelete && it.Delete && !it.Exists {
			// Ignore documents which do not exist and are being deleted.
			continue
		}

		b := d.bindings[it.Binding]
		b.storeFile.start()

		if it.Exists {
			b.mustMerge = true
		}

		flowDocument := it.RawJSON
		if d.cfg.HardDelete && it.Delete {
			flowDocument = json.RawMessage(`"delete"`)
		}

		if converted, err := b.target.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := b.storeFile.encodeRow(ctx, converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var err error
		if d.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.fence); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("evaluating fence template: %w", err))
		}

		return nil, m.RunAsyncOperation(func() error {
			txn, err := d.conn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("store BeginTx: %w", err)
			}
			defer txn.Rollback()

			for idx, b := range d.bindings {
				if !b.storeFile.started {
					// No stores for this binding.
					continue
				}

				delete, err := b.storeFile.flush()
				if err != nil {
					return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
				}
				defer delete(ctx)

				params := &queryParams{Table: b.target, Files: b.storeFile.allFiles()}

				d.be.StartedResourceCommit(b.target.Path)
				if b.mustMerge {
					// In-place updates are accomplished by deleting the
					// existing row and inserting the updated row.
					var storeDeleteQuery strings.Builder
					if err := tplStoreDeleteQuery.Execute(&storeDeleteQuery, params); err != nil {
						return err
					} else if _, err := txn.ExecContext(ctx, storeDeleteQuery.String()); err != nil {
						return fmt.Errorf("executing store delete query for binding[%d]: %w", idx, err)
					}

				}

				var storeQuery strings.Builder
				if err := tplStoreQuery.Execute(&storeQuery, params); err != nil {
					return err
				} else if _, err := txn.ExecContext(ctx, storeQuery.String()); err != nil {
					return fmt.Errorf("executing store query for binding[%d]: %w", idx, err)
				}
				d.be.FinishedResourceCommit(b.target.Path)

				// Reset for next round.
				b.mustMerge = false
			}

			if res, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating checkpoints: %w", err)
			} else if rows, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("getting fence update rows affected: %w", err)
			} else if rows != 1 {
				return fmt.Errorf("this instance was fenced off by another")
			} else if err := txn.Commit(); err != nil {
				return fmt.Errorf("committing store transaction: %w", err)
			}

			return nil
		})
	}, nil
}

func (d *transactor) Destroy() {}
