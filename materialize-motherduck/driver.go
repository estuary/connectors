package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	m "github.com/estuary/connectors/go/protocols/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	_ "github.com/marcboeker/go-duckdb"
)

type config struct {
	Token              string `json:"token" jsonschema:"title=Motherduck Service Token,description=Service token for authenticating with MotherDuck." jsonschema_extras:"secret=true,order=0"`
	Database           string `json:"database" jsonschema:"title=Database,description=The database to materialize to." jsonschema_extras:"order=1"`
	Schema             string `json:"schema" jsonschema:"title=Database Schema,default=main,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads." jsonschema_extras:"order=3"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=4"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=5"`
	Region             string `json:"region" jsonschema:"title=S3 Bucket Region,description=Region of the S3 staging bucket." jsonschema_extras:"order=6"`
	BucketPath         string `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=An optional prefix that will be used to store objects in S3." jsonschema_extras:"order=7"`
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

	return nil
}

func (c *config) db(ctx context.Context) (*stdsql.DB, error) {
	var userAgent = "Estuary"

	db, err := stdsql.Open("duckdb", fmt.Sprintf("md:%s?motherduck_token=%s&custom_user_agent=%s", c.Database, c.Token, userAgent))
	if err != nil {
		if strings.Contains(err.Error(), "Jwt header is an invalid JSON (UNAUTHENTICATED") {
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
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)."`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=true,title=Delta Update,description=Should updates to this table be done via delta updates. Currently this connector only supports delta updates."`

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

	if !r.Delta {
		return fmt.Errorf("connector only supports delta update mode: delta update must be enabled")
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
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("could not parse endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": cfg.Database,
			}).Info("opening database")

			metaSpecs, metaCheckpoints := sql.MetaTables([]string{cfg.Database, cfg.Schema})

			return &sql.Endpoint{
				Config:               cfg,
				Dialect:              duckDialect,
				MetaSpecs:            &metaSpecs,
				MetaCheckpoints:      &metaCheckpoints,
				NewClient:            newClient,
				CreateTableTemplate:  tplCreateTargetTable,
				ReplaceTableTemplate: tplReplaceTargetTable,
				NewResource:          newTableConfig,
				NewTransactor:        newTransactor,
				Tenant:               tenant,
				ConcurrentApply:      false,
				MaxFieldCharLen:      0, // Duckdb has no apparent limit on how long column names can be
			}, nil
		},
	}
}

type transactor struct {
	cfg *config

	fence     sql.Fence
	storeConn *stdsql.Conn

	bindings []*binding
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
) (_ m.Transactor, err error) {
	cfg := ep.Config.(*config)

	s3client, err := cfg.toS3Client(ctx)
	if err != nil {
		return nil, err
	}

	db, err := cfg.db(ctx)
	if err != nil {
		return nil, err
	}

	storeConn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating store connection: %w", err)
	}

	t := &transactor{
		cfg:       cfg,
		storeConn: storeConn,
		fence:     fence,
	}

	for _, b := range bindings {
		t.bindings = append(t.bindings, &binding{
			target:    b,
			storeFile: newStagedFile(s3client, cfg.Bucket, cfg.BucketPath, b.ColumnNames()),
		})
	}

	return t, nil
}

type binding struct {
	target    sql.Table
	storeFile *stagedFile
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("connector must be set to delta updates")
	}
	return nil
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	for it.Next() {
		b := d.bindings[it.Binding]
		b.storeFile.start()

		if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
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
			// NB: Motherduck doesn't actually support transactions yet, but this code is written
			// like it does. Eventually Motherduck will probably support transactions, and the
			// connector pretending like it already does doesn't hurt anything and may make the
			// eventual transition easier.
			txn, err := d.storeConn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("store BeginTx: %w", err)
			}
			defer txn.Rollback()

			for idx, b := range d.bindings {
				if !b.storeFile.started {
					// No stores for this binding.
					continue
				}

				delete, err := b.storeFile.flush(ctx)
				if err != nil {
					return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
				}
				defer delete(ctx)

				var storeQuery strings.Builder
				if err := tplStoreQuery.Execute(&storeQuery, &storeParams{
					Table: b.target,
					Files: b.storeFile.allFiles(),
				}); err != nil {
					return err
				}

				if _, err := txn.ExecContext(ctx, storeQuery.String()); err != nil {
					return fmt.Errorf("executing store query for binding[%d]: %w", idx, err)
				}
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
