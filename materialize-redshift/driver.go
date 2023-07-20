package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

const (
	// This is the maximum length that any redshift VARCHAR column can be. There is no variable
	// unlimited length text column type (equivalent to Postgres TEXT type, for example), and there
	// are negative query performance implications from creating string columns with this maximum
	// length by default.
	//
	// Because of this we create string columns as their default maximum length of 256 characters
	// initially (somewhat confusingly, using the Redshift TEXT type, which is actually a
	// VARCHAR(256)), and only enlarge the columns to the maximum length if we observe a string that
	// is longer than 256 bytes.
	//
	// Strings longer than 65535 bytes will be truncated automatically by Redshift: In the
	// materialized columns if they are included in the field selection via setting TRUNCATECOLUMNS
	// in the COPY command, and in the `flow_document` column by setting
	// `json_parse_truncate_strings=ON` in the session that performs the Store to the target table.
	redshiftVarcharMaxLength = 65535
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database. Example: red-shift-cluster-name.account.us-east-2.redshift.amazonaws.com:5439" jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to. The materialization will attempt to connect to the default database for the provided user if omitted." jsonschema_extras:"order=3"`
	Schema   string `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=4"`

	Bucket             string `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads." jsonschema_extras:"order=5"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=6"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=7"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the S3 staging bucket. For optimal performance this should be in the same region as the Redshift database cluster." jsonschema_extras:"order=8"`
	BucketPath         string `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects in S3." jsonschema_extras:"order=9"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your Redshift cluster through an SSH server that acts as a bastion host for your network."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
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

	if c.BucketPath != "" {
		// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
		// chars in the URI and so that the object key does not start with a /.
		c.BucketPath = strings.TrimPrefix(c.BucketPath, "/")
	}

	return nil
}

func (c *config) networkTunnelEnabled() bool {
	return c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != ""
}

func (c *config) toURI() string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432 to address
	// through the bastion server, so we use the tunnel's address
	if c.networkTunnelEnabled() {
		address = "localhost:5432"
	}
	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}

	// Always require encryption when not connecting through a tunnel.
	if !c.networkTunnelEnabled() {
		uri.RawQuery = "sslmode=require"
	}

	return uri.String()
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
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)."`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{
		// Default to an explicit endpoint configuration schema, if set.
		// This will be over-written by a present `schema` property within `raw`.
		Schema: ep.Config.(*config).Schema,
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
	if c.Schema != "" {
		return []string{c.Schema, c.Table}
	}
	return []string{c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return c.AdditionalSql
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newRedshiftDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-redshift",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("could not parse endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.User,
			}).Info("opening database")

			var metaBase sql.TablePath
			if cfg.Schema != "" {
				metaBase = append(metaBase, cfg.Schema)
			}
			metaSpecs, metaCheckpoints := sql.MetaTables(metaBase)

			// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
			if cfg.networkTunnelEnabled() {
				host, port, err := net.SplitHostPort(cfg.Address)
				if err != nil {
					return nil, fmt.Errorf("splitting address to host and port: %w", err)
				}

				var sshConfig = &networkTunnel.SshConfig{
					SshEndpoint: cfg.NetworkTunnel.SshForwarding.SshEndpoint,
					PrivateKey:  []byte(cfg.NetworkTunnel.SshForwarding.PrivateKey),
					ForwardHost: host,
					ForwardPort: port,
					LocalPort:   "5432",
				}
				var tunnel = sshConfig.CreateTunnel()

				if err := tunnel.Start(); err != nil {
					return nil, fmt.Errorf("error starting network tunnel: %w", err)
				}
			}

			return &sql.Endpoint{
				Config:                      cfg,
				Dialect:                     rsDialect,
				MetaSpecs:                   &metaSpecs,
				MetaCheckpoints:             &metaCheckpoints,
				Client:                      client{uri: cfg.toURI()},
				CreateTableTemplate:         tplCreateTargetTable,
				AlterColumnNullableTemplate: tplAlterColumnNullable,
				NewResource:                 newTableConfig,
				NewTransactor:               newTransactor,
				CheckPrerequisites:          prereqs,
				Tenant:                      tenant,
			}, nil
		},
	}
}

func prereqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if db, err := stdsql.Open("pgx", cfg.toURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(pingCtx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var pgErr *pgconn.PgError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "28000":
				err = fmt.Errorf("incorrect username or password")
			case "3D000":
				err = fmt.Errorf("database %q does not exist", cfg.Database)
			}
		} else if errors.As(err, &netConnErr) {
			if netConnErr.IsNotFound {
				err = fmt.Errorf("host at address %q cannot be found", cfg.Address)
			}
		} else if errors.As(err, &netOpErr) {
			if netOpErr.Timeout() {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", cfg.Address)
			}
		}

		errs.Err(err)
	} else {
		db.Close()
	}

	s3client, err := cfg.toS3Client(ctx)
	if err != nil {
		// This is not caused by invalid S3 credentials, and would most likely be a logic error in
		// the connector code.
		errs.Err(err)
		return errs
	}

	testCol := &sql.Column{}
	testCol.Field = "test"

	s3file := newStagedFile(s3client, cfg.Bucket, cfg.BucketPath, []*sql.Column{testCol})
	s3file.start(ctx)

	var awsErr *awsHttp.ResponseError

	objectDir := path.Join(cfg.Bucket, cfg.BucketPath)

	if err := s3file.encodeRow([]interface{}{[]byte("test")}); err != nil {
		// This won't err immediately until the file is flushed in the case of having the wrong
		// bucket or authorization configured, and would most likely be caused by a logic error in
		// the connector code.
		errs.Err(err)
	} else if _, delete, err := s3file.flush(); err != nil {
		if errors.As(err, &awsErr) {
			// Handling for the two most common cases: The bucket doesn't exist, or the bucket does
			// exist but the configured credentials aren't authorized to write to it.
			if awsErr.Response.Response.StatusCode == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", cfg.Bucket)
			} else if awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to bucket %q", objectDir)
			}
		}

		errs.Err(err)
	} else {
		// Verify that the created object can be read and deleted. The unauthorized case is handled
		// & formatted specifically in these checks since the existence of the bucket has already
		// been verified by creating the temporary test object.
		if _, err := s3client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cfg.Bucket),
			Key:    s3file.uploadOutput.Key,
		}); err != nil {
			if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to read from bucket %q", objectDir)
			}
			errs.Err(err)
		}

		if err := delete(ctx); err != nil {
			if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to delete from bucket %q", objectDir)
			}
			errs.Err(err)
		}
	}

	return errs
}

// client implements the sql.Client interface.
type client struct {
	uri string
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		var specHex string
		specHex, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
		if err != nil {
			return err
		}

		specBytes, err := hex.DecodeString(specHex)
		if err != nil {
			return err
		}

		specB64 = string(specBytes)
		return err
	})
	return
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var err = c.withDB(func(db *stdsql.DB) error {
		var err error
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, func(fenceHex string) ([]byte, error) {
			fenceHexBytes, err := hex.DecodeString(fenceHex)
			if err != nil {
				return nil, err
			}

			return base64.StdEncoding.DecodeString(string(fenceHexBytes))
		})
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("pgx", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	unionSQL string
	fence    sql.Fence
	bindings []*binding
	cfg      *config
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var d = &transactor{}
	d.fence = fence
	d.cfg = ep.Config.(*config)

	s3client, err := d.cfg.toS3Client(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.Connect(ctx, d.cfg.toURI())
	if err != nil {
		return nil, fmt.Errorf("newTransactor pgx.Connect: %w", err)
	}
	defer conn.Close(ctx)

	tableVarchars, err := getVarcharLengths(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("getting existing varchar column lengths: %w", err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(
			ctx,
			binding,
			d.cfg.Bucket,
			d.cfg.BucketPath,
			s3client,
			tableVarchars[strings.ToLower(binding.Identifier)], // Redshift lowercases all table identifiers
		); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	// Build a query which unions the results of each load subquery.
	var subqueries []string
	for _, b := range d.bindings {
		subqueries = append(subqueries, b.loadQuerySQL)
	}
	d.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	return d, nil
}

type binding struct {
	target                       sql.Table
	varcharColumnMetas           []varcharColumnMeta
	loadFile                     *stagedFile
	storeFile                    *stagedFile
	createLoadTableSQL           string
	createStoreTableSQL          string
	storeUpdateDeleteExistingSQL string
	storeUpdateSQL               string
	loadQuerySQL                 string
}

// varcharColumnMeta contains metadata about Redshift varchar columns. Currently this is just the
// maximum length of the field as reported from the database, populated upon connector startup.
type varcharColumnMeta struct {
	identifier string
	maxLength  int
	isVarchar  bool
}

func (t *transactor) addBinding(
	ctx context.Context,
	target sql.Table,
	bucket string,
	bucketPath string,
	client *s3.Client,
	varchars map[string]int,
) error {
	var b = &binding{
		target:    target,
		loadFile:  newStagedFile(client, bucket, bucketPath, target.KeyPtrs()),
		storeFile: newStagedFile(client, bucket, bucketPath, target.Columns()),
	}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.createStoreTableSQL, tplCreateStoreTable},
		{&b.storeUpdateDeleteExistingSQL, tplStoreUpdateDeleteExisting},
		{&b.storeUpdateSQL, tplStoreUpdate},
		{&b.loadQuerySQL, tplLoadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	// Retain column metadata information for this binding as a snapshot of the table configuration
	// when the connector started, indexed in the same order as values will be received from the
	// runtime for Store requests. Only VARCHAR columns will have non-zero-valued varcharColumnMeta.
	allColumns := target.Columns()
	columnMetas := make([]varcharColumnMeta, len(allColumns))
	if varchars != nil { // There may not be any varchar columns for this binding
		for idx, col := range allColumns {
			// If this column is not found in varchars, it must not have been a VARCHAR column.
			if existingMaxLength, ok := varchars[strings.ToLower(col.Identifier)]; ok { // Redshift lowercases all column identifiers
				columnMetas[idx] = varcharColumnMeta{
					identifier: col.Identifier,
					maxLength:  existingMaxLength,
					isVarchar:  true,
				}

				log.WithFields(log.Fields{
					"table":            b.target.Identifier,
					"column":           col.Identifier,
					"varcharMaxLength": existingMaxLength,
					"collection":       b.target.Source.String(),
					"field":            col.Field,
				}).Debug("matched string collection field to table VARCHAR column")
			}
		}
	}
	b.varcharColumnMetas = columnMetas

	t.bindings = append(t.bindings, b)
	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()
	gotLoads := false
	for it.Next() {
		gotLoads = true

		var b = d.bindings[it.Binding]
		b.loadFile.start(ctx)

		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		if err := b.loadFile.encodeRow(converted); err != nil {
			return fmt.Errorf("encoding row for load: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if !gotLoads {
		// Early return as an optimization to avoid the remaining load queries if no loads were
		// received, as would be the case if all bindings of the materialization were set to use
		// delta updates.
		return nil
	}

	conn, err := pgx.Connect(ctx, d.cfg.toURI())
	if err != nil {
		return fmt.Errorf("load pgx.Connect: %w", err)
	}
	defer conn.Close(ctx)

	// Transaction for processing the loads.
	txn, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("load BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	var batch pgx.Batch
	for idx, b := range d.bindings {
		// The temporary load table for each binding always needs to be created because it is used
		// in the UNION ALL load query. This may be an opportunity for a (minor) future
		// optimization.
		batch.Queue(b.createLoadTableSQL)

		if !b.loadFile.started {
			// No loads for this binding.
			continue
		}

		objectLocation, delete, err := b.loadFile.flush()
		if err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		var copySql strings.Builder
		if err := tplCopyFromS3.Execute(&copySql, copyFromS3Params{
			Destination:    fmt.Sprintf("flow_temp_table_%d", idx),
			ObjectLocation: objectLocation,
			Config:         *d.cfg,
		}); err != nil {
			return fmt.Errorf("evaluating copy from s3 template: %w", err)
		}

		batch.Queue(copySql.String())
	}

	res := txn.SendBatch(ctx, &batch)
	for idx := 0; idx < batch.Len(); idx++ {
		if _, err := res.Exec(); err != nil {
			return fmt.Errorf("exec load batch index %d: %w", idx, err)
		}
	}
	if err := res.Close(); err != nil {
		return fmt.Errorf("closing load batch: %w", err)
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := txn.Query(ctx, d.unionSQL)
	if err != nil {
		return fmt.Errorf("querying load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document json.RawMessage

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("commiting load transaction: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()

	// hasUpdates is used to track if a given binding includes only insertions for this store round
	// or if it includes any updates. If it is only insertions a more efficient direct copy from S3
	// can be performed into the target table rather than copying into a staging table and merging
	// into the target table.
	hasUpdates := make([]bool, len(d.bindings))

	// varcharColumnUpdates records any VARCHAR columns that need their lengths increased. The keys
	// are table identifiers, and the values are a list of column identifiers that need altered.
	// Columns will only ever be to altered it to VARCHAR(MAX).
	varcharColumnUpdates := make(map[string][]string)

	for it.Next() {
		if it.Exists {
			hasUpdates[it.Binding] = true
		}

		var b = d.bindings[it.Binding]
		b.storeFile.start(ctx)

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		// See if we need to increase any VARCHAR column lengths.
		for idx, c := range converted {
			varcharMeta := b.varcharColumnMetas[idx]
			if varcharMeta.isVarchar {
				switch v := c.(type) {
				case string:
					// If the column is already at its maximum length, it can't be enlarged any
					// more. The string values will be truncated by Redshift when loading them into
					// the table.
					if len(v) > varcharMeta.maxLength && varcharMeta.maxLength != redshiftVarcharMaxLength {
						log.WithFields(log.Fields{
							"table":               b.target.Identifier,
							"column":              varcharMeta.identifier,
							"currentColumnLength": varcharMeta.maxLength,
							"stringValueLength":   len(v),
						}).Info("column will be altered to VARCHAR(MAX) to accommodate large string value")
						varcharColumnUpdates[b.target.Identifier] = append(varcharColumnUpdates[b.target.Identifier], varcharMeta.identifier)
						b.varcharColumnMetas[idx].maxLength = redshiftVarcharMaxLength // Do not need to alter this column again.
					}
				case nil:
					// Values for string fields may be null, in which case there is nothing to do.
					continue
				default:
					// Invariant: This value must either be a string or nil, since the column it is
					// going into is a VARCHAR.
					return nil, fmt.Errorf("expected type string or nil for column %s of table %s, got %T", varcharMeta.identifier, b.target.Identifier, c)
				}
			}
		}

		if err := b.storeFile.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var err error
		if d.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.fence); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("evaluating fence update template: %w", err))
		}

		return nil, pf.RunAsyncOperation(func() error { return d.commit(ctx, fenceUpdate.String(), hasUpdates, varcharColumnUpdates) })
	}, nil
}

func (d *transactor) commit(ctx context.Context, fenceUpdate string, hasUpdates []bool, varcharColumnUpdates map[string][]string) error {
	conn, err := pgx.Connect(ctx, d.cfg.toURI())
	if err != nil {
		return fmt.Errorf("store pgx.Connect: %w", err)
	}
	defer conn.Close(ctx)

	// Truncate strings within SUPER types by setting this option, since these have the same limits
	// on maximum VARCHAR lengths as table columns do. Notably, this will truncate any strings in
	// `flow_document` (stored as a SUPER column) that otherwise would prevent the row from being
	// added to the table.
	if _, err := conn.Exec(ctx, "SET json_parse_truncate_strings=ON;"); err != nil {
		return fmt.Errorf("configuring json_parse_truncate_strings=ON: %w", err)
	}

	// Update any columns that require setting to VARCHAR(MAX) for storing large strings. ALTER
	// TABLE ALTER COLUMN statements cannot be run inside transaction blocks.
	for table, updates := range varcharColumnUpdates {
		for _, column := range updates {
			if _, err := conn.Exec(ctx, fmt.Sprintf(varcharTableAlter, table, column)); err != nil {
				// It is possible that another shard of this materialization will have already
				// updated this column. Practically this means we will try to set a column that is
				// already VARCHAR(MAX) to VARCHAR(MAX), and Redshift returns a specific error in
				// this case that can be safely discarded.
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					if pgErr.Code == "0A000" && strings.Contains(pgErr.Message, "target column size should be different") {
						log.WithFields(log.Fields{
							"table":  table,
							"column": column,
						}).Info("attempted to alter column VARCHAR(MAX) but it was already VARCHAR(MAX)")
						continue
					}
				}

				return fmt.Errorf("altering size for column %s of table %s: %w", column, table, err)
			}

			log.WithFields(log.Fields{
				"table":  table,
				"column": column,
			}).Info("column altered to VARCHAR(MAX) to accommodate large string value")
		}
	}

	txn, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("store BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	// If there are multiple materializations operating on different tables within the same database
	// using the same metadata table path, they will need to concurrently update the checkpoints
	// table. Aquiring a table-level lock prevents "serializable isolation violation" errors in this
	// case (they still happen even though the queries update different _rows_ in the same table),
	// although it means each materialization will have to take turns updating the checkpoints
	// table. For best performance, a single materialization per database should be used, or
	// separate materializations within the same database should use a different schema for their
	// metadata.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", rsDialect.Identifier(d.fence.TablePath...))); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	var batch pgx.Batch

	for idx, b := range d.bindings {
		if !b.storeFile.started {
			// No loads for this binding.
			continue
		}

		objectLocation, delete, err := b.storeFile.flush()
		if err != nil {
			return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		var dest string
		if hasUpdates[idx] {
			// Must merge from the staging table.
			dest = fmt.Sprintf("flow_temp_table_%d", idx)
		} else {
			// Can copy directly into the target table.
			dest = b.target.Identifier
		}

		var copySql strings.Builder
		if err := tplCopyFromS3.Execute(&copySql, copyFromS3Params{
			Destination:    dest,
			ObjectLocation: objectLocation,
			Config:         *d.cfg,
		}); err != nil {
			return fmt.Errorf("evaluating copy from s3 template: %w", err)
		}

		batch.Queue(copySql.String())

		if hasUpdates[idx] {
			// This command must be executed separately so that the table is "visible" for the batch
			// commands.
			if _, err := txn.Exec(ctx, b.createStoreTableSQL); err != nil {
				return fmt.Errorf("creating store table: %w", err)
			}
			batch.Queue(b.storeUpdateDeleteExistingSQL)
			batch.Queue(b.storeUpdateSQL)
		}
	}

	// The fence update is always the last query in the batch.
	batch.Queue(fenceUpdate)

	res := txn.SendBatch(ctx, &batch)

	for idx := 0; idx < batch.Len()-1; idx++ {
		if _, err := res.Exec(); err != nil {
			return fmt.Errorf("exec store batch index %d: %w", idx, err)
		}
	}

	fenceRes, err := res.Exec()
	if err != nil {
		return fmt.Errorf("fetching fence update rows: %w", err)
	} else if fenceRes.RowsAffected() != 1 {
		return errors.New("this instance was fenced off by another")
	}
	if err := res.Close(); err != nil {
		return fmt.Errorf("closing store batch: %w", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("committing store transaction: %w", err)
	}

	return nil
}

func (d *transactor) Destroy() {}
