package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"text/template"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	networkTunnel "github.com/estuary/connectors/go-network-tunnel"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	log "github.com/sirupsen/logrus"
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to." jsonschema_extras:"order=3"`

	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID" jsonschema_extras:"order=0"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key" jsonschema_extras:"secret=true,order=1"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region for the staging bucket. Should be in the same region as the Redshift database cluster." jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=Bucket,description=Name of the staging bucket." jsonschema_extras:"order=3"`

	Schema string `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables" jsonschema_extras:"order=4"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
		{"bucket", c.Bucket},
	}
	// TODO: Is database required?
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432 to address
	// through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
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
	return uri.String()
}

// TODO: Support schema as well
type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)"`
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

func newRedshiftDriver() pm.DriverServer {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-redshift",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
			}

			// TODO: Add checks for S3 access.

			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.User,
			}).Info("opening database")

			var metaBase sql.TablePath
			if cfg.Schema != "" {
				metaBase = append(metaBase, cfg.Schema)
			}
			var metaSpecs, metaCheckpoints = metaTables(metaBase)

			// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
			if cfg.NetworkTunnel != nil && cfg.NetworkTunnel.SshForwarding != nil && cfg.NetworkTunnel.SshForwarding.SshEndpoint != "" {
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

				err = tunnel.Start()

				if err != nil {
					log.WithField("error", err).Error("network tunnel error")
				}
			}

			return &sql.Endpoint{
				Config:                      cfg,
				Dialect:                     rsDialect,
				MetaSpecs:                   metaSpecs,
				MetaCheckpoints:             &metaCheckpoints,
				Client:                      client{uri: cfg.ToURI()},
				CreateTableTemplate:         tplCreateTargetTable,
				AlterColumnNullableTemplate: tplAlterColumnNullable,
				NewResource:                 newTableConfig,
				NewTransactor:               newTransactor,
			}, nil
		},
	}
}

// client implements the sql.Client interface.
type client struct {
	uri string
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		specB64, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
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
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence)
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
	// Variables exclusively used by Load.
	load struct {
		conn     *pgx.Conn
		unionSQL string
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *pgx.Conn
		fence sql.Fence
	}
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
	d.store.fence = fence

	var cfg = ep.Config.(*config)
	d.cfg = cfg

	if d.load.conn, err = pgx.Connect(ctx, cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load pgx.Connect: %w", err)
	}
	if d.store.conn, err = pgx.Connect(ctx, cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store pgx.Connect: %w", err)
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(cfg.Region),
	)
	if err != nil {
		return nil, err
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding, cfg.Bucket, s3.NewFromConfig(awsCfg)); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	// Build a query which unions the results of each load subquery.
	var subqueries []string
	for _, b := range d.bindings {
		subqueries = append(subqueries, b.loadQuerySQL)
	}
	d.load.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	return d, nil
}

type binding struct {
	target                       sql.Table
	loadFile                     *stagedFile
	storeFile                    *stagedFile
	createLoadTableSQL           string
	createStoreTableSQL          string
	truncateTempTableSQL         string
	storeUpdateDeleteExistingSQL string
	storeUpdateSQL               string
	loadQuerySQL                 string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, bucket string, client *s3.Client) error {
	var b = &binding{
		target:    target,
		loadFile:  newStagedFile(client, bucket, target.KeyPtrs()),
		storeFile: newStagedFile(client, bucket, target.Columns()),
	}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.createStoreTableSQL, tplCreateStoreTable},
		{&b.truncateTempTableSQL, tplTruncateTempTable},
		{&b.storeUpdateDeleteExistingSQL, tplStoreUpdateDeleteExisting},
		{&b.storeUpdateSQL, tplStoreUpdate},
		{&b.loadQuerySQL, tplLoadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)
	return nil
}

// TODO: Handling non-serializable errors :(.
func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	log.Info("loading")

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

		b.loadFile.encodeRow(converted)

	}
	if it.Err() != nil {
		return it.Err()
	}

	if !gotLoads {
		// Optimization to avoid the remaining load query logic if no loads were received, as would
		// be the case if all bindings of the materialization were set to use delta updates.
		return nil
	}

	if err := d.prepareTempTables(ctx, true); err != nil {
		return fmt.Errorf("preparing load temp tables: %w", err)
	}

	// Transaction for processing the loads.
	txn, err := d.load.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("load BeginTx: %w", err)
	}

	var loadBatch pgx.Batch
	for idx, b := range d.bindings {
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

		loadBatch.Queue(copySql.String())
	}

	if err := runBatch(loadBatch.Len(), txn.SendBatch(ctx, &loadBatch)); err != nil {
		return fmt.Errorf("sending load batch: %w", err)
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := txn.Query(ctx, d.load.unionSQL)
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
	log.Info("storing")

	ctx := it.Context()

	// hasUpdates is used to track if a given binding includes only insertions for this store round
	// or if it includes any updates. If it is only insertions a more efficient direct copy from S3
	// can be performed into the target table rather than copying into a staging table and merging
	// into the target table.
	hasUpdates := make([]bool, len(d.bindings))
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

		b.storeFile.encodeRow(converted)

	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint []byte, runtimeAckCh <-chan struct{}) (*pf.DriverCheckpoint, pf.OpFuture) {
		d.store.fence.Checkpoint = runtimeCheckpoint

		var fenceGet strings.Builder
		if err := tplGetFence.Execute(&fenceGet, d.store.fence); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("evaluating fence get template: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("evaluating fence update template: %w", err))
		}

		return nil, pf.RunAsyncOperation(func() error { return d.commit(ctx, fenceGet.String(), fenceUpdate.String(), hasUpdates) })
	}, nil
}

func (d *transactor) commit(ctx context.Context, fenceGet string, fenceUpdate string, hasUpdates []bool) error {
	log.Info("committing")

	if err := d.prepareTempTables(ctx, false); err != nil {
		return fmt.Errorf("preparing store temp tables: %w", err)
	}

	txn, err := d.store.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("store BeginTx: %w", err)
	}
	var batch pgx.Batch

	for idx, b := range d.bindings {
		if !b.storeFile.started {
			// no loads for this binding
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
			batch.Queue(b.storeUpdateDeleteExistingSQL)
			batch.Queue(b.storeUpdateSQL)
		}
	}

	batch.Queue(fenceUpdate)
	batch.Queue(fenceGet)

	res := txn.SendBatch(ctx, &batch)

	for idx := 0; idx < batch.Len()-1; idx++ {
		if _, err := res.Exec(); err != nil {
			return fmt.Errorf("exec store batch: %w", err)
		}
	}

	// The fence confirmation is always the last query in the batch.
	if err := res.QueryRow().Scan(nil); err != nil {
		if err == stdsql.ErrNoRows {
			return errors.New("this instance was fenced off by another")
		}
		return fmt.Errorf("checking fence: %w", err)
	}
	if err := res.Close(); err != nil {
		return fmt.Errorf("closing store batch: %w", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("commiting store transaction: %w", err)
	}

	return nil
}

func (d *transactor) Destroy() {}

// Initialize temp tables for all bindings. They will be created if they don't yet exist and
// truncated. The TRUNCATE operation will commit a current transaction, so it must be run
// separately.
func (d *transactor) prepareTempTables(ctx context.Context, load bool) error {
	conn := d.store.conn
	if load {
		conn = d.load.conn
	}

	var initBatch pgx.Batch
	for _, b := range d.bindings {
		sql := b.createStoreTableSQL
		if load {
			sql = b.createLoadTableSQL
		}
		initBatch.Queue(sql)
	}

	if err := runBatch(initBatch.Len(), conn.SendBatch(ctx, &initBatch)); err != nil {
		return fmt.Errorf("temp table init: %w", err)
	}

	var truncateBatch pgx.Batch
	for _, b := range d.bindings {
		truncateBatch.Queue(b.truncateTempTableSQL)
	}
	if err := runBatch(truncateBatch.Len(), conn.SendBatch(ctx, &truncateBatch)); err != nil {
		return fmt.Errorf("truncating temp table: %w", err)
	}

	return nil
}

func runBatch(l int, b pgx.BatchResults) error {
	for idx := 0; idx < l; idx++ {
		if _, err := b.Exec(); err != nil {
			return fmt.Errorf("exec index %d: %w", idx, err)
		}
	}
	if err := b.Close(); err != nil {
		return fmt.Errorf("closing batch: %w", err)
	}

	return nil
}

func metaTables(metaBase sql.TablePath) (specs sql.TableShape, checkpoints sql.TableShape) {
	metaSpecs, metaCheckpoints := sql.MetaTables(metaBase)

	// Our stored base64-encoded specs and checkpoints can be quite long, so we need to use a
	// permissive text column size.

	maxColumnLength := uint32(65535) // (64K-1), which is the maximum allowable in Redshift.

	for _, p := range metaSpecs.Values {
		if p.Field == "spec" {
			p.Inference.String_.MaxLength = maxColumnLength
		}
	}

	// Napkin math suggests that each source journal adds about ~350 bytes to the checkpoint size,
	// so a 64K maximum checkpoint size allows for somewhere in the ballpark of 100-200 source
	// journals.
	for _, p := range metaCheckpoints.Values {
		if p.Field == "checkpoint" {
			p.Inference.String_.MaxLength = maxColumnLength
		}
	}

	return metaSpecs, metaCheckpoints
}
