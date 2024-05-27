package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"text/template"
	"time"

	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	// Imported for side-effects (registers the required stdsql driver)
	_ "github.com/jackc/pgx/v4/stdlib"
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

	// The default TEXT column is an alias for VARCHAR(256).
	redshiftTextColumnLength = 256
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type config struct {
	Address    string `json:"address" jsonschema:"title=Address,description=Host and port of the database. Example: red-shift-cluster-name.account.us-east-2.redshift.amazonaws.com:5439" jsonschema_extras:"order=0"`
	User       string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password   string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database   string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to. The materialization will attempt to connect to the default database for the provided user if omitted." jsonschema_extras:"order=3"`
	Schema     string `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=4"`
	HardDelete bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=8"`

	Bucket             string `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads." jsonschema_extras:"order=5"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=6"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=7"`
	Region             string `json:"region" jsonschema:"title=Region,description=Region of the S3 staging bucket. For optimal performance this should be in the same region as the Redshift database cluster." jsonschema_extras:"order=8"`
	BucketPath         string `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects in S3." jsonschema_extras:"order=9"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your Redshift cluster through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce active cluster time by increasing the delay between updates. Defaults to 30 minutes if unset.,enum=0s,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`
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

	if _, err := m.ParseDelay(c.Advanced.UpdateDelay); err != nil {
		return err
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
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)."`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
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

			db, err := stdsql.Open("pgx", cfg.toURI())
			if err != nil {
				return nil, fmt.Errorf("opening db: %w", err)
			}
			defer db.Close()

			var caseSensitiveIdentifier string
			if err := db.QueryRowContext(ctx, "SHOW enable_case_sensitive_identifier;").Scan(&caseSensitiveIdentifier); err != nil {
				return nil, fmt.Errorf("querying enable_case_sensitive_identifier: %w", err)
			}

			var caseSensitiveIdentifierEnabled = strings.EqualFold(caseSensitiveIdentifier, "on")
			var dialect = rsDialect(caseSensitiveIdentifierEnabled)
			var templates = renderTemplates(dialect)

			if caseSensitiveIdentifierEnabled {
				log.WithField("caseSensitiveIdentifierEnabled", caseSensitiveIdentifierEnabled).Info("detected value for enable_case_sensitive_identifier")
			}

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             dialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       prepareNewTransactor(templates, caseSensitiveIdentifierEnabled),
				Tenant:              tenant,
				ConcurrentApply:     true,
			}, nil
		},
	}
}

var _ m.DelayedCommitter = (*transactor)(nil)

type transactor struct {
	templates   templates
	dialect     sql.Dialect
	fence       sql.Fence
	bindings    []*binding
	cfg         *config
	updateDelay time.Duration
}

func prepareNewTransactor(
	templates templates,
	caseSensitiveIdentifierEnabled bool,
) func(context.Context, *sql.Endpoint, sql.Fence, []sql.Table, pm.Request_Open) (m.Transactor, error) {
	return func(
		ctx context.Context,
		ep *sql.Endpoint,
		fence sql.Fence,
		bindings []sql.Table,
		open pm.Request_Open,
	) (_ m.Transactor, err error) {
		var cfg = ep.Config.(*config)

		var d = &transactor{
			templates: templates,
			dialect:   ep.Dialect,
			fence:     fence,
			cfg:       cfg,
		}

		if d.updateDelay, err = m.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
			return nil, err
		}

		s3client, err := d.cfg.toS3Client(ctx)
		if err != nil {
			return nil, err
		}

		db, err := stdsql.Open("pgx", d.cfg.toURI())
		if err != nil {
			return nil, err
		}
		defer db.Close()

		schemas := []string{}
		for _, b := range bindings {
			if !slices.Contains(schemas, b.InfoLocation.TableSchema) {
				schemas = append(schemas, b.InfoLocation.TableSchema)
			}
		}

		catalog := cfg.Database
		if catalog == "" {
			// An endpoint-level database configuration is not required, so query for the active
			// database if that's the case.
			if err := db.QueryRowContext(ctx, "select current_database();").Scan(&catalog); err != nil {
				return nil, fmt.Errorf("querying for connected database: %w", err)
			}
		}
		resourcePaths := make([][]string, 0, len(open.Materialization.Bindings))
		for _, b := range open.Materialization.Bindings {
			resourcePaths = append(resourcePaths, b.ResourcePath)
		}
		is, err := sql.StdFetchInfoSchema(ctx, db, ep.Dialect, catalog, resourcePaths)
		if err != nil {
			return nil, err
		}

		for idx, target := range bindings {
			if err = d.addBinding(
				idx,
				target,
				s3client,
				is,
				caseSensitiveIdentifierEnabled,
			); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", target.Path, err)
			}
		}

		return d, nil
	}
}

type binding struct {
	target                  sql.Table
	varcharColumnMetas      []varcharColumnMeta
	loadFile                *stagedFile
	storeFile               *stagedFile
	deleteFile              *stagedFile
	createLoadTableTemplate *template.Template
	createStoreTableSQL     string
	createDeleteTableSQL    string
	mergeIntoSQL            string
	deleteQuerySQL          string
	loadQuerySQL            string
	copyIntoLoadTableSQL    string
	copyIntoMergeTableSQL   string
	copyIntoDeleteTableSQL  string
	copyIntoTargetTableSQL  string
}

// varcharColumnMeta contains metadata about Redshift varchar columns. Currently this is just the
// maximum length of the field as reported from the database, populated upon connector startup.
type varcharColumnMeta struct {
	identifier string
	maxLength  int
	isVarchar  bool
}

func (t *transactor) addBinding(
	bindingIdx int,
	target sql.Table,
	client *s3.Client,
	is *boilerplate.InfoSchema,
	caseSensitiveIdentifierEnabled bool,
) error {
	var b = &binding{
		target:     target,
		loadFile:   newStagedFile(client, t.cfg.Bucket, t.cfg.BucketPath, target.KeyNames()),
		storeFile:  newStagedFile(client, t.cfg.Bucket, t.cfg.BucketPath, target.ColumnNames()),
		deleteFile: newStagedFile(client, t.cfg.Bucket, t.cfg.BucketPath, target.KeyNames()),
	}

	// Render templates that require specific S3 "COPY INTO" parameters.
	for _, m := range []struct {
		sql             *string
		target          string
		columns         []*sql.Column
		truncateColumns bool
		stagedFile      *stagedFile
	}{
		{&b.copyIntoLoadTableSQL, fmt.Sprintf("flow_temp_table_%d", bindingIdx), target.KeyPtrs(), false, b.loadFile},
		{&b.copyIntoMergeTableSQL, fmt.Sprintf("flow_temp_table_%d", bindingIdx), target.Columns(), true, b.storeFile},
		{&b.copyIntoDeleteTableSQL, fmt.Sprintf("flow_temp_table_%d_deleted", bindingIdx), target.KeyPtrs(), false, b.deleteFile},
		{&b.copyIntoTargetTableSQL, target.Identifier, target.Columns(), true, b.storeFile},
	} {
		var sql strings.Builder
		if err := t.templates.copyFromS3.Execute(&sql, copyFromS3Params{
			Target:                         m.target,
			Columns:                        m.columns,
			ManifestURL:                    m.stagedFile.fileURI(manifestFile),
			Config:                         t.cfg,
			CaseSensitiveIdentifierEnabled: caseSensitiveIdentifierEnabled,
			TruncateColumns:                m.truncateColumns,
		}); err != nil {
			return err
		}
		*m.sql = sql.String()
	}

	// Render templates that rely only on the target table.
	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createStoreTableSQL, t.templates.createStoreTable},
		{&b.createDeleteTableSQL, t.templates.createDeleteTable},
		{&b.mergeIntoSQL, t.templates.mergeInto},
		{&b.deleteQuerySQL, t.templates.deleteQuery},
		{&b.loadQuerySQL, t.templates.loadQuery},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	// The load table template is re-evaluated every transaction to account for the specific string
	// lengths observed for string keys in the load key set.
	b.createLoadTableTemplate = t.templates.createLoadTable

	// Retain column metadata information for this binding as a snapshot of the target table
	// configuration when the connector started, indexed in the same order as values will be
	// received from the runtime for Store requests. Only VARCHAR columns will have non-zero-valued
	// varcharColumnMeta.
	allColumns := target.Columns()
	columnMetas := make([]varcharColumnMeta, len(allColumns))
	for idx, col := range allColumns {
		existing, err := is.GetField(target.Path, col.Field)
		if err != nil {
			return fmt.Errorf("getting existing column metadata: %w", err)
		}

		if existing.Type == "character varying" {
			columnMetas[idx] = varcharColumnMeta{
				identifier: col.Identifier,
				maxLength:  existing.CharacterMaxLength,
				isVarchar:  true,
			}

			log.WithFields(log.Fields{
				"table":            b.target.Identifier,
				"column":           col.Identifier,
				"varcharMaxLength": existing.CharacterMaxLength,
				"collection":       b.target.Source.String(),
				"field":            col.Field,
			}).Debug("matched string collection field to table VARCHAR column")
		}
	}
	b.varcharColumnMetas = columnMetas

	t.bindings = append(t.bindings, b)
	return nil
}

func (t *transactor) AckDelay() time.Duration {
	return t.updateDelay
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()
	gotLoads := false

	// Keep track of the maximum length of any string values encountered so the temporary load table
	// can be created with long enough string columns.
	maxStringLengths := make([]int, len(d.bindings))

	log.Info("load: starting encoding and uploading of files")
	for it.Next() {
		gotLoads = true

		var b = d.bindings[it.Binding]
		b.loadFile.start()

		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		for _, c := range converted {
			if s, ok := c.(string); ok {
				l := len(s)

				if l > redshiftVarcharMaxLength {
					return fmt.Errorf(
						"cannot load string keys with byte lengths longer than %d: collection for table %s had a string key with length %d",
						redshiftVarcharMaxLength,
						b.target.Identifier,
						l,
					)
				}

				if l > redshiftTextColumnLength && l > maxStringLengths[it.Binding] {
					maxStringLengths[it.Binding] = l
				}
			}
		}

		if err := b.loadFile.encodeRow(ctx, converted); err != nil {
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

	var subqueries []string
	for idx, b := range d.bindings {
		if !b.loadFile.started {
			// No loads for this binding.
			continue
		}

		subqueries = append(subqueries, b.loadQuerySQL)

		var createLoadTableSQL strings.Builder
		if err := b.createLoadTableTemplate.Execute(&createLoadTableSQL, loadTableParams{
			Target:        b.target,
			VarCharLength: maxStringLengths[idx],
		}); err != nil {
			return fmt.Errorf("evaluating create load table template: %w", err)
		} else if _, err := txn.Exec(ctx, createLoadTableSQL.String()); err != nil {
			return fmt.Errorf("creating load table for target table '%s': %w", b.target.Identifier, err)
		}

		delete, err := b.loadFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		if _, err := txn.Exec(ctx, b.copyIntoLoadTableSQL); err != nil {
			return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.loadFile.prefix, b.target.Identifier, err)
		}
	}

	log.Info("load: finished encoding and uploading of files")

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	loadAllSQL := strings.Join(subqueries, "\nUNION ALL\n") + ";"
	rows, err := txn.Query(ctx, loadAllSQL)
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

	log.Info("load: finished loading")

	return nil
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
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

	log.Info("store: starting encoding and uploading of files")
	for it.Next() {
		var b = d.bindings[it.Binding]
		var file *stagedFile

		if it.Exists {
			hasUpdates[it.Binding] = true
		}

		var err error
		var converted []any

		// Unfortunately Redshift does not support MERGE INTO statements with multiple clauses
		// so we can't use it to both update and delete rows. So instead we use a separate
		// temporary table and run a `DELETE USING` query to delete rows
		if d.cfg.HardDelete && it.Delete {
			file = b.deleteFile

			converted, err = b.target.ConvertKey(it.Key)
			if err != nil {
				return nil, fmt.Errorf("converting delete parameters: %w", err)
			}
		} else {
			file = b.storeFile

			converted, err = b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
			if err != nil {
				return nil, fmt.Errorf("converting store parameters: %w", err)
			}
		}

		file.start()

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

		if err := file.encodeRow(ctx, converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		log.Info("store: starting commit phase")
		var err error
		if d.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := d.templates.updateFence.Execute(&fenceUpdate, d.fence); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("evaluating fence update template: %w", err))
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
	// table. Acquiring a table-level lock prevents "serializable isolation violation" errors in
	// this case (they still happen even though the queries update different _rows_ in the same
	// table), although it means each materialization will have to take turns updating the
	// checkpoints table. For best performance, a single materialization per database should be
	// used, or separate materializations within the same database should use a different schema for
	// their metadata.
	if _, err := txn.Exec(ctx, fmt.Sprintf("lock %s;", d.dialect.Identifier(d.fence.TablePath...))); err != nil {
		return fmt.Errorf("obtaining checkpoints table lock: %w", err)
	}

	log.Info("store: starting copying of files into tables")
	for idx, b := range d.bindings {
		if b.deleteFile.started {
			deleteDeleted, err := b.deleteFile.flush(ctx)
			if err != nil {
				return fmt.Errorf("flushing delete file for binding[%d]: %w", idx, err)
			}
			defer deleteDeleted(ctx)

			// Create the temporary table for staging values to delete from the target table.
			// Redshift actually supports transactional DDL for creating tables, so this can be
			// executed within the transaction.
			if _, err := txn.Exec(ctx, b.createDeleteTableSQL); err != nil {
				return fmt.Errorf("creating delete table: %w", err)
			}

			log.WithField("table", b.target.Identifier).Info("store: starting deleting from table")

			if _, err := txn.Exec(ctx, b.copyIntoDeleteTableSQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.deleteFile.prefix, b.target.Identifier, err)
			} else if _, err := txn.Exec(ctx, b.deleteQuerySQL); err != nil {
				return fmt.Errorf("deleting from table '%s': %w", b.target.Identifier, err)
			}
			log.WithField("table", b.target.Identifier).Info("store: finished deleting from table")
		}

		if !b.storeFile.started {
			continue
		}

		deleteStored, err := b.storeFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}
		defer deleteStored(ctx)

		if hasUpdates[idx] {
			// Create the temporary table for staging values to merge into the target table.
			// Redshift actually supports transactional DDL for creating tables, so this can be
			// executed within the transaction.
			if _, err := txn.Exec(ctx, b.createStoreTableSQL); err != nil {
				return fmt.Errorf("creating store table: %w", err)
			}

			log.WithField("table", b.target.Identifier).Info("store: starting merging data into table")
			if _, err := txn.Exec(ctx, b.copyIntoMergeTableSQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.storeFile.prefix, b.target.Identifier, err)
			} else if _, err := txn.Exec(ctx, b.mergeIntoSQL); err != nil {
				return fmt.Errorf("merging to table '%s': %w", b.target.Identifier, err)
			}
			log.WithField("table", b.target.Identifier).Info("store: finished merging data into table")
		} else {
			log.WithField("table", b.target.Identifier).Info("store: starting direct copying data into table")
			// Can copy directly into the target table since all values are new.
			if _, err := txn.Exec(ctx, b.copyIntoTargetTableSQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, b.storeFile.prefix, b.target.Identifier, err)
			}
			log.WithField("table", b.target.Identifier).Info("store: finishing direct copying data into table")
		}
	}

	log.Info("store: finished encoding and uploading of files")

	if fenceRes, err := txn.Exec(ctx, fenceUpdate); err != nil {
		return fmt.Errorf("fetching fence update rows: %w", err)
	} else if fenceRes.RowsAffected() != 1 {
		return errors.New("this instance was fenced off by another")
	} else if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("committing store transaction: %w", err)
	}

	log.Info("store: finished commit")
	return nil
}

// handleCopyIntoErr queries the `sys_load_error_detail` table for relevant COPY INTO error details
// and returns a more useful error than the opaque error returned by Redshift. This function will
// always return an error. `sys_load_error_detail` is queried instead of `stl_load_errors` since it
// is available to both serverless and provisioned versions of Redshift, whereas `stl_load_errors`
// is only available on provisioned Redshift.
func handleCopyIntoErr(ctx context.Context, txn pgx.Tx, bucket, prefix, table string, copyIntoErr error) error {
	// The transaction has failed. It must be finish being rolled back before using its underlying
	// connection again.
	txn.Rollback(ctx)
	conn := txn.Conn()

	loadErrInfo, err := getLoadErrorInfo(ctx, conn, bucket, prefix)
	if err != nil {
		// If querying sys_load_error_detail fails for some reason, return the original error back
		// unchanged, but log why sys_load_error_detail could not be queried. Some errors (target
		// table doesn't exist, cluster ran out of memory, and maybe others) don't result in an
		// error being added to sys_load_error_detail, but do have a descriptive error message from
		// the COPY INTO operation.
		log.WithError(err).Info("COPY INTO error but could not query sys_load_error_detail")
		return copyIntoErr
	}

	log.WithFields(log.Fields{
		"table":       table,
		"copyInfoErr": copyIntoErr,
		"errMsg":      loadErrInfo.errMsg,
		"errCode":     loadErrInfo.errCode,
		"colName":     loadErrInfo.colName,
		"colType":     loadErrInfo.colType,
		"colLength":   loadErrInfo.colLength,
	}).Info("loadErrInfo")

	// See https://docs.aws.amazon.com/redshift/latest/dg/r_Load_Error_Reference.html for load error
	// codes.
	switch code := loadErrInfo.errCode; code {
	case 1204:
		// Input data exceeded the acceptable range for the data type. This is a case where the
		// column has some kind of length limit (like a VARCHAR(X)), but the input to the column is too
		// long.
		return fmt.Errorf(
			"cannot COPY INTO table '%s' column '%s' having type '%s' and allowable length '%s': %s (code %d)",
			table,
			loadErrInfo.colName,
			loadErrInfo.colType,
			loadErrInfo.colLength,
			loadErrInfo.errMsg,
			loadErrInfo.errCode,
		)
	case 1216:
		// General "Input line is not valid" error. This is almost always going to be because a very
		// large document is in the collection being materialized, and Redshift cannot parse single
		// JSON lines larger than 4MB.
		return fmt.Errorf(
			"cannot COPY INTO table '%s': %s (code %d)",
			table,
			loadErrInfo.errMsg,
			loadErrInfo.errCode,
		)
	case 1224:
		// A problem copying into a SUPER column. The most common cause is field of the root
		// document being excessively large (ex: an object field larger than 1MB, or having too many
		// attributes). May also happen for an individual selected field that is mapped as a SUPER
		// column type.
		return fmt.Errorf(
			"cannot COPY INTO table '%s' SUPER column '%s': %s (code %d)",
			table,
			loadErrInfo.colName,
			loadErrInfo.errMsg,
			loadErrInfo.errCode,
		)
	default:
		// Catch-all: Return general information from sys_load_error_detail. This will be pretty
		// helpful on its own. We'll want to specifically handle any other cases as we see them come
		// up frequently.
		return fmt.Errorf(
			"COPY INTO table `%s` failed with details from sys_load_error_detail: column_name: '%s', column_type: '%s', column_length: '%s', error_code: %d: error_message: %s",
			table,
			loadErrInfo.colName,
			loadErrInfo.colType,
			loadErrInfo.colLength,
			loadErrInfo.errCode,
			loadErrInfo.errMsg,
		)
	}
}

func (d *transactor) Destroy() {}
