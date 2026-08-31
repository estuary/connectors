package connector

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"path"
	"slices"
	"strings"
	"text/template"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/estuary/connectors/go/blob"
	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	// Imported for side-effects (registers the required stdsql driver)
	_ "github.com/jackc/pgx/v5/stdlib"
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

var featureFlagDefaults = map[string]bool{
	"datetime_keys_as_string":          true,
	"s3_use_dualstack_endpoints":       false,
	"retain_existing_data_on_backfill": false,
	"native_binary_column_type":        true,
}

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type advancedConfig struct {
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false" jsonschema_extras:"nonsensitive=true"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

type config struct {
	Address            string           `json:"address" jsonschema:"title=Address,description=Host and port of the database. Example: red-shift-cluster-name.account.us-east-2.redshift.amazonaws.com:5439" jsonschema_extras:"order=0"`
	User               string           `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password           string           `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database           string           `json:"database,omitempty" jsonschema:"title=Database,description=Name of the logical database to materialize to. The materialization will attempt to connect to the default database for the provided user if omitted." jsonschema_extras:"order=3"`
	Schema             string           `json:"schema,omitempty" jsonschema:"title=Database Schema,default=public,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=4"`
	Bucket             string           `json:"bucket" jsonschema:"title=S3 Staging Bucket,description=Name of the S3 bucket to use for staging data loads." jsonschema_extras:"order=5"`
	AWSAccessKeyID     string           `json:"awsAccessKeyId" jsonschema:"title=Access Key ID,description=AWS Access Key ID for reading and writing data to the S3 staging bucket." jsonschema_extras:"order=6"`
	AWSSecretAccessKey string           `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key,description=AWS Secret Access Key for reading and writing data to the S3 staging bucket." jsonschema_extras:"secret=true,order=7"`
	Region             string           `json:"region" jsonschema:"title=Region,description=Region of the S3 staging bucket. For optimal performance this should be in the same region as the Redshift database cluster." jsonschema_extras:"order=8"`
	BucketPath         string           `json:"bucketPath,omitempty" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects in S3." jsonschema_extras:"order=9"`
	HardDelete         bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=10,nonsensitive=true"`
	Schedule           m.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger      dbt.JobConfig    `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`
	NetworkTunnel      *tunnelConfig    `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your Redshift cluster through an SSH server that acts as a bastion host for your network."`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

func (c config) Validate() error {
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

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	if err := blob.ValidateBucketPath(c.effectiveBucketPath()); err != nil {
		return fmt.Errorf("bucketPath %w", err)
	}

	return nil
}

func (c config) effectiveBucketPath() string {
	// If BucketPath starts with a / trim the leading / so that we don't end up with repeated /
	// chars in the URI and so that the object key does not start with a /.
	return strings.TrimPrefix(c.BucketPath, "/")
}

func (c config) DefaultNamespace() string {
	return c.Schema
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) networkTunnelEnabled() bool {
	return c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != ""
}

func (c config) toURI() string {
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

func (c config) toS3Client(ctx context.Context, featureFlags map[string]bool) (*s3.Client, error) {
	opts := []func(*awsConfig.LoadOptions) error{
		awsConfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(c.AWSAccessKeyID, c.AWSSecretAccessKey, ""),
		),
		awsConfig.WithRegion(c.Region),
		awsConfig.WithRetryMaxAttempts(7),
	}
	if featureFlags["s3_use_dualstack_endpoints"] {
		opts = append(opts, awsConfig.WithUseDualStackEndpoint(aws.DualStackEndpointStateEnabled))
	}

	awsCfg, err := awsConfig.LoadDefaultConfig(ctx, opts...)

	if err != nil {
		return nil, err
	}

	return s3.NewFromConfig(awsCfg), nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional)." jsonschema_extras:"x-schema-name=true"`
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	if c.Schema == "" {
		c.Schema = cfg.Schema
	}
	return c
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) Parameters() ([]string, bool, error) {
	var path []string
	if c.Schema != "" {
		path = []string{c.Schema, c.Table}
	} else {
		path = []string{c.Table}
	}
	return path, c.Delta, nil
}

func NewDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-redshift",
		StartTunnel: func(ctx context.Context, cfg config) error {
			// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
			if cfg.networkTunnelEnabled() {
				host, port, err := net.SplitHostPort(cfg.Address)
				if err != nil {
					return fmt.Errorf("splitting address to host and port: %w", err)
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
					return err
				}
			}

			return nil
		},
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.User,
			}).Info("opening database")

			var metaBase sql.TablePath
			if cfg.Schema != "" {
				metaBase = append(metaBase, cfg.Schema)
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
			var dialect = createRsDialect(caseSensitiveIdentifierEnabled, featureFlags)
			var templates = renderTemplates(dialect)

			if caseSensitiveIdentifierEnabled {
				log.WithField("caseSensitiveIdentifierEnabled", caseSensitiveIdentifierEnabled).Info("detected value for enable_case_sensitive_identifier")
			}

			if cfg.Advanced.FeatureFlags != "" {
				log.WithField("flags", featureFlags).Info("parsed feature flags")
			}

			return &sql.Endpoint[config]{
				Config:    cfg,
				Dialect:   dialect,
				SerPolicy: boilerplate.SerPolicyStd,
				// Creates the row that holds applied-transaction tokens; the
				// fence itself is unused.
				MetaCheckpoints:     sql.FlowCheckpointsTable(metaBase),
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       prepareNewTransactor(templates, caseSensitiveIdentifierEnabled),
				ConcurrentApply:     true,
				NoFlowDocument:      cfg.Advanced.NoFlowDocument,
				Options: m.MaterializeOptions{
					ExtendedLogging: true,
					AckSchedule: &m.AckScheduleOption{
						Config: cfg.Schedule,
						Jitter: []byte(cfg.Address),
					},
					DBTJobTrigger: &cfg.DBTJobTrigger,
				},
			}, nil
		},
		PreReqs: preReqs,
	}
}

type transactor struct {
	templates                      templates
	bindings                       []*binding
	be                             *m.BindingEvents
	cfg                            config
	store                          *s3Store
	caseSensitiveIdentifierEnabled bool

	rangeKey string
	// The shard whose key range begins at 0 applies every shard's staged
	// work; the others only stage.
	primary     bool
	checkpoints *checkpointsTable
	// IDs of every checkpointItem already committed, across the materialization.
	committedTokens map[string]bool
	// Staged transactions not yet applied, from every shard.
	pending pendingState
	// Set only for a task crossing over from the old format, whose row still
	// holds the runtime checkpoint and whose state has nothing staged.
	runtimeCheckpoint m.RuntimeCheckpoint
}

var _ m.Transactor = (*transactor)(nil)

func (d *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return d.runtimeCheckpoint, nil
}

func prepareNewTransactor(
	templates templates,
	caseSensitiveIdentifierEnabled bool,
) func(context.Context, string, map[string]bool, *sql.Endpoint[config], sql.Fence, []sql.Table, pm.Request_Open, *boilerplate.InfoSchema, *m.BindingEvents) (m.Transactor, error) {
	return func(
		ctx context.Context,
		materializationName string,
		featureFlags map[string]bool,
		ep *sql.Endpoint[config],
		fence sql.Fence,
		bindings []sql.Table,
		open pm.Request_Open,
		is *boilerplate.InfoSchema,
		be *m.BindingEvents,
	) (m.Transactor, error) {
		var cfg = ep.Config

		var d = &transactor{
			templates:                      templates,
			cfg:                            cfg,
			be:                             be,
			caseSensitiveIdentifierEnabled: caseSensitiveIdentifierEnabled,
			rangeKey:                       rangeKeyOf(fence.KeyBegin, fence.KeyEnd),
			primary:                        fence.KeyBegin == 0,
			checkpoints: &checkpointsTable{
				table:           ep.Dialect.Identifier(fence.TablePath...),
				materialization: fence.Materialization.String(),
				keyBegin:        fence.KeyBegin,
				keyEnd:          fence.KeyEnd,
			},
			committedTokens: make(map[string]bool),
			pending:         make(pendingState),
		}

		client, err := d.cfg.toS3Client(ctx, featureFlags)
		if err != nil {
			return nil, err
		}
		d.store = newS3Store(client, d.cfg.Bucket)

		for _, target := range bindings {
			if err = d.addBinding(target, is); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", target.Path, err)
			}
		}

		// The crossover decision below needs the state before the runtime
		// hands it over separately.
		state, err := parseState(open.StateJson)
		if err != nil {
			return nil, err
		}

		conn, err := pgx.Connect(ctx, d.cfg.toURI())
		if err != nil {
			return nil, fmt.Errorf("open pgx.Connect: %w", err)
		}
		defer conn.Close(ctx)

		var legacyCheckpoint []byte
		if d.committedTokens, legacyCheckpoint, err = d.checkpoints.readAll(ctx, conn); err != nil {
			return nil, err
		}
		if d.primary {
			d.pending = state
		}

		// A shard's row is authoritative only until it has staged a
		// transaction: after that the recovery log is, and returning the
		// row's checkpoint would rewind the runtime and stage that
		// transaction twice.
		if legacyCheckpoint != nil && !state.hasRange(d.rangeKey) {
			d.runtimeCheckpoint = legacyCheckpoint
		}

		return d, nil
	}
}

type binding struct {
	target                    sql.Table
	nullFieldsToStrip         []string
	varcharColumnMetas        []VarcharColumnMeta
	loadFile                  *stagedFile
	storeFile                 *stagedFile
	deleteFile                *stagedFile
	createLoadTableTemplate   *template.Template
	createStoreTableTemplate  *template.Template
	createDeleteTableTemplate *template.Template
	mergeIntoSQL              string
	deleteQuerySQL            string
	loadQuerySQL              string

	// The transaction being staged, if any.
	staged *checkpointItem
}

// VarcharColumnMeta contains metadata about Redshift varchar columns. Currently this is just the
// maximum length of the field as reported from the database, populated upon connector startup.
type VarcharColumnMeta struct {
	Identifier string
	MaxLength  int
	IsVarchar  bool
}

func (t *transactor) addBinding(target sql.Table, is *boilerplate.InfoSchema) error {
	var b = &binding{
		target:     target,
		loadFile:   newStagedFile(t.store, t.cfg.effectiveBucketPath(), target.KeyNames()),
		storeFile:  newStagedFile(t.store, t.cfg.effectiveBucketPath(), target.ColumnNames()),
		deleteFile: newStagedFile(t.store, t.cfg.effectiveBucketPath(), target.KeyNames()),
	}

	if t.cfg.Advanced.NoFlowDocument {
		b.nullFieldsToStrip = target.NullableFieldsToStrip()
	}

	// Choose appropriate templates based on configuration
	var loadQueryTemplate = t.templates.loadQuery
	if t.cfg.Advanced.NoFlowDocument {
		loadQueryTemplate = t.templates.loadQueryNoFlowDocument
	}

	// Render templates that rely only on the target table.
	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.mergeIntoSQL, t.templates.mergeInto},
		{&b.deleteQuerySQL, t.templates.deleteQuery},
		{&b.loadQuerySQL, loadQueryTemplate},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	// The load/store/delete tables template are re-evaluated every transaction
	// to account for the specific string lengths observed for string keys.
	b.createLoadTableTemplate = t.templates.createLoadTable
	b.createStoreTableTemplate = t.templates.createStoreTable
	b.createDeleteTableTemplate = t.templates.createDeleteTable

	// Retain column metadata information for this binding as a snapshot of the target table
	// configuration when the connector started, indexed in the same order as values will be
	// received from the runtime for Store requests. Only VARCHAR columns will have non-zero-valued
	// varcharColumnMeta.
	allColumns := target.Columns()
	columnMetas := make([]VarcharColumnMeta, len(allColumns))
	for idx, col := range allColumns {
		existing := is.GetResource(target.Path).GetField(col.Field)
		if existing.Type == "character varying" {
			columnMetas[idx] = VarcharColumnMeta{
				Identifier: col.Identifier,
				MaxLength:  existing.CharacterMaxLength,
				IsVarchar:  true,
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

// copyFromS3 renders the COPY of a staged manifest into target.
func (t *transactor) copyFromS3(target string, manifestKey string, truncateColumns bool) (string, error) {
	var sql strings.Builder
	if err := t.templates.copyFromS3.Execute(&sql, copyFromS3Params{
		Target:                         target,
		ManifestURL:                    t.store.objectURI(manifestKey),
		Config:                         t.cfg,
		CaseSensitiveIdentifierEnabled: t.caseSensitiveIdentifierEnabled,
		TruncateColumns:                truncateColumns,
	}); err != nil {
		return "", fmt.Errorf("rendering COPY for %s: %w", target, err)
	}
	return sql.String(), nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if !t.primary {
		return nil
	}
	pending, err := parseState(state)
	if err != nil {
		return err
	}
	t.pending = pending
	return nil
}

// mergePeerStatePatches folds every shard's StartedCommit patches of the
// just-committed transaction, including the primary's own, into the
// primary's pending set.
func (t *transactor) mergePeerStatePatches(patches []json.RawMessage) error {
	if !t.primary {
		return nil
	}
	for _, patch := range patches {
		if bytes.Equal(bytes.TrimSpace(patch), []byte("null")) {
			// The runtime's encoding of a peer's full (non-merge-patch) state
			// replacement, which no shard here ever produces.
			return fmt.Errorf("unexpected state reset patch in aggregated shard state")
		}
		state, err := parseState(patch)
		if err != nil {
			return fmt.Errorf("parsing aggregated state patch: %w", err)
		}
		for sk, bucket := range state {
			for rk, e := range bucket {
				// The previous transaction's entry is applied before the next
				// one's patch arrives, so a collision would lose staged work.
				if t.pending[sk][rk] != nil {
					return fmt.Errorf("shard %s staged a new transaction for %s while its previous one is still pending", rk, sk)
				}
				t.pending.add(sk, rk, e)
			}
		}
	}
	return nil
}

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()
	gotLoads := false

	// Keep track of the maximum length of any string values encountered so the temporary load table
	// can be created with long enough string columns.
	maxStringLengths := make([]int, len(d.bindings))

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

		if err := b.loadFile.writeRow(ctx, converted); err != nil {
			return fmt.Errorf("writing row for load: %w", err)
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

		files, err := b.loadFile.flush()
		if err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}
		manifest, err := d.putManifest(ctx, files)
		if err != nil {
			return err
		}
		var objects = append(slices.Clone(files), manifest)
		defer d.store.deleteObjects(ctx, objects)

		if copySQL, err := d.copyFromS3(fmt.Sprintf("flow_temp_table_%d", b.target.Binding), manifest, false); err != nil {
			return err
		} else if _, err := txn.Exec(ctx, copySQL); err != nil {
			return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, files, b.target.Identifier, err)
		}
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	loadAllSQL := strings.Join(subqueries, "\nUNION ALL\n") + ";"
	d.be.StartedEvaluatingLoads()
	rows, err := txn.Query(ctx, loadAllSQL)
	if err != nil {
		return fmt.Errorf("querying load documents: %w", err)
	}
	defer rows.Close()
	d.be.FinishedEvaluatingLoads()

	for rows.Next() {
		var binding int
		var document json.RawMessage

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning load document: %w", err)
		}

		doc := json.RawMessage(document)
		if b := d.bindings[binding]; len(b.nullFieldsToStrip) > 0 {
			if doc, err = sql.StripNullFields(doc, b.nullFieldsToStrip); err != nil {
				return fmt.Errorf("stripping null fields: %w", err)
			}
		}
		if err = loaded(binding, doc); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("committing load transaction: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	flushStagedFile := func(ctx context.Context, b *binding) error {
		var err error
		if b.storeFile.started {
			if b.staged.StoreFiles, err = b.storeFile.flush(); err != nil {
				return fmt.Errorf("flushing store file: %w", err)
			}
		}
		if b.deleteFile.started {
			if b.staged.DeleteFiles, err = b.deleteFile.flush(); err != nil {
				return fmt.Errorf("flushing delete file: %w", err)
			}
		}
		return nil
	}

	var lastBinding = -1
	// Skip deleted, non-existent documents iff HardDelete is enabled.
	for it.Next(d.cfg.HardDelete) {
		if lastBinding == -1 {
			lastBinding = it.Binding
		}

		if lastBinding != it.Binding {
			// Flush the staged file(s) for the binding now that it's stores are
			// fully processed.
			var b = d.bindings[lastBinding]
			if err := flushStagedFile(ctx, b); err != nil {
				return nil, fmt.Errorf("flushing staged files for collection %q: %w", b.target.Source.String(), err)
			}
			lastBinding = it.Binding
		}

		var b = d.bindings[it.Binding]
		if b.staged == nil {
			b.staged = &checkpointItem{ID: uuid.NewString()}
		}
		var file *stagedFile

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
			if it.Exists {
				b.staged.MustMerge = true
			}

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
			if varcharMeta.IsVarchar {
				switch v := c.(type) {
				case string:
					// If the column is already at its maximum length, it can't be enlarged any
					// more. The string values will be truncated by Redshift when loading them into
					// the table.
					if len(v) > varcharMeta.MaxLength && varcharMeta.MaxLength != redshiftVarcharMaxLength {
						log.WithFields(log.Fields{
							"table":               b.target.Identifier,
							"column":              varcharMeta.Identifier,
							"currentColumnLength": varcharMeta.MaxLength,
							"stringValueLength":   len(v),
						}).Info("column will be altered to VARCHAR(MAX) to accommodate large string value")
						b.staged.Widen = append(b.staged.Widen, varcharMeta.Identifier)
						b.varcharColumnMetas[idx].MaxLength = redshiftVarcharMaxLength // Do not need to alter this column again.
					}
				case nil:
					// Values for string fields may be null, in which case there is nothing to do.
					continue
				default:
					// Invariant: This value must either be a string or nil, since the column it is
					// going into is a VARCHAR.
					return nil, fmt.Errorf("expected type string or nil for column %s of table %s, got %T", varcharMeta.Identifier, b.target.Identifier, c)
				}
			}
		}

		if err := file.writeRow(ctx, converted); err != nil {
			return nil, fmt.Errorf("writing row for store: %w", err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	// Flush the final binding.
	if lastBinding != -1 {
		var b = d.bindings[lastBinding]
		if err := flushStagedFile(ctx, b); err != nil {
			return nil, fmt.Errorf("final binding flushing staged files for collection %q: %w", b.target.Source.String(), err)
		}
	}

	return func(ctx context.Context, _ *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		// Only this transaction's entries: an entry whose state key no longer
		// has a binding stays pending forever and must not be re-sent. The
		// primary picks its own entries back up from Acknowledge's statePatches,
		// same as its peers'.
		var update = make(pendingState)
		for _, b := range d.bindings {
			if b.staged == nil {
				continue
			}
			update.add(b.target.StateKey, d.rangeKey, b.staged)
			b.staged = nil
		}

		patch, err := json.Marshal(update)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling staged transactions: %w", err))
		}
		return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
	}, nil
}

func (d *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	if err := d.mergePeerStatePatches(statePatches); err != nil {
		return nil, err
	}
	if !d.primary {
		return nil, nil
	}

	// Entries of a binding with no live table are left pending.
	var shouldProcess = m.StateKeyFilter(stateKeys)
	var pending = make(pendingState)
	for _, b := range d.bindings {
		var sk = b.target.StateKey
		if !shouldProcess(sk) {
			continue
		}
		for rk, e := range d.pending[sk] {
			pending.add(sk, rk, e)
		}
	}
	if len(pending) == 0 {
		return nil, nil
	}

	if err := d.commit(ctx, pending); err != nil {
		return nil, err
	}

	// Clearing an entry from the state is a null at its place.
	var clear = make(pendingState)
	for sk, bucket := range pending {
		for rk, e := range bucket {
			d.store.deleteObjects(ctx, e.objects())
			delete(d.pending[sk], rk)
			clear.add(sk, rk, nil)
		}
		if len(d.pending[sk]) == 0 {
			delete(d.pending, sk)
		}
	}

	patch, err := json.Marshal(clear)
	if err != nil {
		return nil, fmt.Errorf("marshalling state clearing patch: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: patch, MergePatch: true}, nil
}

// commit runs the staged transactions of pending as one Redshift
// transaction, skipping any whose token is already committed.
func (d *transactor) commit(ctx context.Context, pending pendingState) error {
	// group is one binding's pending transactions from every shard, merged
	// into one, with manifests written for the commit.
	type group struct {
		binding                       *binding
		merged                        checkpointItem
		storeManifest, deleteManifest string
	}
	var groups []*group
	var cp = make(checkpoints)
	for _, b := range d.bindings {
		var bucket = pending[b.target.StateKey]
		if len(bucket) == 0 {
			continue
		}
		var g = &group{binding: b}
		for rk, e := range bucket {
			if d.committedTokens[e.ID] {
				log.WithFields(log.Fields{
					"table": b.target.Identifier,
					"range": rk,
					"id":    e.ID,
				}).Info("skipping staged transaction that was already applied")
				continue
			}
			g.merged.StoreFiles = append(g.merged.StoreFiles, e.StoreFiles...)
			g.merged.DeleteFiles = append(g.merged.DeleteFiles, e.DeleteFiles...)
			g.merged.MustMerge = g.merged.MustMerge || e.MustMerge
			for _, column := range e.Widen {
				if !slices.Contains(g.merged.Widen, column) {
					g.merged.Widen = append(g.merged.Widen, column)
				}
			}
			cp.add(b.target.StateKey, rk, e.ID)
		}
		if len(g.merged.StoreFiles) > 0 || len(g.merged.DeleteFiles) > 0 {
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		return nil
	}

	var manifests []string
	defer func() { d.store.deleteObjects(ctx, manifests) }()
	for _, g := range groups {
		var err error
		if g.storeManifest, err = d.putManifest(ctx, g.merged.StoreFiles); err != nil {
			return err
		} else if g.storeManifest != "" {
			manifests = append(manifests, g.storeManifest)
		}
		if g.deleteManifest, err = d.putManifest(ctx, g.merged.DeleteFiles); err != nil {
			return err
		} else if g.deleteManifest != "" {
			manifests = append(manifests, g.deleteManifest)
		}
	}

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
	for _, g := range groups {
		var b = g.binding
		for _, column := range g.merged.Widen {
			if _, err := conn.Exec(ctx, fmt.Sprintf(varcharTableAlter, b.target.Identifier, column)); err != nil {
				// It is possible that another shard of this materialization will have already
				// updated this column. Practically this means we will try to set a column that is
				// already VARCHAR(MAX) to VARCHAR(MAX), and Redshift returns a specific error in
				// this case that can be safely discarded.
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) {
					if pgErr.Code == "0A000" && strings.Contains(pgErr.Message, "target column size should be different") {
						log.WithFields(log.Fields{
							"table":  b.target.Identifier,
							"column": column,
						}).Info("attempted to alter column VARCHAR(MAX) but it was already VARCHAR(MAX)")
						continue
					}
				}

				return fmt.Errorf("altering size for column %s of table %s: %w", column, b.target.Identifier, err)
			}

			log.WithFields(log.Fields{
				"table":  b.target.Identifier,
				"column": column,
			}).Info("column altered to VARCHAR(MAX) to accommodate large string value")
		}
		// A recovered or peer-staged entry may widen a column this shard's
		// metadata still has at its original length.
		for idx := range b.varcharColumnMetas {
			if slices.Contains(g.merged.Widen, b.varcharColumnMetas[idx].Identifier) {
				b.varcharColumnMetas[idx].MaxLength = redshiftVarcharMaxLength
			}
		}
	}

	txn, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("store BeginTx: %w", err)
	}
	defer txn.Rollback(ctx)

	for _, g := range groups {
		var b = g.binding
		d.be.StartedResourceCommit(b.target.Path)

		if g.deleteManifest != "" {
			// Create the temporary table for staging values to delete from the target table.
			// Redshift actually supports transactional DDL for creating tables, so this can be
			// executed within the transaction.
			var createDeleteTableSQL strings.Builder
			if err := b.createDeleteTableTemplate.Execute(&createDeleteTableSQL, deleteTableParams{
				Target:            &b.target,
				VarcharColumnMeta: b.varcharColumnMetas,
			}); err != nil {
				return fmt.Errorf("evaluating create delete table template: %w", err)
			} else if _, err := txn.Exec(ctx, createDeleteTableSQL.String()); err != nil {
				return fmt.Errorf("creating delete table: %w", err)
			}

			if copySQL, err := d.copyFromS3(fmt.Sprintf("flow_temp_table_%d_deleted", b.target.Binding), g.deleteManifest, false); err != nil {
				return err
			} else if _, err := txn.Exec(ctx, copySQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, g.merged.DeleteFiles, b.target.Identifier, err)
			} else if _, err := txn.Exec(ctx, b.deleteQuerySQL); err != nil {
				return fmt.Errorf("deleting from table '%s': %w", b.target.Identifier, err)
			}
		}

		if g.storeManifest == "" {
			// Pass.
		} else if g.merged.MustMerge {
			// Create the temporary table for staging values to merge into the target table.
			// Redshift actually supports transactional DDL for creating tables, so this can be
			// executed within the transaction.
			var createStoreTableSQL strings.Builder
			if err := b.createStoreTableTemplate.Execute(&createStoreTableSQL, storeTableParams{
				Target:            &b.target,
				VarcharColumnMeta: b.varcharColumnMetas,
			}); err != nil {
				return fmt.Errorf("evaluating create store table template: %w", err)
			} else if _, err := txn.Exec(ctx, createStoreTableSQL.String()); err != nil {
				return fmt.Errorf("creating store table: %w", err)
			}

			if copySQL, err := d.copyFromS3(fmt.Sprintf("flow_temp_table_%d", b.target.Binding), g.storeManifest, true); err != nil {
				return err
			} else if _, err := txn.Exec(ctx, copySQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, g.merged.StoreFiles, b.target.Identifier, err)
			} else if _, err := txn.Exec(ctx, b.mergeIntoSQL); err != nil {
				return fmt.Errorf("merging to table '%s': %w", b.target.Identifier, err)
			}
		} else {
			// Can copy directly into the target table since all values are new.
			if copySQL, err := d.copyFromS3(b.target.Identifier, g.storeManifest, true); err != nil {
				return err
			} else if _, err := txn.Exec(ctx, copySQL); err != nil {
				return handleCopyIntoErr(ctx, txn, d.cfg.Bucket, g.merged.StoreFiles, b.target.Identifier, err)
			}
		}

		d.be.FinishedResourceCommit(b.target.Path)
	}

	// Written in the same transaction as the data: a committed token proves
	// the data committed, and a rolled-back commit leaves no token.
	if err := d.checkpoints.write(ctx, txn, cp); err != nil {
		return err
	} else if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("committing store transaction: %w", err)
	}
	for _, bucket := range cp {
		for _, token := range bucket {
			d.committedTokens[token] = true
		}
	}

	return nil
}

// putManifest writes a manifest listing files under a fresh key, or returns
// "" when there are none.
func (d *transactor) putManifest(ctx context.Context, files []string) (string, error) {
	if len(files) == 0 {
		return "", nil
	}
	var key = path.Join(d.cfg.effectiveBucketPath(), uuid.NewString(), manifestFile)
	if err := d.store.putManifest(ctx, key, files); err != nil {
		return "", err
	}
	return key, nil
}

// handleCopyIntoErr queries the `sys_load_error_detail` table for relevant COPY INTO error details
// and returns a more useful error than the opaque error returned by Redshift. This function will
// always return an error. `sys_load_error_detail` is queried instead of `stl_load_errors` since it
// is available to both serverless and provisioned versions of Redshift, whereas `stl_load_errors`
// is only available on provisioned Redshift.
func handleCopyIntoErr(ctx context.Context, txn pgx.Tx, bucket string, files []string, table string, copyIntoErr error) error {
	// The transaction has failed. It must be finish being rolled back before using its underlying
	// connection again.
	txn.Rollback(ctx)
	conn := txn.Conn()

	loadErrInfo, err := getLoadErrorInfo(ctx, conn, bucket, files)
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
