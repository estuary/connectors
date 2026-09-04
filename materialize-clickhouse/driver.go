package connector

import (
	"cmp"
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

// connectorVersion is the connector's release tag, embedded so the client
// identification reported to ClickHouse stays in sync with the image version.
//
//go:embed VERSION
var connectorVersion string

type authType string

const (
	UserPass authType = "user_password"
)

type usernamePassword struct {
	Username string `json:"username,omitempty" jsonschema:"title=Username,description=Username for authentication." jsonschema_extras:"order=0"`
	Password string `json:"password,omitempty" jsonschema:"title=Password,description=Password for authentication." jsonschema_extras:"order=1,secret=true"`
}

type credentialConfig struct {
	AuthType authType `json:"auth_type"`

	usernamePassword
}

func (c credentialConfig) Validate() error {
	switch c.AuthType {
	case UserPass:
		if c.Username == "" {
			return fmt.Errorf("missing username")
		}
		if c.Password == "" {
			return fmt.Errorf("missing password")
		}
		return nil
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (credentialConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(UserPass),
		schemagen.OneOfSubSchema("Username and Password", usernamePassword{}, string(UserPass)),
	)
}

type config struct {
	Address     string           `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Default is 9000 if SSL is disabled\\, 9440 if SSL is enabled." jsonschema_extras:"order=0"`
	Database    string           `json:"database" jsonschema:"title=Database,description=Name of the ClickHouse database to materialize to." jsonschema_extras:"order=1"`
	HardDelete  bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default this is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=2,nonsensitive=true"`
	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=3"`
	Schedule    m.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	SSLMode        string `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Controls the TLS connection behavior.,enum=disable,enum=require,enum=verify-full,default=verify-full"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false" jsonschema_extras:"nonsensitive=true"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if err := c.Credentials.Validate(); err != nil {
		return err
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	switch c.Advanced.SSLMode {
	case "", "disable", "require", "verify-full":
	default:
		return fmt.Errorf("invalid sslmode %q (expected disable, require, or verify-full)", c.Advanced.SSLMode)
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return ""
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, nil
}

func (c config) resolvedAddress() string {
	var address = c.Address
	if !strings.Contains(address, ":") {
		if c.Advanced.SSLMode == "disable" {
			address = address + ":9000"
		} else {
			address = address + ":9440"
		}
	}
	return address
}

func (c config) newClickhouseOptions() *clickhouse.Options {
	var tlsConfig *tls.Config
	switch c.Advanced.SSLMode {
	case "verify-full", "":
		tlsConfig = &tls.Config{InsecureSkipVerify: false}
	case "require":
		tlsConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &clickhouse.Options{
		Addr: []string{c.resolvedAddress()},
		// Identify Estuary in the client name reported to ClickHouse
		// (system.query_log, system.processes, etc.), which otherwise only
		// shows the generic clickhouse-go driver.
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct{ Name, Version string }{
				{Name: "Estuary", Version: strings.TrimSpace(connectorVersion)},
			},
			Comment: []string{"materialize-clickhouse"},
		},
		TLS: tlsConfig,
		Auth: clickhouse.Auth{
			Database: c.Database,
			Username: c.Credentials.Username,
			Password: c.Credentials.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}
}

// tableConfig defines per-binding resource configuration.
type tableConfig struct {
	Table string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Delta bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`
	// PartitionBy is spliced verbatim into the table's PARTITION BY clause.
	// It runs as DDL with the user's own credentials against their own
	// database, so it is the same trust plane as the rest of the endpoint
	// config; malformed input fails the dry-run CREATE TABLE at Validate time.
	PartitionBy string `json:"partition_by,omitempty" jsonschema:"title=Partition By,description=Optional expression to use as the table's PARTITION BY clause\\, for example toYYYYMM(flow_published_at). Leave blank for ClickHouse's default single partition. Use a low-cardinality expression: ClickHouse recommends well under 1000 total partitions\\, and inserts spanning more than 100 partitions are rejected by default. Changing this value requires backfilling the binding\\, which drops and re-creates the table." jsonschema_extras:"advanced=true,nonsensitive=true"`
}

func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (r tableConfig) WithDefaults(_ config) tableConfig { return r }

func (r tableConfig) Parameters() ([]string, bool, error) {
	return []string{r.Table}, r.Delta, nil
}

// driver wraps the generic SQL driver to customize Validate: a changed
// partition_by requires re-creating the table, and partition expressions are
// verified with a dry-run CREATE TABLE. See validate.go.
type driver struct {
	sqlDriver *sql.Driver[config, tableConfig]
}

var _ boilerplate.Connector = &driver{}

func NewDriver() *driver {
	sqlDriver := &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-clickhouse",
		StartTunnel: func(ctx context.Context, cfg config) error {
			return nil
		},
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"database": cfg.Database,
				"address":  cfg.Address,
				"user":     cfg.Credentials.Username,
			}).Info("opening database")

			var dialect = clickHouseDialect(cfg.Database)
			var tpls = renderTemplates(dialect, cfg.HardDelete)

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				NewClient:           newClient,
				CreateTableTemplate: tpls.createTargetTable,
				NewTransactor:       newTransactor,
				ConcurrentApply:     false,
				NoFlowDocument:      cfg.Advanced.NoFlowDocument,
				Options: m.MaterializeOptions{
					ExtendedLogging: true,
					AckSchedule: &m.AckScheduleOption{
						Config: cfg.Schedule,
						Jitter: []byte(cfg.Address + cfg.Database),
					},
				},
			}, nil
		},
		PreReqs: preReqs,
	}
	return &driver{
		sqlDriver: sqlDriver,
	}
}

// NewMaterializer opens a Materializer for the endpoint. ClickHouse wraps the
// standard SQL driver to add its own validation, so this reaches through to the
// wrapped driver's entry point, which is what establishes the initialization
// order the driver's operations assume.
func NewMaterializer(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, sql.FieldConfig, tableConfig, sql.MappedType], error) {
	return NewDriver().sqlDriver.NewMaterializer(ctx, materializationName, cfg, featureFlags)
}

func (d *driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	return d.sqlDriver.Spec(ctx, req)
}

func (d *driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return d.sqlDriver.Apply(ctx, req)
}

func (d *driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return d.sqlDriver.NewTransactor(ctx, req, be)
}

type transactor struct {
	cfg       config
	templates templates
	dialect   sql.Dialect
	load      struct {
		conn chdriver.Conn
	}
	store struct {
		conn chdriver.Conn
	}
	bindings []*binding
	be       *m.BindingEvents
	_range   *pf.RangeSpec

	// recovery is set when the Open request carried prior connector state,
	// meaning this session may be re-applying commits begun by a previous
	// process. Pending commits recovered under this flag skip the pre-move
	// row accounting: a crash mid-move legitimately leaves fewer staged rows
	// than were stored, and an empty stage table means the commit had fully
	// completed. It is cleared after the first Acknowledge.
	recovery bool
	// ensured is closed once ensureTempTables is called to run the
	// ensure pass: recovering any pending commits and creating / truncating /
	// re-creating the persistent temp tables for every binding. It is
	// deliberately independent of the recovery flag, which is only set when
	// the Open request carried prior connector state -- a brand-new task has
	// no state but still needs its temp tables ensured before the first
	// transaction.  A channel is used so that Load can wait for it before
	// reading any Load requests.
	ensured           chan struct{}
	state             connectorState
	runtimeCheckpoint m.RuntimeCheckpoint
}

func (t *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return t.runtimeCheckpoint, nil
}

// stagedPartition is one partition of a store table, with the row count observed
// in it when the transaction's staged rows were verified. Both fields are data
// read from the server, so they carry the same meaning to every connector version
// that recovers them.
type stagedPartition struct {
	ID   string `json:"id"`
	Rows int64  `json:"rows"`
}

// stateItem records that a binding has a committed-but-not-yet-acknowledged
// transaction whose rows are staged in its store table. The SQL needed to
// commit those rows is always rendered freshly from the live binding -- never
// persisted -- so that state written by one version of the connector is safe
// to recover with another.
type stateItem struct {
	// StoredRows is the number of rows inserted into the stage table during
	// the transaction. A value of 0 means the count is unknown, from a
	// checkpoint written by a connector version that did not record it.
	StoredRows int64
	// Verified is set once an authoritative (select_sequential_consistency)
	// count of the stage table has been reconciled against StoredRows, which
	// happens before the checkpoint carrying this state is durable. State
	// without it was written by a connector version predating that check.
	Verified bool
	// MovePartitions is the set of partitions the commit must move, snapshotted
	// when the staged rows were verified. The per-partition counts are what let
	// recovery distinguish a partition a prior process already moved (absent
	// from the stage table entirely) from rows that were never staged (present,
	// but short) -- a distinction a single total cannot make.
	MovePartitions []stagedPartition
}

type connectorState map[string]*stateItem

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &t.state); err != nil {
		return fmt.Errorf("unmarshalling connector state: %w", err)
	}
	t.recovery = true
	return nil
}

func newTransactor(
	ctx context.Context,
	materializationName string,
	featureFlags map[string]bool,
	ep *sql.Endpoint[config],
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	_ *boilerplate.InfoSchema,
	be *m.BindingEvents,
) (m.Transactor, error) {
	var cfg = ep.Config
	t := &transactor{
		dialect:           ep.Dialect,
		templates:         renderTemplates(ep.Dialect, cfg.HardDelete),
		cfg:               cfg,
		be:                be,
		_range:            open.Range,
		ensured:           make(chan struct{}),
		state:             make(connectorState, len(bindings)),
		runtimeCheckpoint: fence.Checkpoint,
	}

	var err error
	loadOptions := cfg.newClickhouseOptions()
	if t.load.conn, err = clickhouse.Open(loadOptions); err != nil {
		return nil, fmt.Errorf("openNativeConn (load): %w", err)
	}

	storeOptions := cfg.newClickhouseOptions()
	if t.store.conn, err = clickhouse.Open(storeOptions); err != nil {
		return nil, fmt.Errorf("openNativeConn (store): %w", err)
	}

	for _, target := range bindings {
		if err = t.addBinding(ctx, target); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", target.Path, err)
		}
	}

	if err = t.requireSortingKeysMatchBindings(ctx); err != nil {
		return nil, err
	}

	if cfg.HardDelete {
		for _, b := range t.bindings {
			if b.target.DeltaUpdates {
				continue
			}
			if err = t.requireIsDeletedColumn(ctx, b); err != nil {
				return nil, err
			}
		}
	}

	return t, nil
}

// requireSortingKeysMatchBindings fails with a clear, actionable error when a
// target table's sorting key does not cover its binding's key fields. Such a
// table cannot be written to without destroying rows -- see checkSortingKey --
// and the damage is silent, so this refuses at session start rather than letting
// a running task shrink its own data.
//
// The check runs regardless of the allow_existing_tables_for_new_bindings feature
// flag that most often produces such a table: the flag waives whether a table may
// already exist, not whether it is usable. A table can also reach this state from
// a restore, a hand edit, or another tool writing at the same name.
func (t *transactor) requireSortingKeysMatchBindings(ctx context.Context) error {
	rows, err := t.store.conn.Query(ctx, fmt.Sprintf(tableKeyMetaSQL, "currentDatabase()"))
	if err != nil {
		return fmt.Errorf("querying table sorting keys: %w", err)
	}
	metas, err := scanTableKeyMeta(rows)
	if err != nil {
		return err
	}

	for _, b := range t.bindings {
		var meta, exists = metas[b.target.Path[0]]
		var keys = b.target.KeyNames()
		warning, err := checkSortingKey(b.target.Identifier, keys, meta, exists)
		var ll = log.WithFields(log.Fields{
			"table":           b.target.Identifier,
			"want_keys":       strings.Join(keys, ", "),
			"got_sorting_key": meta.sortingKey,
			"engine":          meta.engine,
		})
		if err != nil {
			ll.Error("target table sorting key does not match the binding's key fields")
			return err
		} else if warning != "" {
			ll.Warn(warning)
		}
	}
	return nil
}

// requireIsDeletedColumn fails with a clear, actionable error when a
// standard-updates target table exists but lacks the connector-internal
// _is_deleted column that hard-delete mode depends on.
//
// With hard delete enabled, renderTemplates bakes an `INSERT ... (_is_deleted)`
// into every standard-updates store, and the persistent stage table is created
// `AS` the target -- so a target without the column makes every Store fail with a
// cryptic ClickHouse "No such column _is_deleted in table ..." (code 16), which
// crash-loops the task and blocks all of its bindings (issue #4834). Because
// _is_deleted is purely internal (never a Flow projection), the Apply diffing can
// never add it, so a table adopted via allow_existing_tables_for_new_bindings (or
// created before hard delete was enabled) can never acquire it on its own.
// Surface the problem plainly at session start rather than as that exception, and
// deliberately do not attempt to add the column: an in-place ALTER cannot restore
// the ReplacingMergeTree engine's _is_deleted cleanup argument, so the table must
// be recreated by the connector to get correct hard-delete semantics.
func (t *transactor) requireIsDeletedColumn(ctx context.Context, b *binding) error {
	var total, hasIsDeleted uint64
	if err := t.store.conn.QueryRow(ctx,
		"SELECT count(), countIf(name = '_is_deleted') FROM system.columns WHERE database = currentDatabase() AND table = ?",
		b.target.Path[0],
	).Scan(&total, &hasIsDeleted); err != nil {
		return fmt.Errorf("checking for _is_deleted column in %s: %w", b.target.Identifier, err)
	}

	// total == 0: the table does not exist yet, so Apply will create it with the
	// column. hasIsDeleted > 0: the column is already present.
	if total == 0 || hasIsDeleted > 0 {
		return nil
	}

	return fmt.Errorf(
		"table %s is missing the required _is_deleted column. This materialization has hard delete "+
			"enabled, which requires an internal _is_deleted column (UInt8) on every standard-updates "+
			"table: the connector materializes each table as a ReplacingMergeTree that uses _is_deleted "+
			"to remove rows deleted in the source. This table exists without that column -- it was "+
			"pre-created, or hard delete was enabled after the table was created -- so the connector "+
			"cannot store to it. To resolve this, either backfill this binding (increment its backfill "+
			"counter) so the connector re-creates the table with the correct schema, or disable hard "+
			"delete for this materialization",
		b.target.Identifier,
	)
}

type binding struct {
	target            sql.Table
	nullFieldsToStrip []string
	load              struct {
		createTableSQL string
		truncateSQL    string
		countKeysSQL   string
		insertSQL      string
		querySQL       string
		dropTableSQL   string
	}
	store struct {
		createTableSQL   string
		truncateSQL      string
		insertSQL        string
		queryPartsSQL    string
		movePartitionSQL string
		dropTableSQL     string
	}
}

func (t *transactor) addBinding(_ context.Context, target sql.Table) error {
	b := &binding{target: target}

	var queryLoadTemplate = t.templates.queryLoadTable
	if t.cfg.Advanced.NoFlowDocument {
		b.nullFieldsToStrip = target.NullableFieldsToStrip()
		queryLoadTemplate = t.templates.queryLoadTableNoFlowDocument
	}

	var err error
	if b.load.createTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.createLoadTable); err != nil {
		return fmt.Errorf("rendering createLoadTable template: %w", err)
	}
	if b.load.truncateSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.truncateLoadTable); err != nil {
		return fmt.Errorf("rendering truncateLoadTable template: %w", err)
	}
	if b.load.countKeysSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.countLoadKeys); err != nil {
		return fmt.Errorf("rendering countLoadKeys template: %w", err)
	}
	if b.load.insertSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.insertLoadTable); err != nil {
		return fmt.Errorf("rendering insertLoadTable template: %w", err)
	}
	if b.load.querySQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, queryLoadTemplate); err != nil {
		return fmt.Errorf("rendering queryLoadTable template: %w", err)
	}
	if b.load.dropTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.dropLoadTable); err != nil {
		return fmt.Errorf("rendering dropLoadTable template: %w", err)
	}

	if b.store.createTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.createStoreTable); err != nil {
		return fmt.Errorf("rendering createStoreTable template: %w", err)
	}
	if b.store.truncateSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.truncateStoreTable); err != nil {
		return fmt.Errorf("rendering truncateStoreTable template: %w", err)
	}
	if b.store.insertSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.insertStoreTable); err != nil {
		return fmt.Errorf("rendering insertStoreTable template: %w", err)
	}
	if b.store.queryPartsSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.queryStoreParts); err != nil {
		return fmt.Errorf("rendering queryStoreParts template: %w", err)
	}
	if b.store.movePartitionSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.moveStorePartition); err != nil {
		return fmt.Errorf("rendering moveStorePartition template: %w", err)
	}
	if b.store.dropTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.dropStoreTable); err != nil {
		return fmt.Errorf("rendering dropStoreTable template: %w", err)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

const (
	// ClickHouse recommends inserting batches of 100,000 rows.
	// https://clickhouse.com/docs/optimize/bulk-inserts
	// Load phase: insert keys to temporary tables for a subsequent lookup against the target table.
	// Store phase: insert documents to stage tables for subsequent move to the target table.
	maxBatchSize = 100_000

	// As a very rough approximation, this will limit the amount of memory used for accumulating
	// batches of keys to load or documents to store based on the size of their packed tuples. As an
	// example, if documents average 2kb then a 10mb batch size will allow for ~5000 documents per
	// batch.
	batchBytesLimit = 10 * 1024 * 1024
)

type batchItem struct {
	Binding int
	Record  []any
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) (err error) {
	var ctx = it.Context()

	// Staging keys writes to the persistent load tables, which Acknowledge
	// establishes via ensureTempTables. RunTransactions runs Acknowledge and
	// Load in concurrent goroutines, so those tables are only guaranteed to
	// exist once ensureTempTables has had a chance to run.
	<-t.ensured

	// activeBindings tracks the number of keys inserted into each binding's
	// load table this round, for verification against an authoritative count
	// before the load query runs.
	activeBindings := make(map[int]int64, len(t.bindings))
	batch := make([]batchItem, 0, maxBatchSize)
	batchBytes := 0

	flushBindings := func() error {
		if len(batch) == 0 {
			return nil
		}

		slices.SortFunc(batch, func(a, b batchItem) int {
			return cmp.Compare(a.Binding, b.Binding)
		})

		begin := 0
		for begin < len(batch) {
			lastBinding := batch[begin].Binding
			end := begin
			for end < len(batch) && batch[end].Binding == lastBinding {
				end++
			}
			items := batch[begin:end]

			// PrepareBatch fails before any rows have been sent, so retrying a
			// transient connection drop is a clean no-op on the ClickHouse side.
			var chBatch chdriver.Batch
			if err := transientRetryPolicy.retry(ctx, "preparing load batch", isTransientErr, func() (err error) {
				chBatch, err = t.load.conn.PrepareBatch(ctx, t.bindings[lastBinding].load.insertSQL)
				return err
			}); err != nil {
				return fmt.Errorf("preparing load batch for %s: %w", t.bindings[lastBinding].target.Identifier, err)
			}

			for _, item := range items {
				if err = chBatch.Append(item.Record...); err != nil {
					chBatch.Close()
					return fmt.Errorf("appending load batch for %s: %w", t.bindings[lastBinding].target.Identifier, err)
				}
			}

			if err = chBatch.Send(); err != nil {
				chBatch.Close()
				return fmt.Errorf("flushing load batch for %s: %w", t.bindings[lastBinding].target.Identifier, err)
			}
			chBatch.Close()

			begin = end
		}
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	defer func() {
		for i := range activeBindings {
			b := t.bindings[i]
			// Free resources on the ClickHouse server. Truncate rather than
			// drop: the load table's identity must stay stable so that key
			// inserts and the load query resolve the same table from any
			// replica of a clustered deployment.
			//
			// A failure leaves this round's keys in the table, so it is
			// reported: should the truncate at the start of the next round
			// fail too, that round's key-count check trips with no indication
			// of why the table was not empty.
			if truncErr := t.load.conn.Exec(ctx, b.load.truncateSQL); truncErr != nil && err == nil {
				err = fmt.Errorf("truncating load table for %s after load: %w", b.target.Identifier, truncErr)
			}
		}
	}()

	for it.Next() {
		if _, found := activeBindings[it.Binding]; !found {
			b := t.bindings[it.Binding]
			// The load table exists (ensured at session start); truncate
			// clears any keys left over from a previous round.
			if err = t.load.conn.Exec(ctx, b.load.truncateSQL); err != nil {
				return fmt.Errorf("truncating load stage table for %s: %w", b.target.Identifier, err)
			}
			activeBindings[it.Binding] = 0
		}

		b := t.bindings[it.Binding]
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting load key for %s: %w", b.target.Identifier, err)
		}
		batch = append(batch, batchItem{Binding: it.Binding, Record: converted})
		batchBytes += len(it.PackedKey)
		activeBindings[it.Binding]++

		if len(batch) >= maxBatchSize || batchBytes >= batchBytesLimit {
			if err = flushBindings(); err != nil {
				return err
			}
		}
	}
	if it.Err() != nil {
		return it.Err()
	}
	if len(activeBindings) == 0 {
		return nil
	}
	if err = flushBindings(); err != nil {
		return err
	}

	// Hard check: an authoritative count of each load table must account for
	// every key inserted this round. A shortfall means the keys are not
	// visible to the replica that will serve the load query -- running it anyway
	// would silently treat existing documents as absent, storing unreduced
	// documents over them.
	for i, inserted := range activeBindings {
		b := t.bindings[i]
		var counted uint64
		if err = t.load.conn.QueryRow(ctx, b.load.countKeysSQL).Scan(&counted); err != nil {
			return fmt.Errorf("counting load table keys for %s: %w", b.target.Identifier, err)
		}
		if int64(counted) != inserted {
			return fmt.Errorf(
				"refusing to load %s: load table contains %d keys but %d were inserted",
				b.target.Identifier, counted, inserted)
		}
	}

	// Keys are now staged and ready for the target table to be queried for them.
	loadBinding := func(i int, b *binding) error {
		// Documents already delivered via loaded() cannot be recalled, so the
		// query may only be retried while nothing has been emitted: re-running
		// it afterwards would deliver duplicates, which the runtime would
		// reduce together (double-counting under sum-style reductions). The
		// transient failures seen in practice ("failed to read first block
		// packet") surface from Query itself, before any rows are delivered.
		var emitted bool
		return transientRetryPolicy.retry(ctx, "querying Load documents",
			func(err error) bool { return !emitted && isTransientErr(err) },
			func() error {
				rows, err := t.load.conn.Query(ctx, b.load.querySQL)
				if err != nil {
					return fmt.Errorf("querying Load documents for %s: %w", b.target.Identifier, err)
				}
				defer rows.Close()

				for rows.Next() {
					var doc json.RawMessage
					if err = rows.Scan(&doc); err != nil {
						return fmt.Errorf("scanning Load document for %s: %w", b.target.Identifier, err)
					}
					if len(b.nullFieldsToStrip) > 0 {
						if doc, err = sql.StripNullFields(doc, b.nullFieldsToStrip); err != nil {
							return fmt.Errorf("stripping null fields for %s: %w", b.target.Identifier, err)
						}
					}
					emitted = true
					if err = loaded(i, doc); err != nil {
						return err
					}
				}
				if err = rows.Err(); err != nil {
					return fmt.Errorf("querying Load documents for %s: %w", b.target.Identifier, err)
				}
				return nil
			})
	}

	for i := range activeBindings {
		if err = loadBinding(i, t.bindings[i]); err != nil {
			return err
		}
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	ctx := it.Context()

	lastBinding := -1
	batch := make([][]any, 0, maxBatchSize)
	batchBytes := 0
	// The state keys this store phase staged rows for. Only these may be
	// verified at StartCommit: a key left pending by the Apply RPC's narrowed
	// drain (see Acknowledge) holds a prior process's staged rows, which have
	// nothing to do with this transaction's StoredRows.
	var storedKeys []string

	flushLastBinding := func() error {
		if len(batch) == 0 || lastBinding < 0 {
			return nil
		}
		// As with the load batch, PrepareBatch fails before any rows have been
		// sent and is safe to retry.
		var chBatch chdriver.Batch
		if err := transientRetryPolicy.retry(ctx, "preparing store batch", isTransientErr, func() (err error) {
			chBatch, err = t.store.conn.PrepareBatch(ctx, t.bindings[lastBinding].store.insertSQL)
			return err
		}); err != nil {
			return fmt.Errorf("preparing store batch for %s: %w", t.bindings[lastBinding].target.Identifier, err)
		}
		defer chBatch.Close()
		for _, record := range batch {
			if err = chBatch.Append(record...); err != nil {
				return fmt.Errorf("appending store batch for %s: %w", t.bindings[lastBinding].target.Identifier, err)
			}
		}
		batch = batch[:0]
		batchBytes = 0
		if err = chBatch.Send(); err != nil {
			return fmt.Errorf("flushing store batch for %s: %w", t.bindings[lastBinding].target.Identifier, err)
		}
		return nil
	}

	// Skip deleted, non-existent documents iff HardDelete is enabled.
	for it.Next(t.cfg.HardDelete) {
		if it.Binding != lastBinding {
			if err = flushLastBinding(); err != nil {
				return nil, err
			}
			lastBinding = it.Binding

			// The stage table itself exists and is empty: it was ensured at
			// session start and emptied by the previous transaction's
			// partition moves.
			stateKey := t.bindings[it.Binding].target.StateKey
			if _, found := t.state[stateKey]; !found {
				t.state[stateKey] = &stateItem{}
				storedKeys = append(storedKeys, stateKey)
			}
		}

		b := t.bindings[it.Binding]
		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store record for %s: %w", b.target.Identifier, err)
		}
		if t.cfg.HardDelete && !b.target.DeltaUpdates {
			var deleteState uint8 = 0
			if it.Delete {
				deleteState = 1
			}
			converted = append(converted, deleteState)
		}
		batch = append(batch, converted)
		batchBytes += len(it.PackedKey) + len(it.PackedValues) + len(it.RawJSON)
		t.state[b.target.StateKey].StoredRows++

		if len(batch) >= maxBatchSize || batchBytes >= batchBytesLimit {
			if err = flushLastBinding(); err != nil {
				return nil, err
			}
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}
	if err = flushLastBinding(); err != nil {
		return nil, err
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		// Returning a pre-resolved error here makes the runtime fail without
		// sending StartedCommit, so this checkpoint never becomes durable and the
		// transaction is retried from the prior one.
		if err := t.verifyStagedRows(ctx, storedKeys); err != nil {
			return nil, pf.FinishedOperation(err)
		}

		checkpointJSON, err := json.Marshal(t.state)
		if err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling connector state JSON: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON, MergePatch: false}, pf.FinishedOperation(err)
	}, nil
}

func (t *transactor) bindingForStateKey(stateKey string) (*binding, bool) {
	for _, b := range t.bindings {
		if b.target.StateKey == stateKey {
			return b, true
		}
	}
	return nil, false
}

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	shouldProcess := m.StateKeyFilter(stateKeys)
	var drained []string

	select {
	default: // ensured channel is not closed
		// First Acknowledge of the session: recover pending commits and
		// ensure every binding's persistent temp tables. This always runs
		// before the first Store (the runtime serializes Acknowledge ahead
		// of the next transaction's store phase).
		for _, b := range t.bindings {
			if si, pending := t.state[b.target.StateKey]; pending {
				if !shouldProcess(b.target.StateKey) {
					// This binding's pending work was not requested, so its
					// staged rows must stay put. ensureTempTables would
					// discard them, so it is skipped as well; a session
					// always processes every state key, so this only happens
					// in the Apply RPC's narrowed drain, where no Store
					// follows.
					continue
				}
				drained = append(drained, b.target.StateKey)
				// A store table whose partition key differs from the target's
				// predates a partition-changing backfill: the backfill dropped
				// and re-created the target, so the staged rows belong to a
				// table generation that no longer exists and the backfill will
				// re-send them. MOVE PARTITION into the new target can never
				// succeed (code 36), so skip the move and let ensureTempTables
				// re-create the store table.
				if drifted, err := t.storePartitionKeyDrifted(ctx, b); err != nil {
					return nil, fmt.Errorf("comparing store and target partition keys of %s: %w", b.target.Identifier, err)
				} else if drifted {
					log.WithField("target", b.target.Identifier).Warn(
						"store table partition key differs from target; staged rows predate a partition-changing backfill and are discarded")
					if err := t.ensureTempTables(ctx, b); err != nil {
						return nil, fmt.Errorf("ensuring temp tables of %s: %w", b.target.Identifier, err)
					}
					continue
				}
				// A committed-but-unacknowledged transaction has rows staged
				// in this binding's store table. Move them; never truncate or
				// re-create a stage table holding pending rows.
				if err := t.moveStorePartitionsToTarget(ctx, b, si, true); err != nil {
					if !isUnknownTableErr(err) {
						return nil, fmt.Errorf("recovering stage to target %s: %w", b.target.Identifier, err)
					}
					// The store table was dropped out-of-band, so there are no
					// staged rows left to recover. Fall through to re-create
					// the temp tables rather than crash-looping on the missing
					// table; ReplacingMergeTree lets later transactions refresh
					// the lost rows.
					log.WithField("target", b.target.Identifier).Warn(
						"store table missing during recovery; nothing to recover, re-creating temp tables")
				}
				// A successful recovery move leaves the store table empty, so
				// falling through to ensureTempTables is safe and reconciles
				// any target-schema drift that occurred while the commit was
				// pending. Skipping it would fail the next MOVE PARTITION with
				// code 122 ("Tables have different structure") -- a permanent
				// recovery crash-loop (issue #4817).
			}
			if err := t.ensureTempTables(ctx, b); err != nil {
				return nil, fmt.Errorf("ensuring temp tables of %s: %w", b.target.Identifier, err)
			}
		}
		close(t.ensured)
	case <-t.ensured: // ensured channel is closed
		for stateKey, si := range t.state {
			if !shouldProcess(stateKey) {
				continue
			}
			// Skip target tables which do not have a binding anymore since
			// these tables might be deleted already. Note that the persistent
			// stage table of a removed binding with pending state is stranded
			// (never moved, never dropped) -- a known limitation requiring
			// manual cleanup.
			b, found := t.bindingForStateKey(stateKey)
			if !found {
				continue
			}
			if err := t.moveStorePartitionsToTarget(ctx, b, si, false); err != nil {
				return nil, fmt.Errorf("moving stage to target %s: %w", b.target.Identifier, err)
			}
			drained = append(drained, stateKey)
		}
	}
	t.recovery = false

	// After having applied the connectorState, we try to clean up the connectorState in the ack response
	// so that a restart of the connector does not need to run the same queries again
	// Note that this is an best-effort "attempt" and there is no guarantee that this connectorState update
	// can actually be committed
	// Important to note that in this case we do not reset the connectorState for all bindings, but only the ones
	// that have been committed in this transaction. The reason is that it may be the case that a binding
	// which has been disabled right after a failed attempt to run its queries, must be able to recover by enabling
	// the binding and running the queries that are pending for its last transaction.
	if len(drained) == 0 {
		return nil, nil
	}

	var stateClear = make(connectorState, len(drained))
	for _, sk := range drained {
		stateClear[sk] = nil
		delete(t.state, sk)
	}

	checkpointJSON, err := json.Marshal(stateClear)
	if err != nil {
		return nil, fmt.Errorf("marshalling connector state clearing JSON: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: checkpointJSON, MergePatch: true}, nil
}

// isUnknownTableErr reports whether err (anywhere in its chain) is a ClickHouse
// "unknown table" exception (code 60), raised when a query references a table
// that does not exist.
func isUnknownTableErr(err error) bool {
	var exc *clickhouseproto.Exception
	return errors.As(err, &exc) && exc.Code == int32(chproto.ErrUnknownTable)
}

// queryStoreParts reads the binding's store table partitions and their row
// counts under select_sequential_consistency, which makes the result
// authoritative for the connection that will service the partition moves.
func (t *transactor) queryStoreParts(ctx context.Context, b *binding) (parts []stagedPartition, totalRows int64, err error) {
	rows, err := t.store.conn.Query(ctx, b.store.queryPartsSQL)
	if err != nil {
		return nil, 0, fmt.Errorf("querying store table partitions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var part stagedPartition
		var partitionRows uint64
		if err = rows.Scan(&part.ID, &partitionRows); err != nil {
			return nil, 0, fmt.Errorf("scanning store table partition: %w", err)
		}
		part.Rows = int64(partitionRows)
		parts = append(parts, part)
		totalRows += part.Rows
	}
	if err = rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterating store table partitions: %w", err)
	}
	return parts, totalRows, nil
}

// verifyStagedRows reconciles each named binding's stage table against the
// number of rows its Store phase inserted, and records the partitions to move
// on success. It runs before the transaction's checkpoint is durable, so a
// mismatch fails the transaction rather than committing it: the runtime retries
// from the prior checkpoint, ensureTempTables discards the staged rows at the
// next session start, and the documents are re-stored.
//
// Verifying here rather than at Acknowledge is what makes the check meaningful
// on the recovery path. A failure at Acknowledge leaves a durable checkpoint
// whose staged rows are already known to be short, and a later recovery of that
// checkpoint has no way to tell those missing rows from ones a prior process had
// legitimately already moved.
func (t *transactor) verifyStagedRows(ctx context.Context, stateKeys []string) error {
	for _, stateKey := range stateKeys {
		si, ok := t.state[stateKey]
		if !ok {
			continue
		}
		b, found := t.bindingForStateKey(stateKey)
		if !found {
			continue
		}

		parts, totalRows, err := t.queryStoreParts(ctx, b)
		if err != nil {
			return fmt.Errorf("verifying staged rows of %s: %w", b.target.Identifier, err)
		}
		if totalRows != si.StoredRows {
			t.discardFailedTransactionRows(ctx, stateKeys)
			return fmt.Errorf(
				"refusing to commit store table of %s: it contains %d staged rows but %d rows were "+
					"stored; not committing so the transaction can be retried. This means the rows "+
					"inserted during the transaction are not all present in the stage table -- either "+
					"they are not visible to this connection, or the table's engine destroyed them "+
					"because its sorting key does not cover the binding's key fields",
				b.target.Identifier, totalRows, si.StoredRows)
		}
		si.MovePartitions, si.Verified = parts, true
	}
	return nil
}

// discardFailedTransactionRows empties the stage tables of a transaction that is
// being abandoned because its staged rows could not be accounted for.
//
// The rows must not be left behind. This transaction's checkpoint never becomes
// durable, so the runtime re-sends its documents from the prior checkpoint -- but
// the pending state recovered by the next process describes the transaction
// *before* this one, whose own state-clearing patch was still in flight. Leftover
// rows would then be reconciled against that earlier transaction's plan, which
// they do not match, and the pending state stops ensureTempTables from clearing
// them: the next session refuses for a reason no operator can act on, and every
// session after it does the same. Emptying the tables here leaves the stage in
// exactly the state the earlier transaction's completed commit left it in, so
// recovery sees its partitions already moved and the task makes progress.
//
// Failure to truncate is logged and swallowed: the accounting error is the one
// worth reporting, and a session start with no pending state truncates anyway.
func (t *transactor) discardFailedTransactionRows(ctx context.Context, stateKeys []string) {
	for _, stateKey := range stateKeys {
		b, found := t.bindingForStateKey(stateKey)
		if !found {
			continue
		}
		if err := t.store.conn.Exec(ctx, b.store.truncateSQL); err != nil {
			log.WithFields(log.Fields{
				"target": b.target.Identifier,
				"error":  err,
			}).Warn("could not empty the stage table of a failed transaction")
		}
	}
}

// moveStorePartitionsToTarget commits a transaction's staged rows by moving
// every partition of the binding's store table into its target table. All SQL
// is rendered from the live binding -- never from persisted state -- so that
// state written by older connector versions recovers cleanly.
//
// recovery is true when re-applying a commit from a previous process, whose
// partition moves may have partially (or fully) completed already. The verified
// plan recorded by verifyStagedRows resolves that case: a planned partition
// missing from the stage table was moved by that process, while one that is
// present but short means rows were never staged, which must never be committed.
func (t *transactor) moveStorePartitionsToTarget(ctx context.Context, b *binding, si *stateItem, recovery bool) error {
	observed, totalRows, err := t.queryStoreParts(ctx, b)
	if err != nil {
		return err
	}

	toMove, err := t.reconcileStagedPartitions(b, si, observed, totalRows, recovery)
	if err != nil {
		return err
	}

	for _, partitionID := range toMove {
		// Retrying MOVE PARTITION after a transient connection drop is safe
		// even if the server had already applied it: moving a partition with
		// no remaining parts is a no-op, and the row accounting above plus the
		// empty-stage check below still catch any genuinely stuck or partial
		// move as a hard error.
		if err = transientRetryPolicy.retry(ctx, "moving store table partition", isTransientErr, func() error {
			return t.store.conn.Exec(ctx, b.store.movePartitionSQL, partitionID)
		}); err != nil {
			// Never truncate or drop here: unmoved staged rows must survive
			// for a retry (or operator intervention, e.g. if the stage table
			// schema has drifted from a target migrated while this commit was
			// pending).
			return fmt.Errorf("moving store table partition: %w", err)
		}
	}

	// Hard check: after moving every partition the stage table must be empty.
	// Rows appearing here would be re-moved by a later transaction's commit,
	// or destroyed if the table were ever cleaned up.
	if _, remainingRows, err := t.queryStoreParts(ctx, b); err != nil {
		return err
	} else if remainingRows > 0 {
		return fmt.Errorf(
			"store table still contains %d rows after moving %d partition(s)",
			remainingRows, len(toMove))
	}

	return nil
}

// reconcileStagedPartitions returns the partition IDs to move, or an error when
// the stage table's contents cannot be reconciled with what the transaction
// staged. It is the check that stands between an incomplete commit and a
// silently truncated target table.
func (t *transactor) reconcileStagedPartitions(
	b *binding, si *stateItem, observed []stagedPartition, totalRows int64, recovery bool,
) ([]string, error) {
	if si.Verified {
		var observedRows = make(map[string]int64, len(observed))
		for _, p := range observed {
			observedRows[p.ID] = p.Rows
		}

		var toMove = make([]string, 0, len(si.MovePartitions))
		for _, planned := range si.MovePartitions {
			rows, present := observedRows[planned.ID]
			if !present {
				if !recovery {
					// This process verified the partition moments ago and nothing
					// stages into a store table between then and the move, so on
					// this path a missing partition is lost rows, not progress.
					return nil, fmt.Errorf(
						"refusing to commit store table of %s: partition %s held %d staged rows at "+
							"the start of this commit and is now gone from the store table; not "+
							"moving partitions",
						b.target.Identifier, planned.ID, planned.Rows)
				}
				// A prior process moved this partition before exiting. MOVE
				// PARTITION on it is a no-op, so it stays in the list. Note that
				// a stage table truncated out of band is indistinguishable from
				// one whose partitions all moved; both present as empty.
				continue
			}
			delete(observedRows, planned.ID)
			if rows != planned.Rows {
				return nil, fmt.Errorf(
					"refusing to commit store table: partition %s contains %d rows but %d were staged "+
						"in it; not moving partitions so the staged rows are preserved for inspection",
					planned.ID, rows, planned.Rows)
			}
			toMove = append(toMove, planned.ID)
		}
		// Nothing inserts into a stage table between the verification and the
		// move: the runtime completes each Acknowledge before the next
		// transaction's Store. An unplanned partition therefore means the table
		// is not the one that was verified.
		if len(observedRows) > 0 {
			return nil, fmt.Errorf(
				"refusing to commit store table: it contains %d partition(s) that were not staged by "+
					"this transaction", len(observedRows))
		}
		return toMove, nil
	}

	var toMove = make([]string, 0, len(observed))
	for _, p := range observed {
		toMove = append(toMove, p.ID)
	}

	// State written by a connector version that recorded no verified plan. All
	// that can be said is whether the staged rows add up.
	switch {
	case si.StoredRows == 0:
		// The count was never recorded either, so no accounting is possible.
	case totalRows == si.StoredRows:
		// Every stored row is still staged.
	case recovery && totalRows == 0:
		// The prior process completed every move before exiting.
	case recovery:
		// Some rows are missing and there is no plan to explain which: either a
		// prior process moved part of the commit, or they were never staged at
		// all. Committing the remainder would risk truncating the target, and
		// the difference cannot be resolved without an operator.
		return nil, fmt.Errorf(
			"refusing to commit store table of %s: it contains %d staged rows but %d rows were stored, "+
				"and this transaction's checkpoint carries no verified partition plan to explain the "+
				"difference; not moving partitions so the staged rows are preserved. Backfill this "+
				"binding if its target table is missing rows",
			b.target.Identifier, totalRows, si.StoredRows)
	default:
		return nil, fmt.Errorf(
			"refusing to commit store table of %s: it contains %d staged rows but %d rows were stored; "+
				"not moving partitions so staged rows are preserved for retry",
			b.target.Identifier, totalRows, si.StoredRows)
	}
	return toMove, nil
}

// ensureTempTables establishes the binding's persistent temp tables at
// session start: the store stage table for every binding, and the load table
// for standard-updates bindings. Tables are created if missing, re-created if
// their schema has drifted from the target's (a migration ran in a prior
// session -- migrations only happen in the Apply RPC, never while a session
// is running), and truncated otherwise to discard rows staged by a
// transaction that never committed. It must only be called when the store
// table holds no staged rows of a pending commit; on the recovery path this
// is guaranteed by moveStorePartitionsToTarget's post-move emptiness check.
func (t *transactor) ensureTempTables(ctx context.Context, b *binding) error {
	ensure := func(createSQL, truncateSQL, dropSQL string, tableName string) error {
		if err := t.store.conn.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("creating temp table: %w", err)
		}
		matches, err := t.tempTableMatchesTarget(ctx, tableName, b)
		if err != nil {
			return err
		}
		if !matches {
			if err := t.store.conn.Exec(ctx, dropSQL); err != nil {
				return fmt.Errorf("dropping schema-drifted temp table: %w", err)
			}
			if err := t.store.conn.Exec(ctx, createSQL); err != nil {
				return fmt.Errorf("re-creating temp table: %w", err)
			}
			return nil
		}
		if err := t.store.conn.Exec(ctx, truncateSQL); err != nil {
			return fmt.Errorf("truncating temp table: %w", err)
		}
		return nil
	}

	if err := ensure(b.store.createTableSQL, b.store.truncateSQL, b.store.dropTableSQL, storeTableName(b.target, t._range.KeyBegin)); err != nil {
		return fmt.Errorf("store stage table: %w", err)
	}
	if !b.target.DeltaUpdates {
		// Load tables hold only the current round's lookup keys, so schema
		// drift (changed key columns) is handled the same way; a stray drop
		// loses nothing durable.
		if err := ensure(b.load.createTableSQL, b.load.truncateSQL, b.load.dropTableSQL, loadTableName(b.target, t._range.KeyBegin)); err != nil {
			return fmt.Errorf("load table: %w", err)
		}
	}
	return nil
}

// storePartitionKeyDrifted reports whether the binding's store table and
// target table both exist but have different partition keys. If either table
// is missing it reports false, leaving that condition to the caller's
// existing handling (a recovery MOVE against a missing table raises the
// unknown-table error, which is tolerated).
func (t *transactor) storePartitionKeyDrifted(ctx context.Context, b *binding) (bool, error) {
	var storeTable = storeTableName(b.target, t._range.KeyBegin)
	var targetTable = b.target.Path[0]

	rows, err := t.store.conn.Query(ctx,
		"SELECT name, partition_key FROM system.tables WHERE database = currentDatabase() AND name IN (?, ?)",
		storeTable, targetTable)
	if err != nil {
		return false, fmt.Errorf("querying partition keys: %w", err)
	}
	defer rows.Close()

	var keys = make(map[string]string)
	for rows.Next() {
		var name, pk string
		if err := rows.Scan(&name, &pk); err != nil {
			return false, fmt.Errorf("scanning partition key: %w", err)
		}
		keys[name] = pk
	}
	if err := rows.Err(); err != nil {
		return false, err
	}

	storeKey, haveStore := keys[storeTable]
	targetKey, haveTarget := keys[targetTable]
	return haveStore && haveTarget && storeKey != targetKey, nil
}

// tempTableMatchesTarget reports whether the temp table's columns are a
// (positional, typed) prefix-compatible match for what the connector will
// insert into it. The temp table is compared against its own expected shape:
// for store tables that is the target table's full column set (the stage is
// created AS the target); for load tables it is the key columns. Rather than
// re-deriving expectations, both the temp table and the target are read from
// system.columns and compared by (name, type) in position order; for load
// tables only the leading key columns of the target are considered.
func (t *transactor) tempTableMatchesTarget(ctx context.Context, tempTable string, b *binding) (bool, error) {
	type column struct {
		name, typ string
	}
	readColumns := func(table string) ([]column, error) {
		rows, err := t.store.conn.Query(ctx,
			"SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position", table)
		if err != nil {
			return nil, fmt.Errorf("querying system.columns of %q: %w", table, err)
		}
		defer rows.Close()
		var out []column
		for rows.Next() {
			var c column
			if err := rows.Scan(&c.name, &c.typ); err != nil {
				return nil, fmt.Errorf("scanning system.columns of %q: %w", table, err)
			}
			out = append(out, c)
		}
		return out, rows.Err()
	}

	tempCols, err := readColumns(tempTable)
	if err != nil {
		return false, err
	}
	targetCols, err := readColumns(b.target.Path[0])
	if err != nil {
		return false, err
	}

	want := targetCols
	if tempTable == loadTableName(b.target, t._range.KeyBegin) {
		// Load tables contain only the key columns.
		if len(targetCols) < len(b.target.Keys) {
			return false, nil
		}
		want = targetCols[:len(b.target.Keys)]
	}

	if len(tempCols) != len(want) {
		return false, nil
	}
	for i := range want {
		if tempCols[i] != want[i] {
			return false, nil
		}
	}

	// Store tables must also share the target's partition key, because commits
	// move whole partitions between them and MOVE PARTITION requires it, and its
	// sorting key, because a stage table created AS a target with a bad sorting
	// key destroys rows as they are staged even after the target itself has been
	// repaired. Both drift when the target is re-created -- by a backfill with a
	// different partition_by, or by an operator fixing its ORDER BY -- while the
	// persistent store table keeps the old definition. Load tables are never
	// moved and hold only lookup keys, so neither key matters for them.
	if tempTable == storeTableName(b.target, t._range.KeyBegin) {
		readKeys := func(table string) (partitionKey, sortingKey string, err error) {
			if err := t.store.conn.QueryRow(ctx,
				"SELECT partition_key, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = ?", table,
			).Scan(&partitionKey, &sortingKey); err != nil {
				return "", "", fmt.Errorf("querying partition and sorting keys of %q: %w", table, err)
			}
			return partitionKey, sortingKey, nil
		}
		tempPartitionKey, tempSortingKey, err := readKeys(tempTable)
		if err != nil {
			return false, err
		}
		targetPartitionKey, targetSortingKey, err := readKeys(b.target.Path[0])
		if err != nil {
			return false, err
		}
		if tempPartitionKey != targetPartitionKey || tempSortingKey != targetSortingKey {
			return false, nil
		}
	}

	return true, nil
}

func (t *transactor) Destroy() {
	// Temp tables are deliberately NOT dropped here. Store stage tables may
	// hold a committed-but-unacknowledged transaction's rows, which the next
	// session recovers and moves to the target -- dropping them would lose
	// data. Load tables are dropped the same way for symmetry: both kinds are
	// persistent, and are dropped + re-created only by the ensure pass when
	// their schema has drifted from the target's.
	_ = t.store.conn.Close()
	_ = t.load.conn.Close()
}
