package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
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
	HardDelete  bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default this is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=2"`
	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=3"`
	Schedule    m.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	SSLMode        string `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Controls the TLS connection behavior.,enum=disable,enum=require,enum=verify-full,default=verify-full"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
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
		TLS:  tlsConfig,
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
	Delta bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true"`
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

func newClickHouseDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
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
	// ensured is set once the first Acknowledge of the session has run the
	// ensure pass: recovering any pending commits and creating / truncating /
	// re-creating the persistent temp tables for every binding. It is
	// deliberately independent of the recovery flag, which is only set when
	// the Open request carried prior connector state -- a brand-new task has
	// no state but still needs its temp tables ensured before the first
	// transaction.
	ensured           bool
	state             connectorState
	runtimeCheckpoint m.RuntimeCheckpoint
}

func (t *transactor) RecoverCheckpoint(_ context.Context, _ pf.MaterializationSpec, _ pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return t.runtimeCheckpoint, nil
}

// stateItem records that a binding has a committed-but-not-yet-acknowledged
// transaction whose rows are staged in its store table. The SQL needed to
// commit those rows is always rendered freshly from the live binding -- never
// persisted -- so that state written by one version of the connector is safe
// to recover with another.
type stateItem struct {
	// StoredRows is the number of rows inserted into the stage table during
	// the transaction. Before moving partitions to the target table, it is
	// checked against an authoritative (select_sequential_consistency) row
	// count of the stage table: a mismatch means the staged rows are not
	// visible to the connection servicing the move, and moving would silently
	// lose them. A value of 0 means the count is unknown (state recovered
	// from a checkpoint written by a prior version of the connector) and
	// disables the check.
	StoredRows int64
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

	return t, nil
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
	// Load phase: insert keys to temporary tables for subsequent join on the target table.
	// Store phase: insert documents to stage tables for subsequent move to the target table.
	maxBatchSize = 100_000

	// As a very rough approximation, this will limit the amount of memory used for accumulating
	// batches of keys to load or documents to store based on the size of their packed tuples. As an
	// example, if documents average 2kb then a 10mb batch size will allow for ~5000 documents per
	// batch.
	batchBytesLimit = 10 * 1024 * 1024
)

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) (err error) {
	var ctx = it.Context()

	// activeBindings tracks the number of keys inserted into each binding's
	// load table this round, for verification against an authoritative count
	// before the join.
	activeBindings := make(map[int]int64, len(t.bindings))
	lastBinding := -1
	batch := make([][]any, 0, maxBatchSize)
	batchBytes := 0

	flushLastBinding := func() error {
		if len(batch) == 0 || lastBinding < 0 {
			return nil
		}
		chBatch, err := t.load.conn.PrepareBatch(ctx, t.bindings[lastBinding].load.insertSQL)
		if err != nil {
			return fmt.Errorf("preparing load batch: %w", err)
		}
		defer chBatch.Close()
		for _, record := range batch {
			if err = chBatch.Append(record...); err != nil {
				return fmt.Errorf("appending load batch: %w", err)
			}
		}
		batch = batch[:0]
		batchBytes = 0
		if err = chBatch.Send(); err != nil {
			return fmt.Errorf("flushing load batch: %w", err)
		}
		return nil
	}

	defer func() {
		for i := range activeBindings {
			b := t.bindings[i]
			// Free resources on the ClickHouse server. Truncate rather than
			// drop: the load table's identity must stay stable so that key
			// inserts and the join query resolve the same table from any
			// replica of a clustered deployment.
			_ = t.load.conn.Exec(ctx, b.load.truncateSQL)
		}
	}()

	for it.Next() {
		if it.Binding != lastBinding {
			if err = flushLastBinding(); err != nil {
				return err
			}
			lastBinding = it.Binding

			if _, found := activeBindings[it.Binding]; !found {
				b := t.bindings[it.Binding]
				// The load table exists (ensured at session start); truncate
				// clears any keys left over from a previous round.
				if err = t.load.conn.Exec(ctx, b.load.truncateSQL); err != nil {
					return fmt.Errorf("truncating load stage table: %w", err)
				}
				activeBindings[it.Binding] = 0
			}
		}

		b := t.bindings[it.Binding]
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting load key: %w", err)
		}
		batch = append(batch, converted)
		batchBytes += len(it.PackedKey)
		activeBindings[it.Binding]++

		if len(batch) >= maxBatchSize || batchBytes >= batchBytesLimit {
			if err = flushLastBinding(); err != nil {
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
	if err = flushLastBinding(); err != nil {
		return err
	}

	// Hard check: an authoritative count of each load table must account for
	// every key inserted this round. A shortfall means the keys are not
	// visible to the replica that will serve the join -- running it anyway
	// would silently treat existing documents as absent, storing unreduced
	// documents over them.
	for i, inserted := range activeBindings {
		b := t.bindings[i]
		var counted uint64
		if err = t.load.conn.QueryRow(ctx, b.load.countKeysSQL).Scan(&counted); err != nil {
			return fmt.Errorf("counting load table keys: %w", err)
		}
		if int64(counted) != inserted {
			return fmt.Errorf(
				"refusing to load %s: load table contains %d keys but %d were inserted",
				b.target.Identifier, counted, inserted)
		}
	}

	// Keys are now ready to be JOIN'd between the temporary and target tables.
	// The union query runs with select_sequential_consistency so that it
	// observes both the just-inserted keys and target-table rows committed by
	// prior transactions' partition moves, from any replica.

	loadQueries := make([]string, 0, len(activeBindings))
	for i := range activeBindings {
		loadQueries = append(loadQueries, t.bindings[i].load.querySQL)
	}
	loadQueryUnionSQL := strings.Join(loadQueries, "\nUNION ALL\n") + "\nSETTINGS select_sequential_consistency = 1;"
	rows, err := t.load.conn.Query(ctx, loadQueryUnionSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var bindingId int32
		var doc json.RawMessage
		if err = rows.Scan(&bindingId, &doc); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		}
		if b := t.bindings[bindingId]; len(b.nullFieldsToStrip) > 0 {
			if doc, err = sql.StripNullFields(doc, b.nullFieldsToStrip); err != nil {
				return fmt.Errorf("stripping null fields: %w", err)
			}
		}
		if err = loaded(int(bindingId), doc); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	ctx := it.Context()

	lastBinding := -1
	batch := make([][]any, 0, maxBatchSize)
	batchBytes := 0

	flushLastBinding := func() error {
		if len(batch) == 0 || lastBinding < 0 {
			return nil
		}
		chBatch, err := t.store.conn.PrepareBatch(ctx, t.bindings[lastBinding].store.insertSQL)
		if err != nil {
			return fmt.Errorf("preparing store batch: %w", err)
		}
		defer chBatch.Close()
		for _, record := range batch {
			if err = chBatch.Append(record...); err != nil {
				return fmt.Errorf("appending store batch: %w", err)
			}
		}
		batch = batch[:0]
		batchBytes = 0
		if err = chBatch.Send(); err != nil {
			return fmt.Errorf("flushing store batch: %w", err)
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
			}
		}

		b := t.bindings[it.Binding]
		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store record: %w", err)
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

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	if !t.ensured {
		// First Acknowledge of the session: recover pending commits and
		// ensure every binding's persistent temp tables. This always runs
		// before the first Store (the runtime serializes Acknowledge ahead
		// of the next transaction's store phase).
		for _, b := range t.bindings {
			if si, pending := t.state[b.target.StateKey]; pending {
				// A committed-but-unacknowledged transaction has rows staged
				// in this binding's store table. Move them; never truncate or
				// re-create a stage table holding pending rows.
				err := t.moveStorePartitionsToTarget(ctx, b, si, true)
				if err == nil {
					continue
				}
				if !isUnknownTableErr(err) {
					return nil, fmt.Errorf("recovering stage to target %s: %w", b.target.Identifier, err)
				}
				// The store table was lost out-of-band (e.g. dropped) while the
				// connector state still recorded a pending commit referencing
				// it. No staged rows remain to recover -- the same situation as
				// a commit that fully completed before the previous process
				// exited -- so fall through to (re-)create the temp tables for
				// the next transaction instead of crash-looping forever on the
				// missing table. The pending transaction's rows are
				// unrecoverable regardless; ReplacingMergeTree lets later
				// transactions refresh them.
				log.WithField("target", b.target.Identifier).Warn(
					"store table missing during recovery; nothing to recover, re-creating temp tables")
			}
			if err := t.ensureTempTables(ctx, b); err != nil {
				return nil, fmt.Errorf("ensuring temp tables of %s: %w", b.target.Identifier, err)
			}
		}
		t.ensured = true
	} else {
		for stateKey, si := range t.state {
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
	var stateClear = make(connectorState, len(t.bindings))
	for _, b := range t.bindings {
		stateClear[b.target.StateKey] = nil
		delete(t.state, b.target.StateKey)
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

// moveStorePartitionsToTarget commits a transaction's staged rows by moving
// every partition of the binding's store table into its target table. All SQL
// is rendered from the live binding -- never from persisted state -- so that
// state written by older connector versions recovers cleanly.
//
// recovery is true when re-applying a commit from a previous process. In that
// case the staged rows may have been partially (or fully) moved already, so
// the pre-move row accounting is skipped: the consistent read is
// authoritative, and re-applying the remainder is idempotent.
func (t *transactor) moveStorePartitionsToTarget(ctx context.Context, b *binding, si *stateItem, recovery bool) error {
	queryParts := func() (partitionIDs []string, totalRows int64, err error) {
		rows, err := t.store.conn.Query(ctx, b.store.queryPartsSQL)
		if err != nil {
			return nil, 0, fmt.Errorf("querying store table partitions: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var partitionID string
			var partitionRows uint64
			if err = rows.Scan(&partitionID, &partitionRows); err != nil {
				return nil, 0, fmt.Errorf("scanning store table partition: %w", err)
			}
			partitionIDs = append(partitionIDs, partitionID)
			totalRows += int64(partitionRows)
		}
		if err = rows.Err(); err != nil {
			return nil, 0, fmt.Errorf("iterating store table partitions: %w", err)
		}
		return partitionIDs, totalRows, nil
	}

	partitionIDs, totalRows, err := queryParts()
	if err != nil {
		return err
	}

	// Hard check: the authoritative count must account for every row inserted
	// into the stage table during the transaction. A mismatch means the rows
	// are not visible to the connection servicing the move; proceeding would
	// silently lose them. Fail instead, leaving the stage table intact for a
	// retry. The check is skipped during recovery, where a prior partial move
	// legitimately leaves fewer rows (zero rows meaning the commit had fully
	// completed before the previous process exited).
	if !recovery && si.StoredRows > 0 && totalRows != si.StoredRows {
		return fmt.Errorf(
			"refusing to commit store table: it contains %d staged rows but %d rows were stored; "+
				"not moving partitions so staged rows are preserved for retry",
			totalRows, si.StoredRows)
	}

	for _, partitionID := range partitionIDs {
		if err = t.store.conn.Exec(ctx, b.store.movePartitionSQL, partitionID); err != nil {
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
	if _, remainingRows, err := queryParts(); err != nil {
		return err
	} else if remainingRows > 0 {
		return fmt.Errorf(
			"store table still contains %d rows after moving %d partition(s)",
			remainingRows, len(partitionIDs))
	}

	return nil
}

// ensureTempTables establishes the binding's persistent temp tables at
// session start: the store stage table for every binding, and the load table
// for standard-updates bindings. Tables are created if missing, re-created if
// their schema has drifted from the target's (a migration ran in a prior
// session -- migrations only happen in the Apply RPC, never while a session
// is running), and truncated otherwise to discard rows staged by a
// transaction that never committed. It must never be called for a binding
// with a pending commit recorded in the connector state.
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

func main() {
	boilerplate.RunMain(newClickHouseDriver())
}
