package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
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
	Address     string           `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 9000 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	Database    string           `json:"database" jsonschema:"title=Database,description=Name of the ClickHouse database to materialize to." jsonschema_extras:"order=2"`
	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=1"`
	HardDelete  bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default this is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
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
	return c.Credentials.Validate()
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
		address = address + ":9000"
	}
	return address
}

func (c config) newClickhouseOptions() *clickhouse.Options {
	return &clickhouse.Options{
		Addr: []string{c.resolvedAddress()},
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
// Delta updates are not supported: they skip the Load phase and lose reduce/merge
// semantics that Flow uses to compute correct document state. Standard mode always
// performs Load → merge → Store, using ReplacingMergeTree to deduplicate on read.
type tableConfig struct {
	Table string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
}

func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (r tableConfig) WithDefaults(_ config) tableConfig { return r }

// Parameters always returns delta=false. ClickHouse materializations always run in
// standard mode so that Flow loads existing documents, computes reductions, and stores
// the merged result. This is required for correct reduce/sum/etc. semantics.
func (r tableConfig) Parameters() ([]string, bool, error) {
	return []string{r.Table}, false, nil
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

	recovery bool
	state    connectorState
}

type stateItem struct {
	QueryPartsSQL    string
	MovePartitionSQL string
	DropTableSQL     string
}

type connectorState map[string]*stateItem

func (cp connectorState) ToConnectorState(mergePatch bool) (*pf.ConnectorState, error) {
	b, err := json.Marshal(cp)
	if err != nil {
		return nil, fmt.Errorf("marshalling connectorState: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: b, MergePatch: mergePatch}, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &t.state); err != nil {
		return fmt.Errorf("unmarshalling connectorState: %w", err)
	}
	t.recovery = true
	return nil
}

func newTransactor(
	ctx context.Context,
	materializationName string,
	featureFlags map[string]bool,
	ep *sql.Endpoint[config],
	_ sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	_ *boilerplate.InfoSchema,
	be *m.BindingEvents,
) (m.Transactor, error) {
	var cfg = ep.Config
	t := &transactor{
		dialect:   ep.Dialect,
		templates: renderTemplates(ep.Dialect, cfg.HardDelete),
		cfg:       cfg,
		be:        be,
		_range:    open.Range,
		state:     make(connectorState, len(bindings)),
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
	target sql.Table
	load   struct {
		createTableSQL string
		insertSQL      string
		querySQL       string
		dropTableSQL   string
	}
	store struct {
		createTableSQL   string
		insertSQL        string
		queryPartsSQL    string
		movePartitionSQL string
		existsSQL        string
		dropTableSQL     string
	}
}

type activeBinding struct {
	binding *binding
	batch   [][]any
}

func (t *transactor) addBinding(_ context.Context, target sql.Table) error {
	b := &binding{target: target}

	var err error
	if b.load.createTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.createLoadTable); err != nil {
		return fmt.Errorf("rendering createLoadTable template: %w", err)
	}
	if b.load.insertSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.insertLoadTable); err != nil {
		return fmt.Errorf("rendering insertLoadTable template: %w", err)
	}
	if b.load.querySQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.queryLoadTable); err != nil {
		return fmt.Errorf("rendering queryLoadTable template: %w", err)
	}
	if b.load.dropTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.dropLoadTable); err != nil {
		return fmt.Errorf("rendering dropLoadTable template: %w", err)
	}

	if b.store.createTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.createStoreTable); err != nil {
		return fmt.Errorf("rendering createStoreTable template: %w", err)
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
	if b.store.existsSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.existsStoreTable); err != nil {
		return fmt.Errorf("rendering existsStoreTable template: %w", err)
	}
	if b.store.dropTableSQL, err = renderTableAndRangeKey(target, t._range.KeyBegin, t.templates.dropStoreTable); err != nil {
		return fmt.Errorf("rendering dropStoreTable template: %w", err)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

// ClickHouse recommends inserting batches of 100,000 rows.
// https://clickhouse.com/docs/optimize/bulk-inserts
// Load phase: insert keys to temporary tables for subsequent join on the target table.
// Store phase: insert documents to stage tables for subsequent move to the target table.
const maxBatchSize = 100_000

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) (err error) {
	var ctx = it.Context()

	activeBindings := make(map[int]*activeBinding, len(t.bindings))
	flushActiveBinding := func(ab *activeBinding) error {
		if len(ab.batch) == 0 {
			return nil
		}
		chBatch, err := t.load.conn.PrepareBatch(ctx, ab.binding.load.insertSQL)
		if err != nil {
			return fmt.Errorf("preparing load batch: %w", err)
		}
		defer chBatch.Close()
		for _, record := range ab.batch {
			if err = chBatch.Append(record...); err != nil {
				return fmt.Errorf("appending load batch: %w", err)
			}
		}
		ab.batch = ab.batch[:0]
		if err = chBatch.Send(); err != nil {
			return fmt.Errorf("flushing load batch: %w", err)
		}
		return nil
	}

	defer func() {
		for _, ab := range activeBindings {
			// Free memory on the ClickHouse server
			_ = t.load.conn.Exec(ctx, ab.binding.load.dropTableSQL)
		}
	}()

	for it.Next() {
		ab, found := activeBindings[it.Binding]
		if !found {
			b := t.bindings[it.Binding]
			if err = t.load.conn.Exec(ctx, b.load.createTableSQL); err != nil {
				return fmt.Errorf("creating load stage table: %w", err)
			}
			ab = &activeBinding{b, make([][]any, 0, maxBatchSize)}
			activeBindings[it.Binding] = ab
		}

		converted, err := ab.binding.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting load key: %w", err)
		}
		ab.batch = append(ab.batch, converted)
		if len(ab.batch) >= maxBatchSize {
			if err = flushActiveBinding(ab); err != nil {
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
	for _, ab := range activeBindings {
		if err = flushActiveBinding(ab); err != nil {
			return err
		}
	}

	// Keys are now ready to be JOIN'd between the temporary and target tables.

	loadQueries := make([]string, 0, len(activeBindings))
	for _, ab := range activeBindings {
		loadQueries = append(loadQueries, ab.binding.load.querySQL)
	}
	loadQueryUnionSQL := strings.Join(loadQueries, "\nUNION ALL\n") + ";"
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

	activeBindings := make(map[int]*activeBinding, len(t.bindings))
	flushActiveBinding := func(ab *activeBinding) error {
		if len(ab.batch) == 0 {
			return nil
		}
		chBatch, err := t.store.conn.PrepareBatch(ctx, ab.binding.store.insertSQL)
		if err != nil {
			return fmt.Errorf("preparing store batch: %w", err)
		}
		defer chBatch.Close()
		for _, record := range ab.batch {
			if err = chBatch.Append(record...); err != nil {
				return fmt.Errorf("appending store batch: %w", err)
			}
		}
		ab.batch = ab.batch[:0]
		if err = chBatch.Send(); err != nil {
			return fmt.Errorf("flushing store batch: %w", err)
		}
		return nil
	}

	for it.Next() {
		if it.Delete && t.cfg.HardDelete && !it.Exists {
			continue // nothing to delete if it was never stored
		}

		ab, found := activeBindings[it.Binding]
		if !found {
			b := t.bindings[it.Binding]
			if err = t.store.conn.Exec(ctx, b.store.createTableSQL); err != nil {
				return nil, fmt.Errorf("creating store stage table: %w", err)
			}
			ab = &activeBinding{b, make([][]any, 0, maxBatchSize)}
			activeBindings[it.Binding] = ab
		}

		var converted []any
		converted, err = ab.binding.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store record: %w", err)
		}
		if t.cfg.HardDelete {
			var deleteState uint8 = 0
			if it.Delete {
				deleteState = 1
			}
			converted = append(converted, deleteState)
		}
		ab.batch = append(ab.batch, converted)
		if len(ab.batch) >= maxBatchSize {
			if err = flushActiveBinding(ab); err != nil {
				return nil, err
			}
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	for _, ab := range activeBindings {
		if err = flushActiveBinding(ab); err != nil {
			return nil, err
		}
		t.state[ab.binding.target.StateKey] = &stateItem{
			QueryPartsSQL:    ab.binding.store.queryPartsSQL,
			MovePartitionSQL: ab.binding.store.movePartitionSQL,
			DropTableSQL:     ab.binding.store.dropTableSQL,
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		cs, err := t.state.ToConnectorState(false)
		return cs, pf.FinishedOperation(err)
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	for stateKey, ci := range t.state {
		if err := t.moveStorePartitionsToTarget(ctx, ci); err != nil {
			return nil, fmt.Errorf("moving stage to target: %w", err)
		}
		delete(t.state, stateKey)
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

	return stateClear.ToConnectorState(true)
}

func (t *transactor) moveStorePartitionsToTarget(ctx context.Context, si *stateItem) error {
	rows, err := t.store.conn.Query(ctx, si.QueryPartsSQL, t.cfg.Database)
	if err != nil {
		return fmt.Errorf("querying store table partitions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var partitionID string
		if err = rows.Scan(&partitionID); err != nil {
			return fmt.Errorf("scanning store table partition: %w", err)
		}
		if err = t.store.conn.Exec(ctx, si.MovePartitionSQL, partitionID); err != nil {
			if t.recovery {
				var typedErr *clickhouseproto.Exception
				if errors.As(err, &typedErr) && int(typedErr.Code) == int(chproto.ErrUnknownTable) {
					continue
				}
			}
			return fmt.Errorf("moving store table partition: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("iterating store table partitions: %w", err)
	}

	if err = t.store.conn.Exec(ctx, si.DropTableSQL); err != nil {
		return fmt.Errorf("dropping stage table: %w", err)
	}
	return nil
}

func (t *transactor) Destroy() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for _, b := range t.bindings {
		_ = t.load.conn.Exec(ctx, b.load.dropTableSQL)
	}
	_ = t.store.conn.Close()
	_ = t.load.conn.Close()
}

func main() {
	boilerplate.RunMain(newClickHouseDriver())
}
