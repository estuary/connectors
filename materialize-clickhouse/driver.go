package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=1"`
	Database    string           `json:"database" jsonschema:"title=Database,description=Name of the ClickHouse database to materialize to." jsonschema_extras:"order=2"`
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

func (c config) openDB() *stdsql.DB {
	return clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{c.resolvedAddress()},
		Auth: clickhouse.Auth{
			Database: c.Database,
			Username: c.Credentials.Username,
			Password: c.Credentials.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
}

// openNativeConn returns a clickhouse-go/v2 native connection pool used for both
// Load queries (Query with GroupSet IN parameters) and Store inserts (PrepareBatch).
// Unlike openDB() which returns a database/sql wrapper, the native conn supports
// PrepareBatch() and structured parameter binding via clickhouse.GroupSet.
func (c config) openNativeConn() (chdriver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{c.resolvedAddress()},
		Auth: clickhouse.Auth{
			Database: c.Database,
			Username: c.Credentials.Username,
			Password: c.Credentials.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
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
			var tpls = renderTemplates(dialect)

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     nil, // ClickHouse lacks multi-statement transactions; no fencing.
				NewClient:           newClient,
				CreateTableTemplate: tpls.createTargetTable,
				NewTransactor:       prepareNewTransactor(tpls),
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
	store     struct {
		// Native conn pool for both Load queries and Store PrepareBatch inserts.
		// No session state is needed — each operation acquires a conn and releases.
		conn chdriver.Conn
	}
	bindings []*binding
	be       *m.BindingEvents
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                { return nil }
func (t *transactor) Acknowledge(_ context.Context) (*pf.ConnectorState, error) { return nil, nil }

func prepareNewTransactor(
	tpls templates,
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
		var d = &transactor{dialect: ep.Dialect, templates: tpls, cfg: cfg, be: be}

		// The native conn pool is used for both Load queries (Query) and Store
		// batch inserts (PrepareBatch). No session state is required.
		var err error
		if d.store.conn, err = cfg.openNativeConn(); err != nil {
			return nil, fmt.Errorf("openNativeConn: %w", err)
		}

		for _, b := range bindings {
			if err := d.addBinding(b); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", b.Path, err)
			}
		}

		return d, nil
	}
}

type binding struct {
	target         sql.Table
	loadQuerySQL   string
	storeInsertSQL string
}

func (t *transactor) addBinding(target sql.Table) error {
	var b = &binding{target: target}

	var err error
	if b.loadQuerySQL, err = sql.RenderTableTemplate(target, t.templates.loadQuery); err != nil {
		return fmt.Errorf("rendering load query template: %w", err)
	}
	if b.storeInsertSQL, err = sql.RenderTableTemplate(target, t.templates.storeInsert); err != nil {
		return fmt.Errorf("rendering store insert template: %w", err)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	const batchSize = 1000

	keysByBinding := make(map[int][]clickhouse.GroupSet)
	flushBinding := func(binding int) error {
		groupSets := keysByBinding[binding]
		delete(keysByBinding, binding)
		if len(groupSets) == 0 {
			return nil
		}

		b := t.bindings[binding]
		rows, err := t.store.conn.Query(it.Context(), b.loadQuerySQL, groupSets)
		if err != nil {
			return fmt.Errorf("querying Load documents: %w", err)
		}
		for rows.Next() {
			var document json.RawMessage
			if err := rows.Scan(&document); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scanning Load document: %w", err)
			} else if err := loaded(binding, document); err != nil {
				_ = rows.Close()
				return err
			}
		}
		_ = rows.Close()
		if err := rows.Err(); err != nil {
			return fmt.Errorf("querying Loads: %w", err)
		}
		return nil
	}

	for it.Next() {
		b := t.bindings[it.Binding]
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		keysByBinding[it.Binding] = append(keysByBinding[it.Binding], clickhouse.GroupSet{Value: converted})

		if len(keysByBinding[it.Binding]) >= batchSize {
			if err := flushBinding(it.Binding); err != nil {
				return err
			}
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	for bindingIndex := range keysByBinding {
		if err := flushBinding(bindingIndex); err != nil {
			return err
		}
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	const (
		deleteFalse     uint8 = 0
		deleteTrue      uint8 = 1
		maxBatchRecords       = 100_000
	)

	batchByBinding := make(map[int]chdriver.Batch, 2)
	abortAllBatches := func() {
		for _, batch := range batchByBinding {
			_ = batch.Abort()
		}
	}

	for it.Next() {
		if it.Delete && t.cfg.HardDelete && !it.Exists {
			continue // nothing to delete if it was never stored
		}

		b := t.bindings[it.Binding]
		batch, found := batchByBinding[it.Binding]
		if !found {
			batch, err = t.store.conn.PrepareBatch(it.Context(), b.storeInsertSQL)
			if err != nil {
				abortAllBatches()
				return nil, fmt.Errorf("prepare store batch: %w", err)
			}
			batchByBinding[it.Binding] = batch
		}

		var converted []any
		converted, err = b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			abortAllBatches()
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		var deleteState = deleteFalse
		if it.Delete && t.cfg.HardDelete {
			deleteState = deleteTrue
		}
		converted = append(converted, deleteState)

		if err = batch.Append(converted...); err != nil {
			abortAllBatches()
			return nil, fmt.Errorf("store batch append: %w", err)
		}
		if batch.Rows() >= maxBatchRecords {
			if err = batch.Flush(); err != nil {
				abortAllBatches()
				return nil, fmt.Errorf("flush batch: %w", err)
			}
		}
	}
	if it.Err() != nil {
		abortAllBatches()
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		future := m.RunAsyncOperation(func() error {
			var errs []error
			for _, batch := range batchByBinding {
				if err := batch.Send(); err != nil {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		})

		return nil, future
	}, nil
}

func (t *transactor) Destroy() {
	_ = t.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newClickHouseDriver())
}
