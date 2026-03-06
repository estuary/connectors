package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	m "github.com/estuary/connectors/go/materialize"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"go.gazette.dev/core/consumer/protocol"
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])." jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

const (
	PASSWORD_AUTH_TYPE = "password"
)

type credentialConfig struct {
	AuthType string `json:"auth_type"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

func (c *credentialConfig) Validate() error {
	switch c.AuthType {
	case PASSWORD_AUTH_TYPE:
		if c.User == "" {
			return fmt.Errorf("missing user")
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
	passwordProps := orderedmap.New[string, *jsonschema.Schema]()
	passwordProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: PASSWORD_AUTH_TYPE,
		Const:   PASSWORD_AUTH_TYPE,
	})
	passwordProps.Set("user", &jsonschema.Schema{
		Title:       "User",
		Description: "Database user to connect as.",
		Type:        "string",
		Extras: map[string]any{
			"order": 1,
		},
	})
	passwordProps.Set("password", &jsonschema.Schema{
		Title:       "Password",
		Description: "Password for the specified database user.",
		Type:        "string",
		Extras: map[string]any{
			"secret": true,
			"order":  2,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "ClickHouse credentials.",
		Default:     map[string]string{"auth_type": PASSWORD_AUTH_TYPE},
		Extras: map[string]any{
			"order":         1,
			"discriminator": map[string]string{"propertyName": "auth_type"},
		},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "Password",
				Required:   []string{"auth_type", "user", "password"},
				Properties: passwordProps,
			},
		},
	}
}

type config struct {
	Address     string           `json:"address" jsonschema:"title=Address,description=Host and port of the database (in the form of host[:port]). Port 9000 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=1"`
	Database    string           `json:"database" jsonschema:"title=Database,description=Name of the ClickHouse database to materialize to." jsonschema_extras:"order=2"`
	HardDelete  bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default this is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`

	Advanced      advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
	NetworkTunnel *tunnelConfig  `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
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
	address := c.Address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		address = "localhost:9000"
	}
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
			Username: c.Credentials.User,
			Password: c.Credentials.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})
}

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

func (r tableConfig) WithDefaults(_ config) tableConfig {
	return r
}

func (r tableConfig) Parameters() ([]string, bool, error) {
	return []string{r.Table}, r.Delta, nil
}

func newClickHouseDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-clickhouse",
		StartTunnel: func(ctx context.Context, cfg config) error {
			if cfg.NetworkTunnel != nil && cfg.NetworkTunnel.SshForwarding != nil && cfg.NetworkTunnel.SshForwarding.SshEndpoint != "" {
				host, port, err := net.SplitHostPort(cfg.Address)
				if err != nil {
					host = cfg.Address
					port = "9000"
				}

				var sshConfig = &networkTunnel.SshConfig{
					SshEndpoint: cfg.NetworkTunnel.SshForwarding.SshEndpoint,
					PrivateKey:  []byte(cfg.NetworkTunnel.SshForwarding.PrivateKey),
					ForwardHost: host,
					ForwardPort: port,
					LocalPort:   "9000",
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
				"user":     cfg.Credentials.User,
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
	db        *stdsql.DB
	load      struct {
		conn *stdsql.Conn
	}
	store struct {
		conn *stdsql.Conn
	}
	bindings []*binding
	be       *m.BindingEvents
}

func (t *transactor) UnmarshalState(_ json.RawMessage) error                    { return nil }
func (t *transactor) Acknowledge(_ context.Context) (*pf.ConnectorState, error) { return nil, nil }

func prepareNewTransactor(
	tpls templates,
) func(context.Context, map[string]bool, *sql.Endpoint[config], sql.Fence, []sql.Table, pm.Request_Open, *boilerplate.InfoSchema, *m.BindingEvents) (m.Transactor, error) {
	return func(
		ctx context.Context,
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

		// The transactor maintains its own pool separate from the client's pool because
		// the NewTransactor interface only receives the Endpoint, not the client. This
		// matches the pattern used by other SQL materializations (mysql, postgres).
		d.db = cfg.openDB()
		var err error
		if d.load.conn, err = d.db.Conn(ctx); err != nil {
			return nil, fmt.Errorf("load db.Conn: %w", err)
		}
		if d.store.conn, err = d.db.Conn(ctx); err != nil {
			return nil, fmt.Errorf("store db.Conn: %w", err)
		}

		for _, b := range bindings {
			if err := d.addBinding(ctx, b); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", b.Path, err)
			}
		}

		return d, nil
	}
}

type binding struct {
	target             sql.Table
	createLoadTableSQL string
	loadInsertSQL      string
	loadQuerySQL       string
	truncateLoadSQL    string
	storeInsertSQL     string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var b = &binding{target: target}

	for _, q := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, t.templates.createLoadTable},
		{&b.loadInsertSQL, t.templates.loadInsert},
		{&b.loadQuerySQL, t.templates.loadQuery},
		{&b.truncateLoadSQL, t.templates.truncateLoadTable},
		{&b.storeInsertSQL, t.templates.storeInsert},
	} {
		var err error
		if *q.sql, err = sql.RenderTableTemplate(target, q.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)

	// Create the temp table for load keys.
	if _, err := t.load.conn.ExecContext(ctx, b.createLoadTableSQL); err != nil {
		return fmt.Errorf("creating load table: %w", err)
	}

	return nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	var lastBinding = -1
	var currentTx *stdsql.Tx
	var currentStmt *stdsql.Stmt

	flush := func() error {
		if currentTx == nil {
			return nil
		}
		err := currentTx.Commit()
		currentTx = nil
		currentStmt = nil
		return err
	}

	for it.Next() {
		if lastBinding != it.Binding {
			if err := flush(); err != nil {
				return fmt.Errorf("flushing load batch: %w", err)
			}

			b := t.bindings[it.Binding]
			var err error
			currentTx, err = t.load.conn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("begin load batch: %w", err)
			}
			currentStmt, err = currentTx.Prepare(b.loadInsertSQL)
			if err != nil {
				_ = currentTx.Rollback()
				return fmt.Errorf("prepare load insert: %w", err)
			}
			lastBinding = it.Binding
		}

		var b = t.bindings[it.Binding]
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		if _, err := currentStmt.Exec(converted...); err != nil {
			return fmt.Errorf("load key exec: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Flush the final binding's batch.
	if err := flush(); err != nil {
		return fmt.Errorf("flushing final load batch: %w", err)
	}

	// Build and execute a union query over non-delta bindings.
	var subqueries []string
	for _, b := range t.bindings {
		if b.target.DeltaUpdates {
			continue
		}
		subqueries = append(subqueries, b.loadQuerySQL)
	}

	if len(subqueries) > 0 {
		unionSQL := strings.Join(subqueries, "\nUNION ALL\n") + ";"

		t.be.StartedEvaluatingLoads()
		rows, err := t.load.conn.QueryContext(ctx, unionSQL)
		if err != nil {
			return fmt.Errorf("querying Load documents: %w", err)
		}
		defer rows.Close()
		t.be.FinishedEvaluatingLoads()

		for rows.Next() {
			var bindingIdx int
			var document string

			if err = rows.Scan(&bindingIdx, &document); err != nil {
				return fmt.Errorf("scanning Load document: %w", err)
			} else if err = loaded(bindingIdx, json.RawMessage(document)); err != nil {
				return err
			}
		}
		if err = rows.Err(); err != nil {
			return fmt.Errorf("querying Loads: %w", err)
		}
	}

	// Truncate all temp tables for the next cycle (including delta bindings).
	for _, b := range t.bindings {
		if _, err := t.load.conn.ExecContext(ctx, b.truncateLoadSQL); err != nil {
			return fmt.Errorf("truncating load table: %w", err)
		}
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	ctx := it.Context()
	version := uint64(time.Now().UnixMicro())

	var lastBinding = -1
	var currentTx *stdsql.Tx
	var currentStmt *stdsql.Stmt

	flush := func() error {
		if currentTx == nil {
			return nil
		}
		err := currentTx.Commit()
		currentTx = nil
		currentStmt = nil
		return err
	}

	for it.Next() {
		b := t.bindings[it.Binding]

		if lastBinding != it.Binding {
			if err := flush(); err != nil {
				return nil, fmt.Errorf("flushing store batch: %w", err)
			}

			currentTx, err = t.store.conn.BeginTx(ctx, nil)
			if err != nil {
				return nil, fmt.Errorf("begin store batch: %w", err)
			}
			currentStmt, err = currentTx.Prepare(b.storeInsertSQL)
			if err != nil {
				_ = currentTx.Rollback()
				currentTx = nil
				return nil, fmt.Errorf("prepare store insert: %w", err)
			}
			lastBinding = it.Binding
		}

		var converted []interface{}

		if !b.target.DeltaUpdates && it.Delete && t.cfg.HardDelete {
			if !it.Exists {
				continue
			}
			converted, err = b.target.ConvertKey(it.Key)
			if err != nil {
				return nil, fmt.Errorf("converting delete key: %w", err)
			}
			for range b.target.Values {
				converted = append(converted, nil)
			}
			if b.target.Document != nil {
				converted = append(converted, nil)
			}
			converted = append(converted, version, uint8(1))
		} else {
			converted, err = b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
			if err != nil {
				return nil, fmt.Errorf("converting store parameters: %w", err)
			}
			if !b.target.DeltaUpdates {
				converted = append(converted, version, uint8(0))
			}
		}

		if _, err := currentStmt.Exec(converted...); err != nil {
			if currentTx != nil {
				_ = currentTx.Rollback()
				currentTx = nil
			}
			return nil, fmt.Errorf("store exec: %w", err)
		}
	}
	if it.Err() != nil {
		if currentTx != nil {
			_ = currentTx.Rollback()
		}
		return nil, it.Err()
	}

	// Flush the final binding.
	if err := flush(); err != nil {
		return nil, fmt.Errorf("flushing final store batch: %w", err)
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return nil, m.RunAsyncOperation(func() error {
			return nil // Data already written during Store iteration.
		})
	}, nil
}

func (t *transactor) Destroy() {
	_ = t.load.conn.Close()
	_ = t.store.conn.Close()
	_ = t.db.Close()
}

func main() {
	boilerplate.RunMain(newClickHouseDriver())
}
