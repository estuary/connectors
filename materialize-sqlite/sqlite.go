package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	_ "github.com/mattn/go-sqlite3" // Import for register side-effects.
	log "github.com/sirupsen/logrus"
)

type config struct {
	path string
}
func (c config) Validate() error {
	return nil
}

type tableConfig struct {
	Table string `json:"table"`
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected SQLite database configuration `table`")
	}
	return nil
}

func (c tableConfig) GetAdditionalSql() string {
	return ""
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return false // SQLite doesn't support delta updates.
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{}
}

// NewSQLiteDriver creates a new Driver for sqlite.
func NewSQLiteDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://docs.estuary.dev/#FIXME",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, _ json.RawMessage) (*sql.Endpoint, error) {
			var path = "/tmp/sqlite.db"

			// SQLite / go-sqlite3 is a bit fickle about raced opens of a newly created database,
			// often returning "database is locked" errors. We can resolve by ensuring one sql.Open
			// completes before the next starts. This is only required for SQLite, not other drivers.
			sqliteOpenMu.Lock()
			db, err := stdsql.Open("sqlite3", path)
			if err == nil {
				err = db.PingContext(ctx)
			}
			sqliteOpenMu.Unlock()

			if err != nil {
				return nil, fmt.Errorf("opening SQLite database %q: %w", path, err)
			}

			return &sql.Endpoint{
				Config: config{path: path},
				Dialect: sqliteDialect,
				MetaSpecs: nil,
				MetaCheckpoints: nil,
				Client: client{path: path},
				CreateTableTemplate: tplCreateTargetTable,
				NewResource: newTableConfig,
				NewTransactor: newTransactor,
			}, nil
		},
	}
}

type client struct {
	path string
}

// We don't use specs table for sqlite since it is ephemeral and won't be
// persisted between ApplyUpsert and Transactions calls
func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	return "", "", stdsql.ErrNoRows
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
	var db, err = stdsql.Open("sqlite3", c.path)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var d = &transactor{
		dialect: &sqliteDialect,
	}
	d.store.fence = &fence

	var cfg = ep.Config.(config)

	// Establish connections.
	if db, err := stdsql.Open("sqlite3", cfg.path); err != nil {
		return nil, fmt.Errorf("load DB.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load DB.Conn: %w", err)
	}
	if db, err := stdsql.Open("sqlite3", cfg.path); err != nil {
		return nil, fmt.Errorf("store DB.Open: %w", err)
	} else if d.store.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store DB.Conn: %w", err)
	}

	// Attach temporary DB used for staging keys to load.
	if _, err = d.load.conn.ExecContext(ctx, attachSQL); err != nil {
		return nil, fmt.Errorf("Exec(%s): %w", attachSQL, err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding); err != nil {
			return nil, fmt.Errorf("adding binding: %w", err)
		}
	}

	// Build a query which unions the results of each load subquery.
	var subqueries []string
	for _, b := range d.bindings {
		subqueries = append(subqueries, b.load.querySQL)
	}
	d.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	return d, nil
}

type transactor struct {
	dialect *sql.Dialect

	unionSQL string

	// Variables exclusively used by Load.
	load struct {
		conn *stdsql.Conn
		stmt *stdsql.Stmt
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
		fence *sql.Fence
	}
	bindings []*binding
}

type binding struct {
	target sql.Table
	// Variables exclusively used by Load.
	load struct {
		insertSQL string
		querySQL string
		truncateSQL string
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		insertSQL string
		updateSQL string
	}
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var err error
	var b = new(binding)
	b.target = target

	// Build all SQL statements and parameter converters.
	var keyCreateSQL string
	if keyCreateSQL, err = sql.RenderTableTemplate(target, tplCreateLoadTable); err != nil {
		return fmt.Errorf("createLoadTable template: %w", err)
	}
	if b.load.insertSQL, err = sql.RenderTableTemplate(target, tplLoadInsert); err != nil {
		return fmt.Errorf("loadInsert template: %w", err)
	}
	if b.load.querySQL, err = sql.RenderTableTemplate(target, tplLoadQuery); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}
	if b.load.truncateSQL, err = sql.RenderTableTemplate(target, tplLoadTruncate); err != nil {
		return fmt.Errorf("loadTruncate template: %w", err)
	}

	if b.store.insertSQL, err = sql.RenderTableTemplate(target, tplStoreInsert); err != nil {
		return fmt.Errorf("storeInsert template: %w", err)
	}

	if b.store.updateSQL, err = sql.RenderTableTemplate(target, tplStoreUpdate); err != nil {
		return fmt.Errorf("storeUpdate template: %w", err)
	}

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err = t.load.conn.ExecContext(ctx, keyCreateSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", keyCreateSQL, err)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func (d *transactor) Load(
	it *pm.LoadIterator,
	loaded func(int, json.RawMessage) error,
) error {

	var ctx = context.Background()
	// Remove rows left over from the last transaction.
	for _, b := range d.bindings {
		if _, err := d.load.conn.ExecContext(ctx, b.load.truncateSQL); err != nil {
			return fmt.Errorf("truncating Loads: %w", err)
		}
	}

	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if _, err = d.load.conn.ExecContext(ctx, b.load.insertSQL, converted...); err != nil {
			return fmt.Errorf("inserting Load key: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.conn.QueryContext(ctx, d.unionSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document stdsql.RawBytes

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var txn, err = d.store.conn.BeginTx(it.Context(), nil)
	if err != nil {
		return nil, fmt.Errorf("conn.BeginTx: %w", err)
	}

	for it.Next() {
		var ctx = it.Context()
		var b = d.bindings[it.Binding]

		if it.Exists {
			if converted, err := b.target.ConvertKey(it.Key); err != nil {
				return nil, fmt.Errorf("converting update store key: %w", err)
			} else if _, err = txn.ExecContext(ctx, b.store.updateSQL, converted...); err != nil {
				return nil, fmt.Errorf("updating store: %w", err)
			}
		} else {
			if converted, err := b.target.ConvertKey(it.Key); err != nil {
				return nil, fmt.Errorf("converting update store key: %w", err)
			} else if _, err = txn.ExecContext(ctx, b.store.insertSQL, converted...); err != nil {
				return nil, fmt.Errorf("updating store: %w", err)
			}
		}
	}

	return func(ctx context.Context, runtimeCheckpoint []byte, _ <-chan struct{}) (*pf.DriverCheckpoint, pf.OpFuture) {
		d.store.fence.Checkpoint = runtimeCheckpoint

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("evaluating fence template: %w", err))
		}

		return nil, pf.RunAsyncOperation(func() error {
			if result, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating fence: %w", err)
			} else if rowsAffected, err := result.RowsAffected(); err != nil {
				return fmt.Errorf("result.RowsAffected: %w", err)
			} else if rowsAffected == 0 {
				return fmt.Errorf("this instance is fenced off by another")
			}

			if err = txn.Commit(); err != nil {
				return fmt.Errorf("commit transaction: %w", err)
			}

			return nil
		})
	}, nil
}

// RuntimeCommitted is a no-op since the SQLite database is authoritative.
func (d *transactor) RuntimeCommitted(context.Context) error { return nil }

func (d *transactor) Destroy() {
	if err := d.load.conn.Close(); err != nil {
		log.WithField("err", err).Error("failed to close load connection")
	}
	if err := d.store.conn.Close(); err != nil {
		log.WithField("err", err).Error("failed to close store connection")
	}
}


func main() {
	boilerplate.RunMain(NewSQLiteDriver())
}

var sqliteOpenMu sync.Mutex
