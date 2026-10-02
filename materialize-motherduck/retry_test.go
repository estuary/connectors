package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

// duckLakeConflictErr is the error MotherDuck returns when a DuckLake commit
// loses an optimistic concurrency check.
var duckLakeConflictErr = &duckdb.Error{
	Type: duckdb.ErrorTypeTransaction,
	Msg:  "TransactionContext Error: Failed to commit: MotherDuck transaction statement failed: TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.",
}

// injectingConnector opens connections to an embedded in-memory DuckDB whose
// commits fail with the errors in `failures`, in order, one per attempted
// commit. A nil entry, or running out of entries, lets the commit proceed. A
// statement outside an explicit transaction is one commit.
type injectingConnector struct {
	driver.Connector

	mu       sync.Mutex
	failures []error
	commits  int
}

func (c *injectingConnector) Connect(ctx context.Context) (driver.Conn, error) {
	inner, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &injectingConn{inner: inner, c: c}, nil
}

func (c *injectingConnector) inject(failures ...error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures, c.commits = failures, 0
}

func (c *injectingConnector) nextCommit() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.commits++
	if len(c.failures) == 0 {
		return nil
	}
	var err = c.failures[0]
	c.failures = c.failures[1:]
	return err
}

func (c *injectingConnector) attemptedCommits() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.commits
}

type injectingConn struct {
	inner driver.Conn
	c     *injectingConnector
	inTx  bool
}

func (c *injectingConn) Prepare(query string) (driver.Stmt, error) { return c.inner.Prepare(query) }
func (c *injectingConn) Close() error                              { return c.inner.Close() }
func (c *injectingConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *injectingConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.inner.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	c.inTx = true
	return &injectingTx{inner: tx, conn: c}, nil
}

func (c *injectingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if !c.inTx {
		if err := c.c.nextCommit(); err != nil {
			return nil, err
		}
	}
	return c.inner.(driver.ExecerContext).ExecContext(ctx, query, args)
}

func (c *injectingConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return c.inner.(driver.QueryerContext).QueryContext(ctx, query, args)
}

func (c *injectingConn) CheckNamedValue(nv *driver.NamedValue) error {
	return c.inner.(driver.NamedValueChecker).CheckNamedValue(nv)
}

type injectingTx struct {
	inner driver.Tx
	conn  *injectingConn
}

func (t *injectingTx) Commit() error {
	t.conn.inTx = false
	if err := t.conn.c.nextCommit(); err != nil {
		_ = t.inner.Rollback()
		return err
	}
	return t.inner.Commit()
}

func (t *injectingTx) Rollback() error {
	t.conn.inTx = false
	return t.inner.Rollback()
}

func newInjectingClient(t *testing.T) (*client, *injectingConnector) {
	t.Helper()

	inner, err := duckdb.NewConnector("", nil)
	require.NoError(t, err)
	var connector = &injectingConnector{Connector: inner}

	var db = stdsql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })

	return &client{
		db: db,
		ep: &sql.Endpoint[config]{
			Config:  config{Database: "memory", Schema: "main"},
			Dialect: createDuckDialect(nil, true),
		},
	}, connector
}

func queryInt(t *testing.T, c *client, query string) int {
	t.Helper()
	var n int
	require.NoError(t, c.db.QueryRow(query).Scan(&n))
	return n
}

func TestApplyActionsRetryCommitConflicts(t *testing.T) {
	var ctx = context.Background()
	var path = []string{"memory", "main", "target"}

	for _, tc := range []struct {
		name     string
		setup    []string
		failures []error
		apply    func(c *client) error
		check    string
		want     int
	}{
		{
			name:     "create table",
			failures: []error{duckLakeConflictErr},
			apply: func(c *client) error {
				return c.CreateTable(ctx, sql.TableCreate{TableCreateSql: "CREATE TABLE memory.main.target (a INTEGER);"})
			},
			check: "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'target'",
			want:  1,
		},
		{
			name:     "delete table",
			setup:    []string{"CREATE TABLE memory.main.target (a INTEGER);"},
			failures: []error{duckLakeConflictErr},
			apply: func(c *client) error {
				_, fn, err := c.DeleteTable(ctx, path)
				require.NoError(t, err)
				return fn(ctx)
			},
			check: "SELECT count(*) FROM duckdb_tables() WHERE table_name = 'target'",
			want:  0,
		},
		{
			name:     "truncate table",
			setup:    []string{"CREATE TABLE memory.main.target (a INTEGER);", "INSERT INTO memory.main.target VALUES (1), (2);"},
			failures: []error{duckLakeConflictErr},
			apply: func(c *client) error {
				_, fn, err := c.TruncateTable(ctx, path)
				require.NoError(t, err)
				return fn(ctx)
			},
			check: "SELECT count(*) FROM memory.main.target",
			want:  0,
		},
		{
			// The second statement conflicts after the first has committed, so
			// only the second may be retried.
			name:     "alter table",
			setup:    []string{"CREATE TABLE memory.main.target (a INTEGER);"},
			failures: []error{nil, duckLakeConflictErr},
			apply: func(c *client) error {
				_, fn, err := c.AlterTable(ctx, sql.TableAlter{
					Table: sql.Table{Identifier: "memory.main.target"},
					AddColumns: []sql.Column{
						{Identifier: "b", MappedType: sql.MappedType{NullableDDL: "INTEGER"}},
						{Identifier: "c", MappedType: sql.MappedType{NullableDDL: "INTEGER"}},
					},
				})
				require.NoError(t, err)
				return fn(ctx)
			},
			check: "SELECT count(*) FROM duckdb_columns() WHERE table_name = 'target'",
			want:  3,
		},
		{
			name:     "create schema",
			failures: []error{duckLakeConflictErr},
			apply: func(c *client) error {
				_, err := c.CreateSchema(ctx, "other")
				return err
			},
			check: "SELECT count(*) FROM duckdb_schemas() WHERE schema_name = 'other'",
			want:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, connector := newInjectingClient(t)
			for _, stmt := range tc.setup {
				_, err := c.db.Exec(stmt)
				require.NoError(t, err)
			}

			connector.inject(tc.failures...)
			require.NoError(t, tc.apply(c))
			require.Equal(t, tc.want, queryInt(t, c, tc.check))
		})
	}
}

func TestInstallFenceRetriesCommitConflict(t *testing.T) {
	var ctx = context.Background()
	c, connector := newInjectingClient(t)

	checkpoints, err := sql.ResolveTable(*sql.FlowCheckpointsTable([]string{"memory", "main"}), c.ep.Dialect)
	require.NoError(t, err)
	createSQL, err := sql.RenderTableTemplate(checkpoints, renderTemplates(c.ep.Dialect).createTargetTable)
	require.NoError(t, err)
	_, err = c.db.Exec(createSQL)
	require.NoError(t, err)

	connector.inject(duckLakeConflictErr)
	_, err = c.InstallFence(ctx, checkpoints, sql.Fence{
		Materialization: "the/materialization",
		KeyBegin:        0,
		KeyEnd:          ^uint32(0),
		Checkpoint:      []byte("checkpoint"),
	})
	require.NoError(t, err)
	require.Equal(t, 1, queryInt(t, c, fmt.Sprintf("SELECT count(*) FROM %s", checkpoints.Identifier)))
}

func TestCommitConflictRetriesGiveUp(t *testing.T) {
	var ctx = context.Background()
	c, connector := newInjectingClient(t)

	var conflicts []error
	for range 15 {
		conflicts = append(conflicts, duckLakeConflictErr)
	}
	connector.inject(conflicts...)

	err := c.CreateTable(ctx, sql.TableCreate{TableCreateSql: "CREATE TABLE memory.main.target (a INTEGER);"})
	require.ErrorIs(t, err, duckLakeConflictErr)
	require.Equal(t, 10, connector.attemptedCommits())
}

func TestOtherErrorsAreNotRetried(t *testing.T) {
	var ctx = context.Background()
	c, connector := newInjectingClient(t)

	var catalogErr = &duckdb.Error{
		Type: duckdb.ErrorTypeCatalog,
		Msg:  "Catalog Error: Table with name target already exists!",
	}
	connector.inject(catalogErr)

	err := c.CreateTable(ctx, sql.TableCreate{TableCreateSql: "CREATE TABLE memory.main.target (a INTEGER);"})
	require.ErrorIs(t, err, catalogErr)
	require.Equal(t, 1, connector.attemptedCommits())
}
