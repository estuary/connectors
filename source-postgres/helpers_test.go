package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/sirupsen/logrus"
)

var (
	TestConnectionURI = flag.String("test_connection_uri",
		"postgres://flow:flow@localhost:5432/flow",
		"Connect to the specified database in tests")
	TestReplicationSlot = flag.String("test_replication_slot",
		"flow_test_slot",
		"Use the specified replication slot name in tests")
	TestPublicationName = flag.String("test_publication_name",
		"flow_publication",
		"Use the specified publication name in tests")
	TestPollTimeoutSeconds = flag.Float64("test_poll_timeout_seconds",
		0.250, "During test captures, wait at most this long for further replication events")
)

var (
	TestDefaultConfig Config
	TestDatabase      *pgxpool.Pool
	TestBackend       *postgresTestBackend
)

func TestMain(m *testing.M) {
	flag.Parse()
	var ctx = context.Background()

	if testing.Verbose() {
		logrus.SetLevel(logrus.DebugLevel)
	}

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := logrus.ParseLevel(logLevel)
		if err != nil {
			logrus.WithField("level", logLevel).Fatal("invalid log level")
		}
		logrus.SetLevel(level)
	}

	// Open a connection to the database which will be used for creating and
	// tearing down the replication slot.
	var replConnConfig, err = pgconn.ParseConfig(*TestConnectionURI)
	if err != nil {
		logrus.WithFields(logrus.Fields{"uri": *TestConnectionURI, "err": err}).Fatal("error parsing connection config")
	}
	replConnConfig.ConnectTimeout = 30 * time.Second
	replConnConfig.RuntimeParams["replication"] = "database"
	replConn, err := pgconn.ConnectConfig(ctx, replConnConfig)
	if err != nil {
		logrus.WithField("err", err).Fatal("unable to connect to database")
	}
	replConn.Exec(ctx, fmt.Sprintf(`DROP_REPLICATION_SLOT %s;`, *TestReplicationSlot)).Close() // Ignore failures because it probably doesn't exist
	if err := replConn.Exec(ctx, fmt.Sprintf(`CREATE_REPLICATION_SLOT %s LOGICAL pgoutput;`, *TestReplicationSlot)).Close(); err != nil {
		logrus.WithField("err", err).Fatal("error creating replication slot")
	}

	// Initialize test config and database connection
	TestDefaultConfig.Address = fmt.Sprintf("%s:%d", replConnConfig.Host, replConnConfig.Port)
	TestDefaultConfig.Database = replConnConfig.Database
	TestDefaultConfig.User = replConnConfig.User
	TestDefaultConfig.Password = replConnConfig.Password

	TestDefaultConfig.Advanced.SlotName = *TestReplicationSlot
	TestDefaultConfig.Advanced.PublicationName = *TestPublicationName

	if err := TestDefaultConfig.Validate(); err != nil {
		logrus.WithFields(logrus.Fields{"err": err, "config": TestDefaultConfig}).Fatal("error validating test config")
	}
	TestDefaultConfig.SetDefaults()

	pool, err := pgxpool.Connect(ctx, *TestConnectionURI)
	if err != nil {
		logrus.WithField("err", err).Fatal("error connecting to database")
	}
	defer pool.Close()
	TestDatabase = pool
	TestBackend = &postgresTestBackend{pool: pool, cfg: TestDefaultConfig}

	var exitCode = m.Run()
	if err := replConn.Exec(ctx, fmt.Sprintf(`DROP_REPLICATION_SLOT %s;`, *TestReplicationSlot)).Close(); err != nil {
		logrus.WithField("err", err).Fatal("error cleaning up replication slot")
	}
	os.Exit(exitCode)
}

func lowerTuningParameters(t testing.TB) {
	t.Helper()

	// Within the scope of a single test, adjust some tuning parameters so that it's
	// easier to exercise backfill chunking and replication buffering behavior.
	var prevChunkSize = backfillChunkSize
	t.Cleanup(func() { backfillChunkSize = prevChunkSize })
	backfillChunkSize = 16

	var prevBufferSize = replicationBufferSize
	t.Cleanup(func() { replicationBufferSize = prevBufferSize })
	replicationBufferSize = 0
}

type postgresTestBackend struct {
	pool *pgxpool.Pool
	cfg  Config
}

// CreateTable is a test helper for creating a new database table and returning the
// name of the new table. The table is named "test_<testName>", or "test_<testName>_<suffix>"
// if the suffix is non-empty.
func (tb *postgresTestBackend) CreateTable(ctx context.Context, t testing.TB, suffix string, tableDef string) string {
	t.Helper()

	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	for _, str := range []string{"/", "=", "(", ")"} {
		tableName = strings.ReplaceAll(tableName, str, "_")
	}

	logrus.WithFields(logrus.Fields{"table": tableName, "cols": tableDef}).Debug("creating test table")
	tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	tb.Query(ctx, t, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	t.Cleanup(func() {
		logrus.WithField("table", tableName).Debug("destroying test table")
		tb.Query(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	})
	return tableName
}

func (tb *postgresTestBackend) Insert(ctx context.Context, t testing.TB, table string, rows [][]interface{}) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	var tx, err = tb.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("unable to begin transaction: %v", err)
	}
	logrus.WithFields(logrus.Fields{"table": table, "count": len(rows), "first": rows[0]}).Debug("inserting data")
	var query = fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, argsTuple(len(rows[0])))
	for _, row := range rows {
		logrus.WithFields(logrus.Fields{"table": table, "row": row}).Trace("inserting row")
		if len(row) != len(rows[0]) {
			t.Fatalf("incorrect number of values in row %q (expected %d)", row, len(rows[0]))
		}
		var results, err = tx.Query(ctx, query, row...)
		if err != nil {
			t.Fatalf("unable to execute query: %v", err)
		}
		results.Close()
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("unable to commit insert transaction: %v", err)
	}
}

func (tb *postgresTestBackend) Update(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}, setCol string, setVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("UPDATE %s SET %s = $1 WHERE %s = $2;", table, setCol, whereCol), setVal, whereVal)
}

func (tb *postgresTestBackend) Delete(ctx context.Context, t testing.TB, table string, whereCol string, whereVal interface{}) {
	t.Helper()
	tb.Query(ctx, t, fmt.Sprintf("DELETE FROM %s WHERE %s = $1;", table, whereCol), whereVal)
}

func (tb *postgresTestBackend) Query(ctx context.Context, t testing.TB, query string, args ...interface{}) {
	t.Helper()
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	var rows, err = tb.pool.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("unable to execute query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var vals, err = rows.Values()
		if err != nil {
			t.Fatalf("error processing query result: %v", err)
		}
		logrus.WithField("values", vals).Debug("query result row")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error running query: %v", err)
	}
}

func (tb *postgresTestBackend) GetDatabase() sqlcapture.Database {
	var cfg = tb.cfg
	return &postgresDatabase{config: &cfg}
}

func argsTuple(argc int) string {
	var tuple = "($1"
	for idx := 1; idx < argc; idx++ {
		tuple += fmt.Sprintf(",$%d", idx+1)
	}
	return tuple + ")"
}
