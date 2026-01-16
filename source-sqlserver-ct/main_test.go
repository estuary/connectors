package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "test", "Connect to the named database for tests")

	dbControlAddress = flag.String("db_control_addr", "127.0.0.1:1433", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "sa", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "gf6w6dkD", "The password the the test setup/control user")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")

	testSchemaName = flag.String("test_schema_name", "dbo", "The schema in which to create test tables.")
)

func TestMain(m *testing.M) {
	flag.Parse()

	if logLevel := os.Getenv("LOG_LEVEL"); logLevel != "" {
		level, err := log.ParseLevel(logLevel)
		if err != nil {
			log.WithField("level", logLevel).Fatal("invalid log level")
		}
		log.SetLevel(level)
	}

	// Set a 900MiB memory limit, same as we use in production.
	debug.SetMemoryLimit(900 * 1024 * 1024)

	os.Exit(m.Run())
}

var documentSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"lsn":"[0-9A-Za-z+/=]+"`), Replacement: `"lsn":"REDACTED"`},
	{Matcher: regexp.MustCompile(`"seqval":"[0-9A-Za-z+/=]+"`), Replacement: `"seqval":"REDACTED"`},
}

var checkpointSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"cursor":"[0-9A-Za-z+/=]+"`), Replacement: `"cursor":"REDACTED"`},
	{Matcher: regexp.MustCompile(`"lsn":"[0-9A-Za-z+/=]+"`), Replacement: `"lsn":"REDACTED"`},
	{Matcher: regexp.MustCompile(`"seqval":"[0-9A-Za-z+/=]+"`), Replacement: `"seqval":"REDACTED"`},
}

func blackboxTestSetup(t testing.TB) (*sqlserverTestDatabase, *blackbox.TranscriptCapture) {
	t.Helper()

	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil, nil
	}

	// TODO(wgd): Probably scoping this to just flowctl invocations would be cleaner, but this works
	os.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	t.Cleanup(func() { os.Unsetenv("SHUTDOWN_AFTER_POLLING") })

	// Setup: Unique filter ID and full table name
	var uniqueID = uniqueTableID(t)
	var baseName = strings.TrimPrefix(t.Name(), "Test") + "_" + uniqueID
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	var fullName = *testSchemaName + "." + baseName

	// Setup: Create black-box test capture
	tc, err := blackbox.NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(uniqueID)
	tc.DocumentSanitizers = documentSanitizers
	tc.CheckpointSanitizers = checkpointSanitizers

	// Setup: Connect to target database
	var controlURI = (&Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	t.Logf("opening control connection: addr=%q, user=%q", *dbControlAddress, *dbControlUser)
	conn, err := sql.Open("sqlserver", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// Setup: Create database interface with <NAME> templating
	var db = &sqlserverTestDatabase{
		conn: conn,
		vars: map[string]string{
			"<SCHEMA>": *testSchemaName,
			"<NAME>":   fullName,
			"<ID>":     uniqueID,
		},
		transcript: tc.Transcript,
	}

	return db, tc
}

type sqlserverTestDatabase struct {
	conn       *sql.DB           // The control connection to use for test DB operations
	vars       map[string]string // Map of string replacements like <NAME> to a fully qualified table name
	transcript *strings.Builder  // Transcript builder to log SQL query execution to
}

func (db *sqlserverTestDatabase) Expand(s string) string {
	for key, val := range db.vars {
		s = strings.ReplaceAll(s, key, val)
	}
	return s
}

func (db *sqlserverTestDatabase) Exec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	if db.transcript != nil {
		fmt.Fprintf(db.transcript, "sql> %s\n", query)
	}
	if _, err := db.conn.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("error executing control query %q: %v", query, err)
	}
}

func (db *sqlserverTestDatabase) QuietExec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	if _, err := db.conn.ExecContext(context.Background(), query); err != nil {
		t.Fatalf("error executing control query %q: %v", query, err)
	}
}

// QueryRow executes a query and scans results into dest. Does not log to transcript.
func (db *sqlserverTestDatabase) QueryRow(t testing.TB, query string, dest ...any) {
	t.Helper()
	query = db.Expand(query)
	if err := db.conn.QueryRowContext(context.Background(), query).Scan(dest...); err != nil {
		t.Fatalf("error querying %q: %v", query, err)
	}
}

func (db *sqlserverTestDatabase) CreateTable(t testing.TB, name, defs string) {
	t.Helper()
	name = db.Expand(name)
	var tableName = name
	// Extract just the table name (without schema) for CDC enablement
	var parts = strings.Split(name, ".")
	var schema, shortName string
	if len(parts) == 2 {
		schema, shortName = parts[0], parts[1]
	} else {
		schema, shortName = *testSchemaName, name
	}

	db.QuietExec(t, `IF OBJECT_ID('`+tableName+`', 'U') IS NOT NULL DROP TABLE `+tableName)
	db.Exec(t, `CREATE TABLE `+tableName+` `+defs)

	// Enable CDC for the table
	time.Sleep(1 * time.Second) // Sleep to make deadlocks less likely
	var cdcQuery = fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = '%s'`,
		schema, shortName, *dbCaptureUser)
	db.QuietExec(t, cdcQuery)

	t.Cleanup(func() { db.QuietExec(t, `IF OBJECT_ID('`+tableName+`', 'U') IS NOT NULL DROP TABLE `+tableName) })
}

// CreateTableWithoutCDC creates a table without enabling CDC, for tests that need manual control.
func (db *sqlserverTestDatabase) CreateTableWithoutCDC(t testing.TB, name, defs string) {
	t.Helper()
	name = db.Expand(name)
	db.QuietExec(t, `IF OBJECT_ID('`+name+`', 'U') IS NOT NULL DROP TABLE `+name)
	db.Exec(t, `CREATE TABLE `+name+` `+defs)
	t.Cleanup(func() { db.QuietExec(t, `IF OBJECT_ID('`+name+`', 'U') IS NOT NULL DROP TABLE `+name) })
}

func uniqueTableID(t testing.TB, extra ...string) string {
	t.Helper()
	var h = sha256.New()
	h.Write([]byte(t.Name()))
	for _, x := range extra {
		h.Write([]byte{':'})
		h.Write([]byte(x))
	}
	var x = binary.BigEndian.Uint32(h.Sum(nil)[0:4])
	return fmt.Sprintf("%d", (x%900000)+100000)
}

func setShutdownAfterCaughtUp(t testing.TB, setting bool) {
	t.Helper()
	var prevSetting = sqlcapture.TestShutdownAfterCaughtUp
	sqlcapture.TestShutdownAfterCaughtUp = setting
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = prevSetting })
}

func TestColumnNameQuoting(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `([id] INTEGER, [data] INTEGER, [CAPITALIZED] INTEGER, [unique] INTEGER, [type] INTEGER, PRIMARY KEY ([id], [data], [CAPITALIZED], [unique], [type]))`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 0, 0, 0, 0), (1, 1, 1, 1, 1), (2, 2, 2, 2, 2)`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestTextCollation(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(8) PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES ('AAA', '1'), ('BBB', '2'), ('-J C', '3'), ('H R', '4')`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestDiscoveryIrrelevantConstraints verifies that discovery works correctly
// even when there are other non-primary-key constraints on a table.
func TestDiscoveryIrrelevantConstraints(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id VARCHAR(8) PRIMARY KEY, foo INTEGER UNIQUE, data TEXT)`)
	tc.DiscoverFull("Discover Tables")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestUUIDCaptureOrder(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id UNIQUEIDENTIFIER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES
		('00ffffff-ffff-ffff-ffff-ffffffffffff', 'sixteen'),
		('ff00ffff-ffff-ffff-ffff-ffffffffffff', 'fifteen'),
		('ffff00ff-ffff-ffff-ffff-ffffffffffff', 'fourteen'),
		('ffffff00-ffff-ffff-ffff-ffffffffffff', 'thirteen'),
		('ffffffff-00ff-ffff-ffff-ffffffffffff', 'twelve'),
		('ffffffff-ff00-ffff-ffff-ffffffffffff', 'eleven'),
		('ffffffff-ffff-00ff-ffff-ffffffffffff', 'ten'),
		('ffffffff-ffff-ff00-ffff-ffffffffffff', 'nine'),
		('ffffffff-ffff-ffff-00ff-ffffffffffff', 'seven'),
		('ffffffff-ffff-ffff-ff00-ffffffffffff', 'eight'),
		('ffffffff-ffff-ffff-ffff-00ffffffffff', 'one'),
		('ffffffff-ffff-ffff-ffff-ff00ffffffff', 'two'),
		('ffffffff-ffff-ffff-ffff-ffff00ffffff', 'three'),
		('ffffffff-ffff-ffff-ffff-ffffff00ffff', 'four'),
		('ffffffff-ffff-ffff-ffff-ffffffff00ff', 'five'),
		('ffffffff-ffff-ffff-ffff-ffffffffff00', 'six')`)
	tc.Discover("Discover Tables")
	tc.Run("Capture", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestManyTables(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var db, tc = blackboxTestSetup(t)

	// Create 20 tables
	for i := 0; i < 20; i++ {
		db.CreateTable(t, fmt.Sprintf(`<NAME>_%03d`, i), `(id INTEGER PRIMARY KEY, data TEXT)`)
	}

	// Insert initial data
	for i := 0; i < 20; i++ {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME>_%03d VALUES (0, 'table %d row zero'), (1, 'table %d row one')`, i, i, i))
	}
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)

	// More inserts
	for i := 0; i < 20; i++ {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME>_%03d VALUES (2, 'table %d row two'), (3, 'table %d row three')`, i, i, i))
	}
	tc.Run("Replication 1", -1)

	// Partial inserts
	for i := 0; i < 10; i++ {
		db.Exec(t, fmt.Sprintf(`INSERT INTO <NAME>_%03d VALUES (4, 'table %d row four'), (5, 'table %d row five')`, i, i, i))
	}
	tc.Run("Replication 2", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestCaptureInstanceCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// Lower the cleanup interval for the purposes of speedy test execution
	var oldInterval = cdcCleanupInterval
	t.Cleanup(func() { cdcCleanupInterval = oldInterval })
	cdcCleanupInterval = 5 * time.Second

	for _, testCase := range []struct {
		Name    string
		WithDBO bool
	}{
		{"WithDBO", true},
		{"WithoutDBO", false},
	} {
		t.Run(testCase.Name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)

			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
			require.NoError(t, tc.Capture.EditConfig("advanced.change_table_cleanup", true))

			// The db_owner permission is required to automatically clean up change tables
			if testCase.WithDBO {
				db.QuietExec(t, `ALTER ROLE db_owner ADD MEMBER flow_capture`)
				t.Cleanup(func() { db.QuietExec(t, `ALTER ROLE db_owner DROP MEMBER flow_capture`) })
			}

			// Initial backfill
			tc.Discover("Discover Tables")
			tc.Run("Initial Backfill", -1)

			// For the ongoing capture, we need to disable SHUTDOWN_AFTER_POLLING
			os.Unsetenv("SHUTDOWN_AFTER_POLLING")

			// Launch the ongoing capture in a background goroutine
			var captureCtx, cancelCapture = context.WithCancel(context.Background())
			defer cancelCapture()
			var captureDone atomic.Bool
			var captureErr error
			go func() {
				_, captureErr = tc.Capture.RunWithContext(captureCtx, -1)
				captureDone.Store(true)
			}()

			// Traffic generator - insert rows over time
			for i := 0; i < 10; i++ {
				db.QuietExec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES (%d, 'iteration %d row one'), (%d, 'iteration %d row two'), (%d, 'iteration %d row three'), (%d, 'iteration %d row four')`,
					i*100+1, i, i*100+2, i, i*100+3, i, i*100+4, i))
				time.Sleep(1 * time.Second)
			}

			// Wait for cleanup interval to pass
			time.Sleep(cdcCleanupInterval + 2*time.Second)

			// Cancel the capture and wait for it to finish
			cancelCapture()
			for !captureDone.Load() {
				time.Sleep(100 * time.Millisecond)
			}

			// Context cancellation is expected, not an error
			if captureErr != nil && !strings.Contains(captureErr.Error(), "context canceled") {
				t.Logf("capture error (may be expected): %v", captureErr)
			}

			// Query the number of rows in the change table
			var schemaName = *testSchemaName
			var tableName = db.Expand(`<NAME>`)
			var shortName = tableName[len(schemaName)+1:]
			var changeTableName = fmt.Sprintf("cdc.%s_%s_CT", schemaName, shortName)
			var changeTableRows int
			db.QueryRow(t, fmt.Sprintf("SELECT COUNT(*) FROM %s", changeTableName), &changeTableRows)

			// Add the change table row count to transcript
			fmt.Fprintf(tc.Transcript, "# Change Table Row Count: %d\n", changeTableRows)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

func TestAlterationAddColumn(t *testing.T) {
	for _, testCase := range []struct {
		Name     string
		Manual   bool
		Automate bool
	}{
		{"Off", false, false},
		{"Manual", true, false},
		{"Automatic", false, true},
	} {
		t.Run(testCase.Name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)

			// Grant db_owner role required for capture instance automation
			db.QuietExec(t, `ALTER ROLE db_owner ADD MEMBER flow_capture`)
			t.Cleanup(func() { db.QuietExec(t, `ALTER ROLE db_owner DROP MEMBER flow_capture`) })

			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
			if testCase.Automate {
				require.NoError(t, tc.Capture.EditConfig("advanced.capture_instance_management", true))
			}

			// Initial backfill
			db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
			tc.Discover("Discover Tables")
			tc.Run("Initial Backfill", -1)

			// Add first column
			db.Exec(t, `ALTER TABLE <NAME> ADD extra TEXT`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two', 'aaa'), (3, 'three', 'bbb')`)
			time.Sleep(1 * time.Second) // Let alteration propagate to cdc.ddl_history
			if testCase.Manual {
				// Manually create new capture instance for the altered schema
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = 'flow_capture', @capture_instance = '%s_%s_v2'`,
					schemaName, shortName, schemaName, shortName))
				time.Sleep(1 * time.Second)
			}
			tc.Run("After First Column Add", -1)

			db.Exec(t, `INSERT INTO <NAME> VALUES (4, 'four', 'ccc'), (5, 'five', 'ddd')`)
			tc.Run("More Inserts After First Add", -1)

			if testCase.Manual {
				// Disable old capture instance
				time.Sleep(1 * time.Second)
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = '%s_%s'`,
					schemaName, shortName, schemaName, shortName))
				time.Sleep(1 * time.Second)
			}

			// Add second column
			db.Exec(t, `ALTER TABLE <NAME> ADD evenmore TEXT`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six', 'eee', 'foo'), (7, 'seven', 'fff', 'bar')`)
			time.Sleep(1 * time.Second)
			if testCase.Manual {
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = 'flow_capture', @capture_instance = '%s_%s_v3'`,
					schemaName, shortName, schemaName, shortName))
				time.Sleep(1 * time.Second)
			}
			tc.Run("After Second Column Add", -1)

			db.Exec(t, `INSERT INTO <NAME> VALUES (8, 'eight', 'ggg', 'baz'), (9, 'nine', 'hhh', 'asdf')`)
			tc.Run("Final Inserts", -1)

			if testCase.Manual {
				time.Sleep(1 * time.Second)
				var schemaName = *testSchemaName
				var tableName = db.Expand(`<NAME>`)
				var shortName = tableName[len(schemaName)+1:]
				db.QuietExec(t, fmt.Sprintf(`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = '%s_%s_v2'`,
					schemaName, shortName, schemaName, shortName))
			}

			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

func TestDeletedTextColumn(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, v_text TEXT NOT NULL, v_varchar VARCHAR(32), v_int INTEGER)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero', 'zero', 100), (1, 'one', 'one', 101)`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two', 'two', 102), (3, 'three', 'three', 103)`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 1`)
	db.Exec(t, `DELETE FROM <NAME> WHERE id = 2`)
	tc.Run("Replication With Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestComputedColumn(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, a VARCHAR(32), b VARCHAR(32), computed AS ISNULL(a, ISNULL(b, 'default')))`)
	tc.DiscoverFull("Discover Table Schema")
	db.Exec(t, `INSERT INTO <NAME> (id, a, b) VALUES (0, 'a0', 'b0'), (1, NULL, 'b1'), (2, 'a2', NULL), (3, NULL, NULL)`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> (id, a, b) VALUES (4, 'a4', 'b4'), (5, NULL, 'b5'), (6, 'a6', NULL), (7, NULL, NULL)`)
	tc.Run("Replication Inserts", -1)
	db.Exec(t, `UPDATE <NAME> SET a = 'a4-modified' WHERE id = 4`)
	db.Exec(t, `UPDATE <NAME> SET b = 'b5-modified' WHERE id = 5`)
	db.Exec(t, `UPDATE <NAME> SET a = 'a6-modified' WHERE id = 6`)
	tc.Run("Updates", -1)
	db.Exec(t, `DELETE FROM <NAME> WHERE id IN (4, 5, 6, 7)`)
	tc.Run("Deletes", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestDroppedAndRecreatedTable(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Some Replication", -1)

	// Drop and recreate
	db.Exec(t, `DROP TABLE <NAME>`)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (6, 'six'), (7, 'seven'), (8, 'eight')`)
	tc.Run("After Drop/Recreate", -1)

	db.Exec(t, `INSERT INTO <NAME> VALUES (9, 'nine'), (10, 'ten'), (11, 'eleven')`)
	tc.Run("More Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestFilegroupAndRole(t *testing.T) {
	var db, tc = blackboxTestSetup(t)

	// Create table without CDC (connector will create CDC instance with specified config)
	db.CreateTableWithoutCDC(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)

	// Grant db_owner so connector can create CDC instance
	db.Exec(t, `ALTER ROLE db_owner ADD MEMBER flow_capture`)
	t.Cleanup(func() { db.QuietExec(t, `ALTER ROLE db_owner DROP MEMBER flow_capture`) })

	require.NoError(t, tc.Capture.EditConfig("advanced.filegroup", "PRIMARY"))
	require.NoError(t, tc.Capture.EditConfig("advanced.role_name", "flow_capture"))
	tc.Discover("Discover Tables")

	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestPrimaryKeyUpdate(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Some Replication", -1)
	db.Exec(t, `UPDATE <NAME> SET id = 6 WHERE id = 1`)
	db.Exec(t, `UPDATE <NAME> SET id = 7 WHERE id = 4`)
	tc.Run("Primary Key Updates", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestComputedPrimaryKey(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(actual_id INTEGER NOT NULL, data VARCHAR(32), id AS actual_id PRIMARY KEY)`)
	tc.DiscoverFull("Discover Table Schema")
	db.Exec(t, `INSERT INTO <NAME> (actual_id, data) VALUES (0, 'zero'), (1, 'one'), (2, 'two')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> (actual_id, data) VALUES (3, 'three'), (4, 'four'), (5, 'five')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestFeatureFlagEmitSourcedSchemas runs a capture with the `emit_sourced_schemas` feature flag set.
func TestFeatureFlagEmitSourcedSchemas(t *testing.T) {
	for _, testCase := range []struct {
		name string
		flag string
	}{
		{"Default", ""},
		{"Enabled", "emit_sourced_schemas"},
		{"Disabled", "no_emit_sourced_schemas"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var db, tc = blackboxTestSetup(t)
			db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data VARCHAR(32))`)
			db.Exec(t, `INSERT INTO <NAME> VALUES (1, 'hello'), (2, 'world')`)
			if testCase.flag != "" {
				require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", testCase.flag))
			}
			tc.Discover("Discover Tables")
			tc.Run("Capture", -1)
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

// TestSourceTag verifies the output of a capture with /advanced/source_tag set
func TestSourceTag(t *testing.T) {
	var db, tc = blackboxTestSetup(t)
	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY, data TEXT)`)
	require.NoError(t, tc.Capture.EditConfig("advanced.source_tag", "example_source_tag_1234"))
	db.Exec(t, `INSERT INTO <NAME> VALUES (0, 'zero'), (1, 'one')`)
	tc.Discover("Discover Tables")
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO <NAME> VALUES (2, 'two'), (3, 'three')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}
