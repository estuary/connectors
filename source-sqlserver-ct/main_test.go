package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/estuary/connectors/go/capture/sqlserver/tests"
	mssqldb "github.com/microsoft/go-mssqldb"
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

	os.Exit(runTests(m))
}

// connectorBinary is the path to the connector built by runTests, which every capture in
// this package is pointed at. Empty when tests are skipped for lack of a database.
var connectorBinary string

// runTests builds the connector once and runs the suite against that binary. Each test
// performs many flowctl invocations and each one spawns the connector, so building here
// avoids paying `go run`'s link step hundreds of times over.
//
// Split out from TestMain because os.Exit would skip deferred cleanup of the build.
func runTests(m *testing.M) int {
	if os.Getenv("TEST_DATABASE") == "yes" {
		buildDir, err := os.MkdirTemp("", "source-sqlserver-ct-build-*")
		if err != nil {
			log.WithField("err", err).Error("error creating build directory")
			return 1
		}
		defer os.RemoveAll(buildDir)

		connectorBinary = buildDir + "/connector"
		var build = exec.Command("go", "build", "-o", connectorBinary, ".")
		build.Stderr = os.Stderr
		if err := build.Run(); err != nil {
			log.WithField("err", err).Error("error building connector for tests")
			return 1
		}
	}
	return m.Run()
}

var documentSanitizers = []blackbox.JSONSanitizer{
	// Redact the CT version number in document source metadata
	{Matcher: regexp.MustCompile(`"version":[0-9]+`), Replacement: `"version":"REDACTED"`},
}

var checkpointSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"cursor":{[^}]*}`), Replacement: `"cursor":"REDACTED"`},
}

func blackboxTestSetup(t testing.TB) (*sqlserverTestDatabase, *blackbox.TranscriptCapture) {
	t.Helper()

	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil, nil
	}

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
	tc.Capture.Env["SHUTDOWN_AFTER_POLLING"] = "yes"
	require.NoError(t, tc.Capture.SetLocalCommand(connectorBinary))
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
	connector, err := mssqldb.NewConnector(controlURI)
	require.NoError(t, err)

	// Lose deadlocks deliberately. Tests run concurrently, so one test setting up its
	// tables can deadlock against another test's capture reading that same metadata. The
	// connector under test does not retry those reads, and should not have to for the sake
	// of our test harness, whereas this control connection retries cheaply. Volunteering as
	// the victim keeps the contention on the side which can recover from it.
	connector.SessionInitSQL = "SET DEADLOCK_PRIORITY LOW"

	var conn = sql.OpenDB(connector)
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
	db.execWithDeadlockRetry(t, query)
}

func (db *sqlserverTestDatabase) QuietExec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	db.execWithDeadlockRetry(t, query)
}

// sqlserverDeadlockVictim is the error number SQL Server reports to the session it kills
// to break a deadlock. The victim's work is rolled back, so it just tries again.
const sqlserverDeadlockVictim = 1205

const (
	maxDeadlockRetries = 10
	deadlockRetryDelay = 250 * time.Millisecond
)

// isDeadlockError reports whether err is, or merely reports, SQL Server's deadlock-victim
// error. The error number alone is not enough, because the stored procedures which
// manipulate change tracking metadata catch a deadlock hit during their own work and
// re-raise it under their own error number, quoting the original 1205 in the message.
func isDeadlockError(err error) bool {
	var mssqlErr mssqldb.Error
	if errors.As(err, &mssqlErr) && mssqlErr.Number == sqlserverDeadlockVictim {
		return true
	}
	return strings.Contains(err.Error(), "was deadlocked on lock resources")
}

// execWithDeadlockRetry executes a control query, retrying if this session is chosen as
// the victim of a deadlock. Any other failure is fatal to the test.
//
// Retrying is safe even for statements which aren't idempotent, such as INSERT, because
// being chosen as the deadlock victim rolls the statement back in its entirety.
func (db *sqlserverTestDatabase) execWithDeadlockRetry(t testing.TB, query string) {
	t.Helper()
	for attempt := 1; ; attempt++ {
		var _, err = db.conn.ExecContext(context.Background(), query)
		if err == nil {
			return
		}
		if !isDeadlockError(err) {
			t.Fatalf("error executing control query %q: %v", query, err)
		}
		if attempt >= maxDeadlockRetries {
			t.Fatalf("error executing control query %q: still deadlocking after %d attempts: %v", query, attempt, err)
		}
		// Back off proportionally to the attempt so that a pile-up of contending tests
		// spreads out rather than retrying in lockstep.
		time.Sleep(time.Duration(attempt) * deadlockRetryDelay)
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

	db.QuietExec(t, `IF OBJECT_ID('`+tableName+`', 'U') IS NOT NULL DROP TABLE `+tableName)
	db.Exec(t, `CREATE TABLE `+tableName+` `+defs)

	// Enable Change Tracking for the table. Guarded so that it stays idempotent under the
	// deadlock retry, which can run after an attempt already took effect.
	var ctQuery = fmt.Sprintf(
		`IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables WHERE object_id = OBJECT_ID('%s')) ALTER TABLE %s ENABLE CHANGE_TRACKING`,
		tableName, tableName)
	db.execWithDeadlockRetry(t, ctQuery)

	// Dropping a change-tracked table implicitly disables change tracking for it, so this
	// contends for the same metadata as enabling does.
	t.Cleanup(func() {
		db.execWithDeadlockRetry(t, `IF OBJECT_ID('`+tableName+`', 'U') IS NOT NULL DROP TABLE `+tableName)
	})
}

// CreateTableWithoutCT creates a table without enabling Change Tracking, for tests that need manual control.
func (db *sqlserverTestDatabase) CreateTableWithoutCT(t testing.TB, name, defs string) {
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

// TestSpec verifies the connector's spec output against a snapshot.
func TestSpec(t *testing.T) {
	var _, tc = blackboxTestSetup(t)
	tc.Spec("Get Connector Spec")
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// Interface return type adapter
func genericSetupFunc(t testing.TB) (tests.TestDatabase, *blackbox.TranscriptCapture) {
	return blackboxTestSetup(t)
}

func TestBasics(t *testing.T) {
	tests.TestBasics(t, genericSetupFunc)
}

func TestDatatypes(t *testing.T) {
	tests.TestDatatypes(t, genericSetupFunc)
}

func TestCapture(t *testing.T) {
	tests.TestCapture(t, genericSetupFunc)
}

func TestCollation(t *testing.T) {
	tests.TestCollation(t, genericSetupFunc)
}
