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
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/estuary/connectors/go/sqlserver/tests"
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
