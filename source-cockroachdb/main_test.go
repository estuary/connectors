package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "defaultdb", "Use the named database for tests")

	dbControlAddress = flag.String("db_control_address", "localhost:26257", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "root", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "", "The password for the test setup/control user")

	dbCaptureAddress = flag.String("db_capture_address", "localhost:26257", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "root", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "dummy", "The password for the capture user")
)

const testSchemaName = "test"

// The HLC cursor and event timestamps vary from run to run, so they're redacted from snapshots.
var documentSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"loc":"[0-9.]+"`), Replacement: `"loc":"REDACTED"`},
	{Matcher: regexp.MustCompile(`"ts_ms":[0-9]+`), Replacement: `"ts_ms":"REDACTED"`},
}

var checkpointSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"cursor":"[0-9.]+"`), Replacement: `"cursor":"REDACTED"`},
}

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

func blackboxTestSetup(t testing.TB) (*cockroachdbTestDatabase, *blackbox.TranscriptCapture) {
	t.Helper()

	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	// Shut the capture down once it has caught up, rather than streaming indefinitely.
	os.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	t.Cleanup(func() { os.Unsetenv("SHUTDOWN_AFTER_POLLING") })

	var uniqueID = uniqueTableID(t)
	var baseName = strings.TrimPrefix(t.Name(), "Test") + "_" + uniqueID
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	baseName = strings.ToLower(baseName)
	var fullName = testSchemaName + "." + baseName

	tc, err := blackbox.NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(uniqueID)
	tc.DocumentSanitizers = documentSanitizers
	tc.CheckpointSanitizers = checkpointSanitizers

	var controlURI = fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", *dbControlUser, *dbControlPass, *dbControlAddress, *dbName)
	t.Logf("opening control connection: addr=%q, user=%q", *dbControlAddress, *dbControlUser)
	pool, err := pgxpool.New(context.Background(), controlURI)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Ensure the test schema exists (CockroachDB doesn't create it implicitly).
	_, err = pool.Exec(context.Background(), "CREATE SCHEMA IF NOT EXISTS "+testSchemaName)
	require.NoError(t, err)

	var db = &cockroachdbTestDatabase{
		conn: pool,
		vars: map[string]string{
			"<SCHEMA>": testSchemaName,
			"<NAME>":   fullName,
			"<ID>":     uniqueID,
		},
		transcript: tc.Transcript,
	}
	return db, tc
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

type cockroachdbTestDatabase struct {
	conn       *pgxpool.Pool
	vars       map[string]string
	transcript *strings.Builder
}

func (db *cockroachdbTestDatabase) Expand(s string) string {
	for key, val := range db.vars {
		s = strings.ReplaceAll(s, key, val)
	}
	return s
}

func (db *cockroachdbTestDatabase) Exec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	if db.transcript != nil {
		fmt.Fprintf(db.transcript, "sql> %s\n", query)
	}
	if _, err := db.conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("error executing control query %q: %v", query, err)
	}
}

func (db *cockroachdbTestDatabase) QuietExec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	if _, err := db.conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("error executing control query %q: %v", query, err)
	}
}

func (db *cockroachdbTestDatabase) CreateTable(t testing.TB, name, defs string) {
	t.Helper()
	db.QuietExec(t, `DROP TABLE IF EXISTS `+name)
	db.Exec(t, `CREATE TABLE `+name+` `+defs)
	t.Cleanup(func() { db.QuietExec(t, `DROP TABLE IF EXISTS `+name) })
}
