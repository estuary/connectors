package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/flow"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

var (
	dbName = flag.String("db_name", "postgres", "Use the named database for tests")

	dbControlAddress = flag.String("db_control_address", "localhost:5432", "The database server address to use for test setup/control operations")
	dbControlUser    = flag.String("db_control_user", "postgres", "The user for test setup/control operations")
	dbControlPass    = flag.String("db_control_pass", "postgres", "The password the the test setup/control user")

	dbCaptureAddress = flag.String("db_capture_address", "localhost:5432", "The database server address to use for test captures")
	dbCaptureUser    = flag.String("db_capture_user", "flow_capture", "The user to perform captures as")
	dbCapturePass    = flag.String("db_capture_pass", "secret1234", "The password for the capture user")

	readOnlyCapture = flag.Bool("read_only_capture", false, "When true, run test captures in read-only mode")

	testFeatureFlags = flag.String("feature_flags", "", "Feature flags to apply to all test captures.")
)

const testSchemaName = "test"

var documentSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"txid":[0-9]+`), Replacement: `"txid":"REDACTED"`},
	{Matcher: regexp.MustCompile(`"loc":\[(-1|[0-9]+),[0-9]+,[0-9]+\]`), Replacement: `"loc":"REDACTED"`},
	{Matcher: regexp.MustCompile(`"ts_ms":[0-9]+`), Replacement: `"ts_ms":"REDACTED"`},
}

var checkpointSanitizers = []blackbox.JSONSanitizer{
	{Matcher: regexp.MustCompile(`"cursor":"[0-9A-F]+/[0-9A-F]+"`), Replacement: `"cursor":"REDACTED"`},
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

func postgresBlackboxSetup(t testing.TB) (*postgresTestDatabase, *blackbox.TranscriptCapture) {
	t.Helper()

	// TODO(wgd): Probably scoping this to just flowctl invocations would be cleaner, but this works
	os.Setenv("SHUTDOWN_AFTER_POLLING", "yes")
	t.Cleanup(func() { os.Unsetenv("SHUTDOWN_AFTER_POLLING") })

	// Setup: Unique filter ID and full table name
	var uniqueID = uniqueTableID(t)
	var baseName = strings.TrimPrefix(t.Name(), "Test") + "_" + uniqueID
	for _, str := range []string{"/", "=", "(", ")"} {
		baseName = strings.ReplaceAll(baseName, str, "_")
	}
	baseName = strings.ToLower(baseName)
	var fullName = testSchemaName + "." + baseName

	// Setup: Create black-box test capture
	tc, err := blackbox.NewWithTranscript("testdata/flow.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(uniqueID)
	tc.DocumentSanitizers = documentSanitizers
	tc.CheckpointSanitizers = checkpointSanitizers

	// Setup: Connect to target database
	var controlURI = fmt.Sprintf(`postgres://%s:%s@%s/%s`, *dbControlUser, *dbControlPass, *dbControlAddress, *dbName)
	t.Logf("opening control connection: addr=%q, user=%q", *dbControlAddress, *dbControlUser)
	pool, err := pgxpool.New(context.Background(), controlURI)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	// Setup: Create database interface with <NAME> templating
	var db = &postgresTestDatabase{
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

type postgresTestDatabase struct {
	conn       *pgxpool.Pool     // The control connection to use for test DB operations
	vars       map[string]string // Map of string replacements like <NAME> to a fully qualified table name
	transcript *strings.Builder  // Transcript builder to log SQL query execution to
}

func (db *postgresTestDatabase) Expand(s string) string {
	for key, val := range db.vars {
		s = strings.ReplaceAll(s, key, val)
	}
	return s
}

func (db *postgresTestDatabase) Exec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	if db.transcript != nil {
		fmt.Fprintf(db.transcript, "sql> %s\n", query)
	}
	if _, err := db.conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("error executing control query %q: %v", query, err)
	}
}

func (db *postgresTestDatabase) QuietExec(t testing.TB, query string) {
	t.Helper()
	query = db.Expand(query)
	if _, err := db.conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("error executing control query %q: %v", query, err)
	}
}

// QueryRow executes a query and scans results into dest. Does not log to transcript.
func (db *postgresTestDatabase) QueryRow(t testing.TB, query string, dest ...any) {
	t.Helper()
	query = db.Expand(query)
	if err := db.conn.QueryRow(context.Background(), query).Scan(dest...); err != nil {
		t.Fatalf("error querying %q: %v", query, err)
	}
}

func (db *postgresTestDatabase) CreateTable(t testing.TB, name, defs string) {
	t.Helper()
	db.QuietExec(t, `DROP TABLE IF EXISTS `+name)
	db.Exec(t, `CREATE TABLE `+name+` `+defs)
	t.Cleanup(func() { db.QuietExec(t, `DROP TABLE IF EXISTS `+name) })
}

func setShutdownAfterCaughtUp(t testing.TB, setting bool) {
	t.Helper()
	var prevSetting = sqlcapture.TestShutdownAfterCaughtUp
	sqlcapture.TestShutdownAfterCaughtUp = setting
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = prevSetting })
}

func setResourceBackfillMode(t *testing.T, binding *flow.CaptureSpec_Binding, mode sqlcapture.BackfillMode) {
	t.Helper()
	require.NotNil(t, binding)
	var res sqlcapture.Resource
	require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
	res.Mode = mode
	var bs, err = json.Marshal(&res)
	require.NoError(t, err)
	binding.ResourceConfigJson = bs
}

func TestCapitalizedTables(t *testing.T) {
	var db, tc = postgresBlackboxSetup(t)

	// Create table with quoted capitalized name (outside of template system)
	db.QuietExec(t, `DROP TABLE IF EXISTS "<SCHEMA>"."USERS"`)
	db.Exec(t, `CREATE TABLE "<SCHEMA>"."USERS" (id INTEGER PRIMARY KEY, data TEXT NOT NULL)`)
	t.Cleanup(func() { db.QuietExec(t, `DROP TABLE IF EXISTS "<SCHEMA>"."USERS"`) })

	// Override discovery filter to match "USERS" table
	tc.Capture.DiscoveryFilter = regexp.MustCompile(`(?i:users)`)
	tc.Discover("Discover Tables")
	db.Exec(t, `INSERT INTO "<SCHEMA>"."USERS" VALUES (1, 'Alice'), (2, 'Bob')`)
	tc.Run("Initial Backfill", -1)
	db.Exec(t, `INSERT INTO "<SCHEMA>"."USERS" VALUES (3, 'Carol'), (4, 'Dave')`)
	tc.Run("Replication", -1)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]Config{
		"Basic": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
		},
		"RequireSSL": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
			Advanced: advancedConfig{
				SSLMode: "verify-full",
			},
		},
		"IncorrectSSL": {
			Address:  "example.com",
			User:     "will",
			Password: "secret1234",
			Database: "somedb",
			Advanced: advancedConfig{
				SSLMode: "whoops-this-isnt-right",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			cfg.SetDefaults()
			var uri, err = cfg.ToURI(context.Background())
			require.NoError(t, err)
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", uri, valid))
		})
	}
}
