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
	"sync"
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
		buildDir, err := os.MkdirTemp("", "source-sqlserver-build-*")
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

		stopScanner, err := startManualCDCScanner()
		if err != nil {
			log.WithField("err", err).Error("error taking over CDC log scanning")
			return 1
		}
		defer stopScanner()
	}
	return m.Run()
}

// startManualCDCScanner stops the SQL Agent capture job and drives log scanning from the
// test harness instead, returning a function which restores the job.
//
// Every stream-to-fence waits for a log scan session which completed after the fence
// attempt began, so how often scans complete is a hard floor on how fast a capture reaches
// a fence, and the suite pays that floor hundreds of times over. The capture job cannot
// scan more than once a second, so drive the scans ourselves instead. This is invisible to
// the connector, which only observes completed entries in sys.dm_cdc_log_scan_sessions and
// the resulting maximum LSN.
//
// Only pays off alongside ${TEST_FENCE_RETRY_INTERVAL}, which blackboxTestSetup sets:
// frequent scans make a fence available sooner, but the connector's default one second
// poll still quantizes every fence to a one second boundary.
func startManualCDCScanner() (func(), error) {
	var controlURI = (&Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	conn, err := sql.Open("sqlserver", controlURI)
	if err != nil {
		return nil, fmt.Errorf("opening scanner connection: %w", err)
	}

	if _, err := conn.ExecContext(context.Background(), `EXEC sys.sp_cdc_stop_job @job_type = 'capture'`); err != nil {
		conn.Close()
		return nil, fmt.Errorf("stopping capture job: %w", err)
	}

	// sp_cdc_stop_job only requests the stop, so wait for the job to actually leave the
	// running state: sp_cdc_scan refuses to run alongside it.
	const jobRunningQuery = `SELECT TOP 1 CASE WHEN ja.start_execution_date IS NOT NULL AND ja.stop_execution_date IS NULL THEN 1 ELSE 0 END ` +
		`FROM msdb.dbo.sysjobs j JOIN msdb.dbo.sysjobactivity ja ON ja.job_id = j.job_id ` +
		`WHERE j.name LIKE '%capture%' ORDER BY ja.session_id DESC`
	var deadline = time.Now().Add(60 * time.Second)
	for {
		var running int
		if err := conn.QueryRowContext(context.Background(), jobRunningQuery).Scan(&running); err != nil {
			conn.Close()
			return nil, fmt.Errorf("querying capture job state: %w", err)
		}
		if running == 0 {
			break
		}
		if time.Now().After(deadline) {
			conn.Close()
			return nil, fmt.Errorf("capture job still running 60s after being asked to stop")
		}
		time.Sleep(250 * time.Millisecond)
	}

	var scannerCtx, cancelScanner = context.WithCancel(context.Background())
	var scannerDone = make(chan struct{})
	go func() {
		defer close(scannerDone)
		for scannerCtx.Err() == nil {
			// The transaction limit has to match what msdb.dbo.cdc_jobs advertises, because
			// the connector reads that value and treats a scan session which hit the limit
			// as one which may not have caught up.
			//
			// Deliberately not tied to scannerCtx: cancelling mid-scan would abort a scan
			// the connector may already be waiting on, and a scan only takes a moment.
			if _, err := conn.ExecContext(context.Background(),
				`EXEC sys.sp_cdc_scan @maxtrans = 500, @maxscans = 1, @continuous = 0`); err != nil && scannerCtx.Err() == nil {
				log.WithField("err", err).Warn("manual CDC log scan failed")
			}
			select {
			case <-scannerCtx.Done():
			case <-time.After(50 * time.Millisecond):
			}
		}
	}()

	return func() {
		cancelScanner()
		<-scannerDone
		if _, err := conn.ExecContext(context.Background(), `EXEC sys.sp_cdc_start_job @job_type = 'capture'`); err != nil {
			log.WithField("err", err).Warn("error restarting capture job")
		}
		conn.Close()
	}, nil
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

// blackboxTestSetup prepares an isolated capture for a test and marks that test as safe
// to run concurrently with others. Isolation comes from the table names and discovery
// filter both deriving from the test's own name, so tests neither collide on tables nor
// observe each other's.
func blackboxTestSetup(t testing.TB) (*sqlserverTestDatabase, *blackbox.TranscriptCapture) {
	t.Helper()
	if tp, ok := t.(*testing.T); ok {
		tp.Parallel()
	}
	return blackboxTestSetupSerial(t)
}

// blackboxTestSetupSerial is for the minority of tests which alter database-wide state,
// such as role membership or server configuration, and so must not overlap with anything
// else. Go defers every parallel test until the sequential ones have finished, so these
// get the database to themselves.
func blackboxTestSetupSerial(t testing.TB) (*sqlserverTestDatabase, *blackbox.TranscriptCapture) {
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
	// Pointless without the manual scanner started in runTests, and vice versa: see the
	// comment there for why both halves are needed to shorten a fence wait.
	tc.Capture.Env["TEST_FENCE_RETRY_INTERVAL"] = "100ms"
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
// error. The error number alone is not enough: sp_cdc_enable_table catches a deadlock hit
// during its own work and re-raises it as error 22834, quoting the original 1205 only in
// the message text.
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
// being chosen as the deadlock victim rolls the statement back in its entirety. The
// exception is a stored procedure which catches the deadlock internally and returns its own
// error after leaving work behind, which is why enableCDC needs its own retry.
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

// cdcMetadataLock serializes the operations which mutate CDC metadata. SQL Server already
// serializes these internally, via an exclusive applock held against the 'cdc' principal,
// so performing them concurrently gains nothing: it only converts the wait into deadlocks
// and lock starvation which retrying does not reliably clear. Tests still run in parallel,
// they just queue for this one step.
var cdcMetadataLock sync.Mutex

// sqlserverCaptureInstanceExists is reported when sp_cdc_enable_table is asked to create a
// capture instance which already exists. That is the state a partially-failed earlier
// attempt leaves behind: the capture instance gets created but the table is not marked as
// tracked, so the operation half-happened.
const sqlserverCaptureInstanceExists = 22926

func isCaptureInstanceExistsError(err error) bool {
	var mssqlErr mssqldb.Error
	if errors.As(err, &mssqlErr) && mssqlErr.Number == sqlserverCaptureInstanceExists {
		return true
	}
	return strings.Contains(err.Error(), "already exists in the current database")
}

// enableCDC turns on CDC for a table. Enabling CDC, and dropping a table which has it
// enabled, both take locks on the shared CDC metadata tables, so tests doing either
// concurrently can deadlock.
//
// This needs its own retry rather than execWithDeadlockRetry because sp_cdc_enable_table
// is not idempotent, so each retry has to clear the partial state a failed attempt leaves
// behind before trying again.
func (db *sqlserverTestDatabase) enableCDC(t testing.TB, schema, name string) {
	t.Helper()
	cdcMetadataLock.Lock()
	defer cdcMetadataLock.Unlock()

	var enable = fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = '%s', @source_name = '%s', @role_name = '%s'`,
		schema, name, *dbCaptureUser)
	for attempt := 1; ; attempt++ {
		var _, err = db.conn.ExecContext(context.Background(), enable)
		if err == nil {
			return
		}
		if !isDeadlockError(err) && !isCaptureInstanceExistsError(err) {
			t.Fatalf("error enabling CDC for %s.%s: %v", schema, name, err)
		}
		if attempt >= maxDeadlockRetries {
			t.Fatalf("error enabling CDC for %s.%s: giving up after %d attempts: %v", schema, name, attempt, err)
		}
		time.Sleep(time.Duration(attempt) * deadlockRetryDelay)
		db.dropCaptureInstances(t, schema, name)
	}
}

// dropCaptureInstances removes any capture instances for a table, tolerating there being
// none. This clears the leftovers of a partially-failed sp_cdc_enable_table so that the
// next attempt starts from a clean slate.
func (db *sqlserverTestDatabase) dropCaptureInstances(t testing.TB, schema, name string) {
	t.Helper()
	var query = fmt.Sprintf(`IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE source_object_id = OBJECT_ID('%s.%s')) `+
		`EXEC sys.sp_cdc_disable_table @source_schema = '%s', @source_name = '%s', @capture_instance = 'all'`,
		schema, name, schema, name)
	if _, err := db.conn.ExecContext(context.Background(), query); err != nil {
		// Not fatal: the following enable attempt will report the real problem if this
		// left the table in a state it can't recover from.
		t.Logf("error clearing capture instances for %s.%s before retry: %v", schema, name, err)
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
	db.enableCDC(t, schema, shortName)

	// Dropping a CDC-enabled table implicitly disables CDC for it, so this contends for
	// the same metadata as enabling does and takes the same lock.
	t.Cleanup(func() {
		cdcMetadataLock.Lock()
		defer cdcMetadataLock.Unlock()
		db.execWithDeadlockRetry(t, `IF OBJECT_ID('`+tableName+`', 'U') IS NOT NULL DROP TABLE `+tableName)
	})
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
