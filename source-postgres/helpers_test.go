package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
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
	TestDatabase      *pgx.Conn
)

func TestMain(m *testing.M) {
	flag.Parse()
	var ctx = context.Background()

	if testing.Verbose() {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// Tweak some parameters to make things easier to test on a smaller scale
	snapshotChunkSize = 16

	// Open a connection to the database which will be used for creating and
	// tearing down the replication slot.
	var replConnConfig, err = pgconn.ParseConfig(*TestConnectionURI)
	if err != nil {
		logrus.WithField("uri", *TestConnectionURI).WithField("err", err).Fatal("error parsing connection config")
	}
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
	TestDefaultConfig.ConnectionURI = *TestConnectionURI
	TestDefaultConfig.SlotName = *TestReplicationSlot
	TestDefaultConfig.PublicationName = *TestPublicationName
	TestDefaultConfig.PollTimeoutSeconds = *TestPollTimeoutSeconds

	conn, err := pgx.Connect(ctx, *TestConnectionURI)
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	TestDatabase = conn

	var exitCode = m.Run()
	if err := replConn.Exec(ctx, fmt.Sprintf(`DROP_REPLICATION_SLOT %s;`, *TestReplicationSlot)).Close(); err != nil {
		logrus.WithField("err", err).Fatal("error cleaning up replication slot")
	}
	os.Exit(exitCode)
}

// createTestTable is a test helper for creating a new database table and returning the
// name of the new table. The table is named "test_<testName>", or "test_<testName>_<suffix>"
// if the suffix is non-empty.
func createTestTable(ctx context.Context, t *testing.T, suffix string, tableDef string) string {
	t.Helper()

	var tableName = "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}
	tableName = strings.ReplaceAll(tableName, "/", "_")
	tableName = strings.ReplaceAll(tableName, "=", "_")

	logrus.WithField("table", tableName).WithField("cols", tableDef).Info("creating test table")
	dbQueryInternal(ctx, t, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	dbQueryInternal(ctx, t, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	t.Cleanup(func() {
		logrus.WithField("table", tableName).Info("destroying test table")
		dbQueryInternal(ctx, t, fmt.Sprintf(`DROP TABLE %s;`, tableName))
	})
	return tableName
}

// shortTestContext is a test helper which creates a time-bounded context for running test logic
func shortTestContext(t *testing.T) context.Context {
	return longTestContext(t, 10*time.Second)
}

// longTestContext is a test helper which creates a time-bounded context for running test logic
func longTestContext(t *testing.T, timeout time.Duration) context.Context {
	var ctx, cancel = context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	return ctx
}

// testCatalog is a test helper for constructing a ConfiguredCatalog from stream names
func testCatalog(streams ...string) airbyte.ConfiguredCatalog {
	var catalog = airbyte.ConfiguredCatalog{}
	for _, s := range streams {
		catalog.Streams = append(catalog.Streams, airbyte.ConfiguredStream{
			Stream: airbyte.Stream{Name: s, Namespace: "public"},
		})
	}
	return catalog
}

// dbLoadCSV is a test helper which opens a CSV file and inserts its contents
// into the specified database table. It supports strings and integers, however
// integers are autodetected as "any element which can be parsed as an integer",
// so the source dataset needs to be clean. For test data this should be fine.
func dbLoadCSV(ctx context.Context, t *testing.T, table string, filename string, limit int) {
	t.Helper()
	logrus.WithField("table", table).WithField("file", filename).Info("loading csv")
	var file, err = os.Open("testdata/" + filename)
	if err != nil {
		t.Fatalf("unable to open CSV file: %q", "testdata/"+filename)
	}
	defer file.Close()

	var dataset [][]interface{}
	var r = csv.NewReader(file)
	// If `limit` is positive, load at most `limit` rows
	for count := 0; count < limit || limit <= 0; count++ {
		var row, err = r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error reading from CSV: %v", err)
		}

		var datarow []interface{}
		for _, elemStr := range row {
			// Convert elements to numbers where possible
			var numeric, err = strconv.ParseFloat(elemStr, 64)
			if err == nil {
				datarow = append(datarow, numeric)
			} else {
				datarow = append(datarow, elemStr)
			}
		}
		dataset = append(dataset, datarow)

		// Insert in chunks of 64 rows at a time
		if len(dataset) >= 64 {
			dbInsert(ctx, t, table, dataset)
			dataset = nil
		}
	}
	// Perform one final insert for the remaining rows
	if len(dataset) > 0 {
		dbInsert(ctx, t, table, dataset)
	}
}

// dbInsert is a test helper for inserting multiple rows into TestDatabase
// as a single transaction.
func dbInsert(ctx context.Context, t *testing.T, table string, rows [][]interface{}) {
	t.Helper()

	if len(rows) < 1 {
		t.Fatalf("must insert at least one row")
	}
	var tx, err = TestDatabase.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatalf("unable to begin transaction: %v", err)
	}
	logrus.WithFields(logrus.Fields{"table": table, "count": len(rows), "first": rows[0]}).Info("inserting data")
	var query = fmt.Sprintf(`INSERT INTO %s VALUES %s`, table, argsTuple(len(rows[0])))
	for _, row := range rows {
		logrus.WithField("table", table).WithField("row", row).Debug("inserting row")
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

func argsTuple(argc int) string {
	var tuple = "($1"
	for idx := 1; idx < argc; idx++ {
		tuple += fmt.Sprintf(",$%d", idx+1)
	}
	return tuple + ")"
}

// dbQuery is a test helper for executing arbitrary queries against TestDatabase
func dbQuery(ctx context.Context, t *testing.T, query string, args ...interface{}) {
	t.Helper()
	logrus.WithField("query", query).WithField("args", args).Debug("executing query")
	dbQueryInternal(ctx, t, query, args...)
}

func dbQueryInternal(ctx context.Context, t *testing.T, query string, args ...interface{}) {
	var rows, err = TestDatabase.Query(ctx, query, args...)
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
}

// verifiedCapture is a test helper which performs a database capture and automatically
// verifies the result against a golden snapshot. It returns a list of all states
// emitted during the capture, and updates the `state` argument to the final one.
func verifiedCapture(ctx context.Context, t *testing.T, cfg *Config, catalog *airbyte.ConfiguredCatalog, state *PersistentState, suffix string) []PersistentState {
	t.Helper()
	var result, states = performCapture(ctx, t, cfg, catalog, state)
	verifySnapshot(t, suffix, result)
	return states
}

// performCapture runs a new capture instance with the specified configuration, catalog,
// and state. The resulting messages are stored into a buffer, and returned as a string
// holding all emitted records, plus a list of all state updates. The records string is
// sanitized of "nondeterministic" data like timestamps and LSNs which will vary across
// test runs, and so can be fed directly into verifySnapshot.
//
// As a side effect the input state is modified to the final result state.
func performCapture(ctx context.Context, t *testing.T, cfg *Config, catalog *airbyte.ConfiguredCatalog, state *PersistentState) (string, []PersistentState) {
	t.Helper()

	// Use a JSON round-trip to deep-copy the state, so that the act of running a
	// capture can't modify the passed-in state argument, and thus we can treat
	// the sequence of states as having value semantics within tests.
	var bs, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var cleanState = new(PersistentState)
	if err := json.Unmarshal(bs, cleanState); err != nil {
		t.Fatal(err)
	}

	var buf = new(CaptureOutputBuffer)
	if err := RunCapture(ctx, cfg, catalog, cleanState, buf); err != nil {
		t.Fatal(err)
	}

	var result, states = buf.Output()
	if len(states) > 0 {
		*state = states[len(states)-1]
	}
	return result, states
}

// A CaptureOutputBuffer receives the stream of output messages from a
// Capture instance, recording State updates in one list and Records
// in another.
type CaptureOutputBuffer struct {
	States   []PersistentState
	Snapshot strings.Builder
}

func (buf *CaptureOutputBuffer) Encode(v interface{}) error {
	var msg, ok = v.(airbyte.Message)
	if !ok {
		return fmt.Errorf("output message is not an airbyte.Message: %#v", v)
	}

	// Accumulate State updates in one list
	if msg.Type == airbyte.MessageTypeState {
		return buf.bufferState(msg)
	}
	if msg.Type == airbyte.MessageTypeRecord {
		return buf.bufferRecord(msg)
	}
	return fmt.Errorf("unhandled message type: %#v", msg.Type)
}

func (buf *CaptureOutputBuffer) bufferState(msg airbyte.Message) error {
	// Parse state data and store a copy for later resume testing. Note
	// that because we're unmarshalling each state update from JSON we
	// can rely on the states being independent and not sharing any
	// pointer-identity in their 'Streams' map or `ScanRanges` lists.
	var state PersistentState
	if err := json.Unmarshal(msg.State.Data, &state); err != nil {
		return fmt.Errorf("error unmarshaling to PersistentState: %w", err)
	}
	buf.States = append(buf.States, state)

	// Sanitize state by rewriting the LSN to a constant
	var cleanState = PersistentState{CurrentLSN: 1234, Streams: state.Streams}

	// Encode and buffer
	var bs, err = json.Marshal(cleanState)
	if err != nil {
		return fmt.Errorf("error encoding cleaned state: %w", err)
	}
	return buf.bufferMessage(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(bs)},
	})
}

func (buf *CaptureOutputBuffer) bufferRecord(msg airbyte.Message) error {
	return buf.bufferMessage(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: msg.Record.Namespace,
			Stream:    msg.Record.Stream,
			EmittedAt: 1234, // Replaced because non-reproducible
			Data:      msg.Record.Data,
		},
	})
}

func (buf *CaptureOutputBuffer) bufferMessage(msg airbyte.Message) error {
	var bs, err = json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error encoding sanitized message: %w", err)
	}
	logrus.WithField("data", string(bs)).Debug("buffered message")
	buf.Snapshot.Write(bs)
	buf.Snapshot.WriteByte('\n')
	return nil
}

func (buf *CaptureOutputBuffer) Output() (string, []PersistentState) {
	return buf.Snapshot.String(), buf.States
}

// verifySnapshot loads snapshot content from a file and compares with the
// actual result of a test. The snapshot filename is derived automatically
// from the current test name, with an optional suffix in case a single
// test needs multiple snapshots. In the event of a mismatch, a ".new"
// file is written for ease of comparison/updating.
func verifySnapshot(t *testing.T, suffix string, actual string) {
	t.Helper()

	var snapshotDir = "testdata"
	var snapshotFile = snapshotDir + "/" + t.Name()
	if suffix != "" {
		snapshotFile += "_" + suffix
	}
	snapshotFile += ".snapshot"

	var snapBytes, err = os.ReadFile(snapshotFile)
	// Nonexistent snapshots aren't an error, because when adding a
	// new test we'd like it to produce a "snapshot.new" file for us
	// and the empty string won't match the expected result anyway.
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("error reading snapshot %q: %v", snapshotFile, err)
	}

	if actual == string(snapBytes) {
		return
	}

	var newSnapshotFile = snapshotFile + ".new"
	if err := os.WriteFile(newSnapshotFile, []byte(actual), 0644); err != nil {
		t.Errorf("error writing new snapshot file %q: %v", newSnapshotFile, err)
	}

	// Locate the first non-matching line and log it
	var actualLines = strings.Split(actual, "\n")
	var snapshotLines = strings.Split(string(snapBytes), "\n")
	for idx := 0; idx < len(snapshotLines) || idx < len(actualLines); idx++ {
		var x, y string
		if idx < len(snapshotLines) {
			x = snapshotLines[idx]
		}
		if idx < len(actualLines) {
			y = actualLines[idx]
		}
		if x != y {
			t.Errorf("snapshot %q mismatch at line %d", snapshotFile, idx)
			t.Errorf("old: %s", x)
			t.Errorf("new: %s", y)
			return
		}
	}
	t.Errorf("snapshot %q mismatch at undetermined line", snapshotFile)
}
