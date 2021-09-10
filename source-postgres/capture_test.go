package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	testDefaultConfig = Config{
		ConnectionURI:      TestingConnectionURI,
		SlotName:           "flow_slot",
		PublicationName:    "flow_publication",
		PollTimeoutSeconds: 1,
	}
)

var TestDatabase *pgx.Conn

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Verbose() {
		logrus.SetLevel(logrus.DebugLevel)
	}

	// Reuse the same database connection for all test setup/teardown queries
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, TestingConnectionURI)
	if err != nil {
		log.Fatalf("error connecting to database: %v", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	TestDatabase = conn

	os.Exit(m.Run())
}

func TestSimpleCapture(t *testing.T) {
	cfg, ctx := testDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	// Add data, perform capture, verify result
	for id, data := range []string{"A", "bbb", "CDEFGHIJKLMNOP", "Four", "5"} {
		dbQuery(t, ctx, fmt.Sprintf(`INSERT INTO %s(id, data) VALUES ($1, $2)`, tableName), id, data)
	}
	result, _ := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "", result)
}

// TestSecondCapture runs two captures, as the name implies, with some additional
// data added in between and with the second capture resuming from the final state
// of the first one. The second capture will emit only those rows which were added
// after the first run, and because it ought to be using replication to get the new
// rows they should be emitted in the order they were added rather than primary-key
// order.
func TestSecondCapture(t *testing.T) {
	cfg, ctx := testDefaultConfig, shortTestContext(t)
	tableName := createTestTable(t, ctx, "", "(id INTEGER PRIMARY KEY, data TEXT)")
	catalog, state := testCatalog(tableName), PersistentState{}

	// Add data, perform capture, repeat
	for id, data := range []string{"A", "bbb", "CDEFGHIJKLMNOP", "Four", "5"} {
		dbQuery(t, ctx, fmt.Sprintf(`INSERT INTO %s(id, data) VALUES ($1, $2)`, tableName), id, data)
	}
	firstResult, states := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "first", firstResult)
	state = states[len(states)-1]

	for _, row := range [][]interface{}{{1002, "some"}, {1000, "more"}, {1001, "rows"}} {
		dbQuery(t, ctx, fmt.Sprintf(`INSERT INTO %s(id, data) VALUES ($1, $2)`, tableName), row...)
	}
	secondResult, _ := performCapture(t, ctx, &cfg, &catalog, &state)
	verifySnapshot(t, "second", secondResult)
}

func createTestTable(t *testing.T, ctx context.Context, suffix string, tableDef string) string {
	t.Helper()

	tableName := "test_" + strings.TrimPrefix(t.Name(), "Test")
	if suffix != "" {
		tableName += "_" + suffix
	}

	logrus.WithField("table", tableName).Info("creating test table")

	dbQuery(t, ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s;`, tableName))
	dbQuery(t, ctx, fmt.Sprintf(`CREATE TABLE %s%s;`, tableName, tableDef))
	t.Cleanup(func() { dbQuery(t, ctx, fmt.Sprintf(`DROP TABLE %s;`, tableName)) })
	return tableName
}

func shortTestContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func testCatalog(streams ...string) airbyte.ConfiguredCatalog {
	catalog := airbyte.ConfiguredCatalog{}
	for _, s := range streams {
		catalog.Streams = append(catalog.Streams, airbyte.ConfiguredStream{
			Stream: airbyte.Stream{Name: s},
		})
	}
	return catalog
}

func dbQuery(t *testing.T, ctx context.Context, query string, args ...interface{}) {
	t.Helper()

	logrus.WithField("query", query).WithField("args", args).Info("executing query")
	rows, err := TestDatabase.Query(ctx, query, args...)
	if err != nil {
		t.Fatalf("unable to execute query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			t.Fatalf("error processing query result: %v", err)
		}
		logrus.WithField("values", vals).Debug("query result row")
	}
}

// performCapture runs a new capture instance with the specified configuration, catalog,
// and state. The resulting messages are stored into a buffer, and returned as a string
// holding all emitted records, plus a list of all state updates. The records string is
// sanitized of "nondeterministic" data like timestamps and LSNs which will vary across
// test runs, and so can be fed directly into verifySnapshot.
func performCapture(t *testing.T, ctx context.Context, cfg *Config, catalog *airbyte.ConfiguredCatalog, state *PersistentState) (string, []PersistentState) {
	t.Helper()

	buf := new(CaptureOutputBuffer)
	capture, err := NewCapture(ctx, cfg, catalog, state, buf)
	if err != nil {
		t.Fatal(err)
	}
	if err := capture.Execute(ctx); err != nil {
		t.Fatal(err)
	}

	return buf.Output()
}

// A CaptureOutputBuffer receives the stream of output messages from a
// Capture instance, recording State updates in one list and Records
// in another.
type CaptureOutputBuffer struct {
	States  []PersistentState
	Records []string
}

func (buf *CaptureOutputBuffer) Encode(v interface{}) error {
	msg, ok := v.(airbyte.Message)
	if !ok {
		return errors.Errorf("output message is not an airbyte.Message: %#v", v)
	}

	// Accumulate State updates in one list
	if msg.Type == airbyte.MessageTypeState {
		var state PersistentState
		if err := json.Unmarshal(msg.State.Data, &state); err != nil {
			return errors.Wrap(err, "error unmarshaling to PersistentState")
		}
		buf.States = append(buf.States, state)
		return nil
	}
	if msg.Type == airbyte.MessageTypeRecord {
		return buf.encodeSanitizedRecord(*msg.Record)
	}
	return errors.Errorf("unhandled message type: %#v", msg.Type)
}

func (buf *CaptureOutputBuffer) encodeSanitizedRecord(record airbyte.Record) error {
	logrus.WithField("record", record).Debug("sanitizing record")
	// Unmarshal record data into a generic map
	fields := make(map[string]interface{})
	if err := json.Unmarshal(record.Data, &fields); err != nil {
		return errors.Wrap(err, "error unmarshalling record data")
	}

	// Overwrite "non-deterministic" fields of the change event
	if _, ok := fields["_ingested_at"]; ok {
		fields["_ingested_at"] = "SANITIZED"
	}
	if _, ok := fields["_change_lsn"]; ok {
		fields["_change_lsn"] = "SANITIZED"
	}

	// Re-encode the sanitized data into a sanitized record
	sanitizedData, err := json.Marshal(fields)
	if err != nil {
		return errors.Wrap(err, "error re-encoding record data")
	}
	record.EmittedAt = 0
	record.Data = json.RawMessage(sanitizedData)
	sanitizedRecord, err := json.Marshal(record)
	if err != nil {
		return errors.Wrap(err, "error re-encoding record")
	}

	// Append the result to the buffer
	buf.Records = append(buf.Records, string(sanitizedRecord))
	return nil
}

func (buf *CaptureOutputBuffer) Output() (string, []PersistentState) {
	return strings.Join(buf.Records, "\n"), buf.States
}

// verifySnapshot loads snapshot content from a file and compares with the
// actual result of a test. The snapshot filename is derived automatically
// from the current test name, with an optional suffix in case a single
// test needs multiple snapshots. In the event of a mismatch, a ".new"
// file is written for ease of comparison/updating.
func verifySnapshot(t *testing.T, suffix string, actual string) {
	t.Helper()

	snapshotDir := "testdata"
	snapshotFile := snapshotDir + "/" + t.Name()
	if suffix != "" {
		snapshotFile += "_" + suffix
	}
	snapshotFile += ".snapshot"

	snapBytes, err := os.ReadFile(snapshotFile)
	// Nonexistent snapshots aren't an error, because when adding a
	// new test we'd like it to produce a "snapshot.new" file for us
	// and the empty string won't match the expected result anyway.
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("error reading snapshot %q: %v", snapshotFile, err)
	}

	if actual == string(snapBytes) {
		return
	}

	newSnapshotFile := snapshotFile + ".new"
	if err := os.WriteFile(newSnapshotFile, []byte(actual), 0644); err != nil {
		t.Errorf("error writing new snapshot file %q: %v", newSnapshotFile, err)
	}

	// TODO(wgd): Maybe add logic to break up the expected and actual strings
	// by lines and show the first mismatched line, so that test output actually
	// indicates the problem in simple cases?
	t.Fatalf("snapshot %q mismatch", snapshotFile)
}
