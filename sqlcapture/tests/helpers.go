package tests

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/protocols/airbyte"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/sirupsen/logrus"
)

// VerifiedCapture is a test helper which performs a database capture and automatically
// verifies the result against a golden snapshot. It returns a list of all states
// emitted during the capture, and updates the `state` argument to the final one.
func VerifiedCapture(ctx context.Context, t *testing.T, tb TestBackend, catalog *airbyte.ConfiguredCatalog, state *sqlcapture.PersistentState, suffix string) []sqlcapture.PersistentState {
	t.Helper()
	var result, states = PerformCapture(ctx, t, tb, catalog, state)
	VerifySnapshot(t, suffix, result)
	return states
}

// PerformCapture runs a new capture instance with the specified catalog and state. The resulting
// messages are stored into a buffer, and returned as a string holding all emitted records,
// plus a list of all state updates. The records string is sanitized of data like timestamps
// and LSNs which will vary across test runs, and so can be fed directly into VerifySnapshot.
//
// As a side effect the input state is modified to the final result state.
func PerformCapture(ctx context.Context, t *testing.T, tb TestBackend, catalog *airbyte.ConfiguredCatalog, state *sqlcapture.PersistentState) (string, []sqlcapture.PersistentState) {
	t.Helper()

	// Use a JSON round-trip to deep-copy the state, so that the act of running a
	// capture can't modify the passed-in state argument, and thus we can treat
	// the sequence of states as having value semantics within tests.
	var bs, err = json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var cleanState = new(sqlcapture.PersistentState)
	if err := json.Unmarshal(bs, cleanState); err != nil {
		t.Fatal(err)
	}

	var buf = new(CaptureOutputBuffer)
	if err := sqlcapture.RunCapture(ctx, tb.GetDatabase(), catalog, cleanState, buf); err != nil {
		t.Fatal(err)
	}

	var result, states = buf.Output()
	if len(states) > 0 {
		*state = states[len(states)-1]
	}
	return result, states
}

// ConfiguredCatalog is a test helper for constructing a ConfiguredCatalog from stream names
func ConfiguredCatalog(streams ...string) airbyte.ConfiguredCatalog {
	var catalog = airbyte.ConfiguredCatalog{}
	for _, s := range streams {
		catalog.Streams = append(catalog.Streams, airbyte.ConfiguredStream{
			Stream: airbyte.Stream{Name: s},
		})
	}
	return catalog
}

// VerifySnapshot loads snapshot content from a file and compares with the
// actual result of a test. The snapshot filename is derived automatically
// from the current test name, with an optional suffix in case a single
// test needs multiple snapshots. In the event of a mismatch, a ".new"
// file is written for ease of comparison/updating.
func VerifySnapshot(t *testing.T, suffix string, actual string) {
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
			break
		}
	}
	if !t.Failed() {
		t.Errorf("snapshot %q mismatch at undetermined line", snapshotFile)
	}

	// Replicate cupaloy's use of an environment variable to update snapshots in-place.
	var newSnapshotFile = snapshotFile
	if os.Getenv("UPDATE_SNAPSHOTS") != "1" {
		newSnapshotFile += ".new"
	}
	t.Logf("writing snapshot %q", newSnapshotFile)
	if err := os.WriteFile(newSnapshotFile, []byte(actual), 0644); err != nil {
		t.Errorf("error writing new snapshot file %q: %v", newSnapshotFile, err)
	}
}

// VerifyStream is a helper function which locates a particular stream by name in
// the discovered catalog and then uses VerifySnapshot on that. This is necessary
// because we don't want to make assumptions about what other tables might be
// present in the test database or what order they might be discovered in, we
// just want to test that the specific table we created in our test looks good.
func VerifyStream(t *testing.T, suffix string, catalog *airbyte.Catalog, expectedStream string) {
	t.Helper()
	for _, stream := range catalog.Streams {
		if !strings.EqualFold(stream.Name, expectedStream) {
			continue
		}

		var buf bytes.Buffer
		var enc = json.NewEncoder(&buf)
		enc.SetIndent("", "  ")

		if err := enc.Encode(stream); err != nil {
			t.Fatalf("error marshalling stream %q: %v", expectedStream, err)
		}

		VerifySnapshot(t, suffix, buf.String())
		return
	}
	t.Fatalf("test stream %q not found in catalog", expectedStream)
}

// LoadCSV is a test helper which opens a CSV file and inserts its contents
// into the specified database table. It supports strings and integers, however
// integers are autodetected as "any element which can be parsed as an integer",
// so the source dataset needs to be clean. For test data this should be fine.
func LoadCSV(ctx context.Context, t *testing.T, tb TestBackend, table string, filename string, limit int) {
	t.Helper()
	logrus.WithFields(logrus.Fields{"table": table, "file": filename}).Info("loading csv")
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
			tb.Insert(ctx, t, table, dataset)
			dataset = nil
		}
	}
	// Perform one final insert for the remaining rows
	if len(dataset) > 0 {
		tb.Insert(ctx, t, table, dataset)
	}
}

// A CaptureOutputBuffer receives the stream of output messages from a
// Capture instance, recording State updates in one list and Records
// in another.
type CaptureOutputBuffer struct {
	States    []sqlcapture.PersistentState
	Snapshot  strings.Builder
	lastState string
}

// Encode accepts output messages from a capture, and is named 'Encode' to
// satisfy the sqlcapture.MessageOutput interface.
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
	if msg.Type == airbyte.MessageTypeLog {
		return nil // Ignore log messages when validating test output
	}
	return fmt.Errorf("unhandled message type: %#v", msg.Type)
}

func (buf *CaptureOutputBuffer) bufferState(msg airbyte.Message) error {
	// Parse state data and store a copy for later resume testing. Note
	// that because we're unmarshalling each state update from JSON we
	// can rely on the states being independent and not sharing any
	// pointer-identity in their 'Streams' map or `ScanRanges` lists.
	var originalState sqlcapture.PersistentState
	if err := json.Unmarshal(msg.State.Data, &originalState); err != nil {
		return fmt.Errorf("error unmarshaling to PersistentState: %w", err)
	}

	// Sanitize state by rewriting the LSN to a constant, then encode
	// back into new bytes.
	var cleanState = sqlcapture.PersistentState{Cursor: "REDACTED", Streams: originalState.Streams}
	var bs, err = json.Marshal(cleanState)
	if err != nil {
		return fmt.Errorf("error encoding cleaned state: %w", err)
	}

	// Suppress identical (after sanitizing LSN) successive state updates.
	// This improves test stability in the presence of unexpected 'Commit'
	// events (typically on other tables not subject to the current test).
	if string(bs) == buf.lastState {
		return nil
	}
	buf.lastState = string(bs)

	// Buffer the original state for later resuming, and append the sanitized
	// state to the output buffer.
	buf.States = append(buf.States, originalState)
	return buf.bufferMessage(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(bs)},
	})
}

func (buf *CaptureOutputBuffer) bufferRecord(msg airbyte.Message) error {
	buf.lastState = ""

	// Blank fields which are not reproducible across tests.
	var err error
	if msg.Record.Data, err = jsonpatch.MergePatch(msg.Record.Data, []byte(`{
		"_meta": {
			"source": {
				"loc": null,
				"ts_ms": null
			}
		}
	}`)); err != nil {
		return err
	}

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
	buf.Snapshot.Write(bs)
	buf.Snapshot.WriteByte('\n')
	return nil
}

// Output returns the buffered output of a capture.
func (buf *CaptureOutputBuffer) Output() (string, []sqlcapture.PersistentState) {
	return buf.Snapshot.String(), buf.States
}
