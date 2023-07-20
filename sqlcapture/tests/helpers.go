package tests

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/flow"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// VerifiedCapture performs a capture using the provided st.CaptureSpec and shuts it down after
// a suitable time has elapsed without any documents or state checkpoints being emitted. It
// then performs snapshot verification on the results.
func VerifiedCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	var summary = RunCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

// CaptureShutdownDelay is the length of time after which RunCapture() will
// terminate a test capture if there hasn't been any further output.
var CaptureShutdownDelay = 200 * time.Millisecond

// RunCapture performs a capture using the provided st.CaptureSpec and shuts it down after
// a suitable time has elapsed without any documents or state checkpoints being emitted.
func RunCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) string {
	t.Helper()
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	var shutdownWatchdog *time.Timer
	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(CaptureShutdownDelay, func() {
				log.WithField("delay", CaptureShutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(CaptureShutdownDelay)
	})
	var summary = cs.Summary()
	cs.Reset()
	return summary
}

// RestartingBackfillCapture runs a capture multiple times, killing it after each time a new
// resume key is observed in a state checkpoint and then restarting from that checkpoint. It
// also returns the sequence of unique restart checkpoints.
func RestartingBackfillCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) (string, []json.RawMessage) {
	t.Helper()

	// After multiple rounds of trying to fix it the RestartingBackfillCapture helper
	// still suffers from some sort of nondeterminism which causes frequent flake in
	// our CI builds. Oddly it doesn't appear nearly so often locally, but for now
	// we need to skip them under CI so builds reliably succeed.
	if val := os.Getenv("CI_BUILD"); val != "" {
		t.Skipf("skipping %q in CI builds", t.Name())
	}

	var checkpointRegex = regexp.MustCompile(`^{"cursor":`)
	var scanCursorRegex = regexp.MustCompile(`"scanned":"(.*)"`)

	var checkpoints = []json.RawMessage{cs.Checkpoint}
	var startKey string
	if m := scanCursorRegex.FindStringSubmatch(string(cs.Checkpoint)); len(m) > 1 {
		startKey = m[1]
	}

	var summary = new(strings.Builder)
	for {
		fmt.Fprintf(summary, "####################################\n")
		if startKey != "" {
			fmt.Fprintf(summary, "### Capture from Key %q\n", startKey)
		} else {
			fmt.Fprintf(summary, "### Capture from Start\n")
		}
		fmt.Fprintf(summary, "####################################\n")

		var captureCtx, cancelCapture = context.WithCancel(ctx)
		cs.Capture(captureCtx, t, func(data json.RawMessage) {
			// After the capture context is cancelled, don't do any of this processing
			// on subsequent checkpoints that might be emitted. We want to act as though
			// there's only the one new state checkpoint emitted by a capture and ignore
			// anything after that.
			if captureCtx.Err() != nil {
				return
			}
			if checkpointRegex.Match(data) {
				var nextKey string
				if m := scanCursorRegex.FindStringSubmatch(string(data)); len(m) > 1 {
					nextKey = m[1]
				}
				if nextKey != startKey {
					log.WithField("checkpoint", string(data)).Info("new scan key/checkpoint")
					cs.Checkpoint, startKey = data, nextKey
					checkpoints = append(checkpoints, cs.Checkpoint)
					cancelCapture()

					// Copy the capture output so far into the summary *now* so that
					// any documents emitted after this point won't be included. This
					// should improve test stability.
					io.Copy(summary, strings.NewReader(cs.Summary()))
					fmt.Fprintf(summary, "\n\n")
				}
			}
		})

		if len(cs.Errors) > 0 {
			fmt.Fprintf(summary, "####################################\n")
			fmt.Fprintf(summary, "### Terminating Capture due to Errors\n")
			fmt.Fprintf(summary, "####################################\n")
			for _, err := range cs.Errors {
				fmt.Fprintf(summary, "%v\n", err)
			}
			break
		}
		cs.Reset()

		if startKey == "" {
			break
		}
	}

	return summary.String(), checkpoints
}

func DiscoverBindings(ctx context.Context, t testing.TB, tb TestBackend, streamMatchers ...*regexp.Regexp) []*flow.CaptureSpec_Binding {
	t.Helper()
	var cs = tb.CaptureSpec(ctx, t)

	// Perform discovery, expecting to match one stream with each provided regexp.
	var discovery = cs.Discover(ctx, t, streamMatchers...)
	if len(discovery) != len(streamMatchers) {
		t.Fatalf("discovered incorrect number of streams: got %d, expected %d", len(discovery), len(streamMatchers))
	}

	// Translate discovery bindings into capture bindings.
	var bindings []*flow.CaptureSpec_Binding
	for _, b := range discovery {
		var res sqlcapture.Resource
		require.NoError(t, json.Unmarshal(b.ResourceConfigJson, &res))
		bindings = append(bindings, &flow.CaptureSpec_Binding{
			ResourceConfigJson: b.ResourceConfigJson,
			Collection: flow.CollectionSpec{
				Name:           pf.Collection("acmeCo/test/" + b.RecommendedName),
				ReadSchemaJson: b.DocumentSchemaJson,
				Key:            b.Key,
				// Converting the discovered schema into a list of projections would be quite
				// a task and all we actually need it for is to enable transaction IDs in
				// MySQL and Postgres.
				Projections: []pf.Projection{{Ptr: "/_meta/source/txid"}},
			},

			ResourcePath: []string{res.Namespace, res.Stream},
		})
	}
	return bindings
}

// BindingReplace returns a copy of a "template" binding with the string substitution
// `s/old/new` applied to the collection name, resource spec, and resource path values.
// This allows tests to exercise missing-table functionality by creating and discovering
// a table (for instance `foobar_aaa`) and then by string substitution turning that into
// a binding for table `foobar_bbb` which doesn't actually exist.
func BindingReplace(b *flow.CaptureSpec_Binding, old, new string) *flow.CaptureSpec_Binding {
	var x = *b
	x.Collection.Name = flow.Collection(strings.ReplaceAll(string(x.Collection.Name), old, new))
	x.ResourceConfigJson = json.RawMessage(strings.ReplaceAll(string(x.ResourceConfigJson), old, new))
	for idx := range x.ResourcePath {
		x.ResourcePath[idx] = strings.ReplaceAll(x.ResourcePath[idx], old, new)
	}
	return &x
}

// LoadCSV is a test helper which opens a CSV file and inserts its contents
// into the specified database table. It supports strings and integers, however
// integers are autodetected as "any element which can be parsed as an integer",
// so the source dataset needs to be clean. For test data this should be fine.
func LoadCSV(ctx context.Context, t *testing.T, tb TestBackend, table string, filename string, limit int) {
	t.Helper()
	log.WithFields(log.Fields{"table": table, "file": filename}).Info("loading csv")
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
