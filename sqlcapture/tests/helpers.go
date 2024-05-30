package tests

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// VerifiedCapture performs a capture using the provided st.CaptureSpec and shuts it
// down after replication is caught up and all backfills are complete. It then performs
// snapshot verification on the results as summarized by the capture validator.
func VerifiedCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	cupaloy.SnapshotT(t, RunCapture(ctx, t, cs))
}

// RunCapture performs a capture using the provided st.CaptureSpec and shuts it down after
// replication is caught up and all backfills are complete. It then returns the test results
// as summarized by the capture validator, and resets the validator for the next run.
func RunCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) string {
	t.Helper()

	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() { sqlcapture.TestShutdownAfterCaughtUp = false })

	cs.Capture(ctx, t, nil)
	var summary = cs.Summary()
	cs.Reset()
	return summary
}

// RestartingBackfillCapture runs a capture multiple times, using test behavior flags to
// stop and restart it after each backfill chunk. It stops running the capture after the
// state checkpoint no longer has a non-empty backfill cursor.
func RestartingBackfillCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) (string, []json.RawMessage) {
	t.Helper()

	sqlcapture.TestShutdownAfterBackfill = true
	sqlcapture.TestShutdownAfterCaughtUp = true
	t.Cleanup(func() {
		sqlcapture.TestShutdownAfterBackfill = false
		sqlcapture.TestShutdownAfterCaughtUp = false
	})

	var scanCursorRegex = regexp.MustCompile(`"scanned":"(.*?)"`)
	var checkpoints = []json.RawMessage{cs.Checkpoint}
	var summary = new(strings.Builder)
	for {
		var startKey string
		if m := scanCursorRegex.FindStringSubmatch(string(cs.Checkpoint)); len(m) > 1 {
			startKey = m[1]
		}
		if startKey == "" && len(checkpoints) > 1 {
			log.Info("restarting backfill capture complete")
			break
		}

		fmt.Fprintf(summary, "####################################\n")
		if startKey != "" {
			fmt.Fprintf(summary, "### Capture from Key %q\n", startKey)
		} else {
			fmt.Fprintf(summary, "### Capture from Start\n")
		}
		fmt.Fprintf(summary, "####################################\n")

		cs.Capture(ctx, t, nil)
		checkpoints = append(checkpoints, cs.Checkpoint)
		io.Copy(summary, strings.NewReader(cs.Summary()))
		fmt.Fprintf(summary, "\n\n")

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
	}

	return summary.String(), checkpoints
}

func DiscoverBindings(ctx context.Context, t testing.TB, tb TestBackend, streamMatchers ...*regexp.Regexp) []*pf.CaptureSpec_Binding {
	t.Helper()
	var cs = tb.CaptureSpec(ctx, t)

	// Perform discovery, expecting to match one stream with each provided regexp.
	var discovery = cs.Discover(ctx, t, streamMatchers...)
	if len(discovery) != len(streamMatchers) {
		t.Fatalf("discovered incorrect number of streams: got %d, expected %d", len(discovery), len(streamMatchers))
	}

	// Translate discovery bindings into capture bindings.
	var bindings []*pf.CaptureSpec_Binding
	for _, b := range discovery {
		var res sqlcapture.Resource
		require.NoError(t, json.Unmarshal(b.ResourceConfigJson, &res))
		var path = []string{res.Namespace, res.Stream}
		bindings = append(bindings, &pf.CaptureSpec_Binding{
			ResourceConfigJson: b.ResourceConfigJson,
			Collection: pf.CollectionSpec{
				Name:           pf.Collection("acmeCo/test/" + b.RecommendedName),
				ReadSchemaJson: b.DocumentSchemaJson,
				Key:            b.Key,
				// Converting the discovered schema into a list of projections would be quite
				// a task and all we actually need it for is to enable transaction IDs in
				// MySQL and Postgres.
				Projections: []pf.Projection{{Ptr: "/_meta/source/txid"}},
			},
			ResourcePath: path,
			StateKey:     StateKey(path),
		})
	}
	return bindings
}

// StateKey provides an approximation for a runtime-computed state key based on a path, where the
// path is joined with slashes and then percent encoded.
func StateKey(path []string) string {
	return url.QueryEscape(strings.Join(path, "/"))
}

// BindingReplace returns a copy of a "template" binding with the string substitution `s/old/new`
// applied to the collection name, resource spec, resource path and state key values.
//
// This allows tests to exercise missing-table functionality by creating and discovering a table
// (for instance `foobar_aaa`) and then by string substitution turning that into a binding for table
// `foobar_bbb` which doesn't actually exist.
func BindingReplace(b *pf.CaptureSpec_Binding, old, new string) *pf.CaptureSpec_Binding {
	var x = *b
	x.Collection.Name = pf.Collection(strings.ReplaceAll(string(x.Collection.Name), old, new))
	x.ResourceConfigJson = json.RawMessage(strings.ReplaceAll(string(x.ResourceConfigJson), old, new))
	for idx := range x.ResourcePath {
		x.ResourcePath[idx] = strings.ReplaceAll(x.ResourcePath[idx], old, new)
	}
	x.StateKey = strings.ReplaceAll(x.StateKey, old, new)
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
