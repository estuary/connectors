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
	log "github.com/sirupsen/logrus"
)

// VerifiedCapture performs a capture using the provided st.CaptureSpec and shuts it down after
// a suitable time has elapsed without any documents or state checkpoints being emitted. It
// then performs snapshot verification on the results.
func VerifiedCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	var summary = RunCapture(ctx, t, cs)
	cupaloy.SnapshotT(t, summary)
}

// RunCapture performs a capture using the provided st.CaptureSpec and shuts it down after
// a suitable time has elapsed without any documents or state checkpoints being emitted.
func RunCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) string {
	t.Helper()
	var captureCtx, cancelCapture = context.WithCancel(ctx)
	const shutdownDelay = 200 * time.Millisecond
	var shutdownWatchdog *time.Timer
	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(shutdownDelay)
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
				}
			}
		})

		io.Copy(summary, strings.NewReader(cs.Summary()))
		fmt.Fprintf(summary, "\n\n")
		if len(cs.Errors) > 0 {
			fmt.Fprintf(summary, "####################################\n")
			fmt.Fprintf(summary, "### Terminating Capture due to Errors\n")
			fmt.Fprintf(summary, "####################################\n")
			break
		}
		cs.Reset()

		if startKey == "" {
			break
		}
	}

	return summary.String(), checkpoints
}

// ResourceBindings returns a new list of capture bindings for the specified streamIDs.
func ResourceBindings(t testing.TB, streamIDs ...string) []*flow.CaptureSpec_Binding {
	t.Helper()
	var bindings []*flow.CaptureSpec_Binding
	for _, streamID := range streamIDs {
		var parts = strings.SplitN(streamID, ".", 2)
		var bs, err = json.Marshal(sqlcapture.Resource{
			Namespace: parts[0],
			Stream:    parts[1],
		})
		if err != nil {
			t.Fatalf("error marshalling resource json: %v", err)
		}
		bindings = append(bindings, &flow.CaptureSpec_Binding{
			ResourceSpecJson: bs,
			ResourcePath:     []string{streamID},
		})
	}
	return bindings
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
