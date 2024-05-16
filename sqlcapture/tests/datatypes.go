package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/estuary/flow/go/protocols/capture"
	"github.com/stretchr/testify/require"
)

// A DatatypeTestCase defines the inputs and expected outputs to a single
// instance of the TestDatatypes test.
type DatatypeTestCase struct {
	ColumnType  string
	ExpectType  string
	InputValue  interface{}
	ExpectValue string

	PrimaryKeyValue    interface{}
	PrimaryExpectValue string
}

// TestDatatypes runs a series of tests creating tables with specific column types
// and performing discovery/capture to verify that each type is supported properly.
func TestDatatypes(ctx context.Context, t *testing.T, tb TestBackend, cases []DatatypeTestCase) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	for idx, tc := range cases {
		var testName = sanitizeName(tc.ColumnType)
		if len(testName) > 8 {
			testName = testName[:8]
		}
		t.Run(fmt.Sprintf("%d_%s", idx, testName), func(t *testing.T) {
			var uniqueID = fmt.Sprintf("1%07d", idx)
			var tableName string
			if tc.PrimaryKeyValue != nil {
				tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(b %s PRIMARY KEY)", tc.ColumnType))
			} else {
				tableName = tb.CreateTable(ctx, t, uniqueID, fmt.Sprintf("(a INTEGER PRIMARY KEY, b %s)", tc.ColumnType))
			}
			var stream *capture.Response_Discovered_Binding

			// Perform discovery and verify that the generated JSON schema looks correct
			t.Run("discovery", func(t *testing.T) {
				var bindings = tb.CaptureSpec(ctx, t).Discover(ctx, t, regexp.MustCompile(uniqueID))
				stream = bindings[0]
				var skimmed = struct {
					Definitions map[string]struct {
						Properties map[string]json.RawMessage
					} `json:"$defs"`
				}{}
				require.NoError(t, json.Unmarshal(stream.DocumentSchemaJson, &skimmed))
				require.Len(t, skimmed.Definitions, 1)

				for _, tbl := range skimmed.Definitions {
					var expectParsed, actualParsed interface{}
					require.NoError(t, json.Unmarshal([]byte(tc.ExpectType), &expectParsed))
					require.NoError(t, json.Unmarshal(tbl.Properties["b"], &actualParsed))
					require.Equal(t, expectParsed, actualParsed)
				}
			})

			// Insert a test row and scan it back out, then do the same via replication
			t.Run("roundtrip", func(t *testing.T) {
				var cs = tb.CaptureSpec(ctx, t, regexp.MustCompile(uniqueID))

				// Don't sanitize anything, we're only looking for the output value field
				// and sometimes that could contain a timestamp-looking value.
				cs.Sanitizers = make(map[string]*regexp.Regexp)

				t.Run("scan", func(t *testing.T) {
					var v [][]any
					if tc.PrimaryKeyValue != nil {
						v = [][]any{{tc.InputValue}}
					} else {
						v = [][]any{{1, tc.InputValue}}
					}
					tb.Insert(ctx, t, tableName, v)
					var output = RunCapture(ctx, t, cs)
					verifyRoundTrip(t, tc, tc.ExpectValue, output)
				})

				t.Run("replication", func(t *testing.T) {
					var v [][]any
					var expected string = tc.ExpectValue
					if tc.PrimaryKeyValue != nil {
						v = [][]any{{tc.PrimaryKeyValue}}
						expected = tc.PrimaryExpectValue
					} else {
						v = [][]any{{2, tc.InputValue}}
					}
					tb.Insert(ctx, t, tableName, v)
					var output = RunCapture(ctx, t, cs)

					verifyRoundTrip(t, tc, expected, output)
				})
			})
		})
	}
}

type datatypeTestRecord struct {
	Value json.RawMessage `json:"b"`
}

func verifyRoundTrip(t *testing.T, tc DatatypeTestCase, expected, actual string) {
	t.Helper()

	// Extract the document of interest from the full capture summary
	const documentPrefix = `{"_meta":`

	var record *datatypeTestRecord
	for _, line := range strings.Split(actual, "\n") {
		if strings.HasPrefix(line, documentPrefix) {
			record = new(datatypeTestRecord)
			if err := json.Unmarshal([]byte(line), record); err != nil {
				t.Errorf("error unmarshalling result record: %v", err)
				return
			}
		}
	}
	if record == nil {
		t.Errorf("expected record not found in capture output")
		return
	}

	if string(record.Value) != expected {
		t.Errorf("result mismatch for type %q: input %q, got %q, expected %q",
			tc.ColumnType, tc.InputValue, string(record.Value), tc.ExpectValue)
	}
}

func sanitizeName(name string) string {
	var xs = []byte(name)
	for i := range xs {
		if '0' <= xs[i] && xs[i] <= '9' {
			continue
		}
		if 'a' <= xs[i] && xs[i] <= 'z' {
			continue
		}
		if 'A' <= xs[i] && xs[i] <= 'Z' {
			continue
		}
		xs[i] = '_'
	}
	return strings.TrimRight(string(xs), "_")
}
