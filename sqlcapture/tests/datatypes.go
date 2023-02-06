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
			var uniqueSuffix = fmt.Sprintf("scrabbled_%d_quinine", idx)
			var tableName = tb.CreateTable(ctx, t, uniqueSuffix, fmt.Sprintf("(a INTEGER PRIMARY KEY, b %s)", tc.ColumnType))
			var stream *capture.DiscoverResponse_Binding

			// Perform discovery and verify that the generated JSON schema looks correct
			t.Run("discovery", func(t *testing.T) {
				var bindings = tb.CaptureSpec(t).Discover(ctx, t, regexp.MustCompile(regexp.QuoteMeta(uniqueSuffix)))
				if len(bindings) == 0 {
					t.Errorf("column type %q: no table named %q discovered", tc.ColumnType, tableName)
					return
				}
				if len(bindings) > 1 {
					t.Errorf("column type %q: multiple tables named %q discovered", tc.ColumnType, tableName)
					return
				}
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
				var cs = tb.CaptureSpec(t, tableName)

				// Don't sanitize anything, we're only looking for the output value field
				// and sometimes that could contain a timestamp-looking value.
				cs.Sanitizers = make(map[string]*regexp.Regexp)

				t.Run("scan", func(t *testing.T) {
					tb.Insert(ctx, t, tableName, [][]interface{}{{1, tc.InputValue}})
					var output = RunCapture(ctx, t, cs)
					verifyRoundTrip(t, tc, output)
				})

				t.Run("replication", func(t *testing.T) {
					tb.Insert(ctx, t, tableName, [][]interface{}{{2, tc.InputValue}})
					var output = RunCapture(ctx, t, cs)
					verifyRoundTrip(t, tc, output)
				})
			})
		})
	}
}

type datatypeTestRecord struct {
	Value json.RawMessage `json:"b"`
}

func verifyRoundTrip(t *testing.T, tc DatatypeTestCase, actual string) {
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

	if string(record.Value) != tc.ExpectValue {
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
