package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/airbyte"
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
		t.Run(fmt.Sprintf("%d_%s", idx, sanitizeName(tc.ColumnType)), func(t *testing.T) {
			var table = tb.CreateTable(ctx, t, "", fmt.Sprintf("(a INTEGER PRIMARY KEY, b %s)", tc.ColumnType))
			var stream *airbyte.Stream

			// Perform discovery and verify that the generated JSON schema looks correct
			t.Run("discovery", func(t *testing.T) {
				var discoveredCatalog, err = sqlcapture.DiscoverCatalog(ctx, tb.GetDatabase())
				require.NoError(t, err)

				for idx := range discoveredCatalog.Streams {
					if strings.EqualFold(discoveredCatalog.Streams[idx].Name, table) {
						stream = &discoveredCatalog.Streams[idx]
						break
					}
				}
				if stream == nil {
					t.Errorf("column type %q: no stream named %q discovered", tc.ColumnType, table)
					return
				}

				var skimmed = struct {
					Definitions map[string]struct {
						Properties map[string]json.RawMessage
					} `json:"$defs"`
				}{}
				require.NoError(t, json.Unmarshal(stream.JSONSchema, &skimmed))
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
				var catalog = airbyte.ConfiguredCatalog{
					Streams: []airbyte.ConfiguredStream{
						{Stream: *stream},
					},
				}
				var state = sqlcapture.PersistentState{}

				t.Run("scan", func(t *testing.T) {
					tb.Insert(ctx, t, table, [][]interface{}{{1, tc.InputValue}})
					var output, _ = PerformCapture(ctx, t, tb, &catalog, &state, 1, "")
					verifyRoundTrip(t, tc, output)
				})

				t.Run("replication", func(t *testing.T) {
					tb.Insert(ctx, t, table, [][]interface{}{{2, tc.InputValue}})
					var output, _ = PerformCapture(ctx, t, tb, &catalog, &state, 1, "")
					verifyRoundTrip(t, tc, output)
				})
			})
		})
	}
}

type datatypeTestRecord struct {
	Stream string `json:"stream"`
	Data   struct {
		Value json.RawMessage `json:"b"`
	} `json:"data"`
}

func verifyRoundTrip(t *testing.T, tc DatatypeTestCase, actual string) {
	t.Helper()

	// Extract the value record from the full output
	var record *datatypeTestRecord
	for _, line := range strings.Split(actual, "\n") {
		if strings.HasPrefix(line, `{"type":"RECORD","record":`) && strings.HasSuffix(line, `}`) {
			line = strings.TrimPrefix(line, `{"type":"RECORD","record":`)
			line = strings.TrimSuffix(line, `}`)

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

	if string(record.Data.Value) != tc.ExpectValue {
		t.Errorf("result mismatch for type %q: input %q, got %q, expected %q",
			tc.ColumnType, tc.InputValue, string(record.Data.Value), tc.ExpectValue)
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
