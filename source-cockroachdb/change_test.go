package main

import (
	"encoding/json"
	"testing"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/stretchr/testify/require"
)

// TestChangeEventJSON pins the hand-rolled change-event serialization. Because AppendJSON builds
// the `_meta` object by direct byte appends rather than reflection, these cases lock in its exact
// output across every branch: insert/update/delete, backfill snapshots, the optional loc/tag
// properties, and string escaping.
func TestChangeEventJSON(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event cockroachdbChangeEvent
		want  string
	}{
		{
			name: "streamed update",
			event: cockroachdbChangeEvent{
				op:       sqlcapture.UpdateOp,
				document: []byte(`{"id": 1, "name": "alice"}`),
				source: cockroachdbSource{
					SourceCommon: sqlcapture.SourceCommon{Millis: 1700000000000, Schema: "public", Table: "users"},
					Cursor:       "1781626537709301005.0000000000",
				},
			},
			want: `{"id": 1, "name": "alice","_meta":{"op":"u","source":{"ts_ms":1700000000000,"schema":"public","table":"users","loc":"1781626537709301005.0000000000"}}}`,
		},
		{
			name: "backfill insert (snapshot, no ts_ms)",
			event: cockroachdbChangeEvent{
				op:       sqlcapture.InsertOp,
				document: []byte(`{"id": 2, "name": "bob"}`),
				source: cockroachdbSource{
					SourceCommon: sqlcapture.SourceCommon{Schema: "public", Table: "users", Snapshot: true},
					Cursor:       "1781626537709301005.0000000000",
				},
			},
			want: `{"id": 2, "name": "bob","_meta":{"op":"c","source":{"schema":"public","snapshot":true,"table":"users","loc":"1781626537709301005.0000000000"}}}`,
		},
		{
			name: "delete (key-only document)",
			event: cockroachdbChangeEvent{
				op:       sqlcapture.DeleteOp,
				document: []byte(`{"id":3}`),
				source: cockroachdbSource{
					SourceCommon: sqlcapture.SourceCommon{Millis: 1700000000000, Schema: "public", Table: "users"},
					Cursor:       "1781626537709301005.0000000000",
				},
			},
			want: `{"id":3,"_meta":{"op":"d","source":{"ts_ms":1700000000000,"schema":"public","table":"users","loc":"1781626537709301005.0000000000"}}}`,
		},
		{
			name: "source tag with characters requiring escaping",
			event: cockroachdbChangeEvent{
				op:       sqlcapture.UpdateOp,
				document: []byte(`{"id": 4}`),
				source: cockroachdbSource{
					SourceCommon: sqlcapture.SourceCommon{Schema: "public", Table: "users"},
					Cursor:       "1781626537709301005.0000000000",
					Tag:          `prod"x`,
				},
			},
			want: `{"id": 4,"_meta":{"op":"u","source":{"schema":"public","table":"users","loc":"1781626537709301005.0000000000","tag":"prod\"x"}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := tc.event.AppendJSON(nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(out))
			require.True(t, json.Valid(out), "output must be valid JSON")
		})
	}
}
