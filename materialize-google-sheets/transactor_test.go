package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

func TestFlushTruncations(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name     string
		flags    string
		opColumn int64
		want     map[int]time.Time
	}{
		{name: "default", flags: "", opColumn: 3, want: map[int]time.Time{0: boundary}},
		{name: "flag disabled", flags: "no_truncate_after_backfill", opColumn: 3, want: map[int]time.Time{}},
		{name: "op excluded", flags: "", opColumn: -1, want: map[int]time.Time{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg = config{Advanced: advancedConfig{FeatureFlags: tc.flags}}
			flags, err := boilerplate.ResolveFlags(cfg, &pf.MaterializationSpec{})
			require.NoError(t, err)
			var d = &transactor{
				bindings:              []transactorBinding{{UserSheetName: "sheet", opColumn: tc.opColumn}},
				truncateAfterBackfill: flags["truncate_after_backfill"],
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestOpColumn(t *testing.T) {
	var binding = &pf.MaterializationSpec_Binding{
		Collection: pf.CollectionSpec{Projections: []pf.Projection{
			{Field: "_meta/op", Ptr: "/_meta/op"},
			{Field: "flow_document", Ptr: ""},
			{Field: "id", Ptr: "/id"},
			{Field: "operation", Ptr: "/_meta/op"},
			{Field: "val", Ptr: "/val"},
		}},
		FieldSelection: pf.FieldSelection{
			Keys:     []string{"id"},
			Values:   []string{"val", "operation"},
			Document: "flow_document",
		},
	}
	// Column 0 holds the row state, then "id", "val" and "operation".
	require.Equal(t, int64(3), opColumn(binding))

	binding.FieldSelection.Values = []string{"val"}
	require.Equal(t, int64(-1), opColumn(binding))
}

func TestMarkStaleRowsDeleted(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	var stale = docAt(boundary.Add(-time.Hour), "u")
	var fresh = docAt(boundary.Add(time.Hour), "u")
	const round = 7

	var rows = []transactorRow{
		{PackedKey: "a", Doc: stale, Round: 3},                                // sheet row 1: marked
		{PackedKey: "b", Doc: stale, Round: 4},                                // sheet row 2: marked
		{PackedKey: "c", Doc: fresh, Round: 5},                                // sheet row 3: published after the boundary
		{PackedKey: "d", Doc: nil, Round: 0},                                  // sheet row 4: placeholder
		{PackedKey: "e", Doc: stale, Round: round},                            // sheet row 5: stored this round
		{PackedKey: "f", Doc: fresh, Round: 2},                                // sheet row 6: published after the boundary
		{PackedKey: "g", Doc: stale, Round: 6},                                // sheet row 7: marked
		{PackedKey: "h", Doc: docAt(boundary, "u"), Round: 6},                 // sheet row 8: at the boundary
		{PackedKey: "i", Doc: docAt(boundary.Add(-time.Hour), "d"), Round: 6}, // sheet row 9: already a deletion
	}

	requests, marked, err := markStaleRowsDeleted(rows, round, boundary, 42, 3)
	require.NoError(t, err)
	require.Equal(t, int64(3), marked)

	var d = "d"
	var mark = func() *sheets.RowData {
		return &sheets.RowData{Values: []*sheets.CellData{{UserEnteredValue: &sheets.ExtendedValue{StringValue: &d}}}}
	}
	require.Equal(t, []*sheets.Request{
		{UpdateCells: &sheets.UpdateCellsRequest{
			Range: &sheets.GridRange{
				SheetId: 42, StartRowIndex: 1, EndRowIndex: 3, StartColumnIndex: 3, EndColumnIndex: 4,
			},
			Rows:   []*sheets.RowData{mark(), mark()},
			Fields: "userEnteredValue",
		}},
		{UpdateCells: &sheets.UpdateCellsRequest{
			Range: &sheets.GridRange{
				SheetId: 42, StartRowIndex: 7, EndRowIndex: 8, StartColumnIndex: 3, EndColumnIndex: 4,
			},
			Rows:   []*sheets.RowData{mark()},
			Fields: "userEnteredValue",
		}},
	}, requests)
}

func TestMarkStaleRowsDeletedErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		doc  string
		want string
	}{
		{name: "missing uuid", doc: `{"x":1}`, want: "document has no /_meta/uuid"},
		{name: "invalid uuid", doc: `{"_meta":{"uuid":"not-a-uuid"}}`, want: "parsing /_meta/uuid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rows = []transactorRow{{PackedKey: "a", Doc: json.RawMessage(tc.doc), Round: 1}}
			_, _, err := markStaleRowsDeleted(rows, 2, time.Now(), 42, 3)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func docAt(t time.Time, op string) json.RawMessage {
	var id = message.BuildUUID(message.ProducerID{1, 2, 3, 4, 5, 6}, message.NewClock(t), message.Flag_ACK_TXN)
	return json.RawMessage(fmt.Sprintf(`{"_meta":{"op":%q,"uuid":%q},"x":1}`, op, id.String()))
}

func TestStoreMarksAfterInserts(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	var stale = docAt(boundary.Add(-time.Hour), "u")
	var fresh = docAt(boundary.Add(time.Hour), "u")
	const sheetID, opCol = 42, 3

	var batches []*sheets.BatchUpdateSpreadsheetRequest
	var srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req sheets.BatchUpdateSpreadsheetRequest
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		batches = append(batches, &req)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	client, err := sheets.NewService(t.Context(), option.WithEndpoint(srv.URL+"/"), option.WithHTTPClient(srv.Client()))
	require.NoError(t, err)

	var key = func(k string) []byte { return tuple.Tuple{k}.Pack() }
	var d = &transactor{
		bindings: []transactorBinding{{
			rows: []transactorRow{
				{PackedKey: string(key("a")), Doc: stale, Round: 3},
				{PackedKey: string(key("c")), Doc: stale, Round: 4},
				{PackedKey: string(key("e")), Doc: stale, Round: 5},
			},
			Fields:        pf.FieldSelection{Keys: []string{"id"}, Values: []string{"val", "_meta/op"}},
			UserSheetId:   sheetID,
			UserSheetName: "sheet",
			opColumn:      opCol,
		}},
		client:                client,
		round:                 5,
		spreadsheetId:         "spreadsheet",
		be:                    m.NewBindingEvents(),
		truncateAfterBackfill: true,
	}
	require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary}))

	// "b" is inserted between the stale rows "a" and "c", shifting "c" down a
	// row. "e" is updated in this round, so it is not marked.
	var store = func(k string) *pm.Request {
		return &pm.Request{Store: &pm.Request_Store{
			KeyPacked:    key(k),
			ValuesPacked: tuple.Tuple{"v", "u"}.Pack(),
			DocJson:      fresh,
		}}
	}
	var stream = &storeStream{reqs: []*pm.Request{store("b"), store("e")}}
	var it = m.NewStoreIterator(t.Context(), stream, &pm.Request{Flush: &pm.Request_Flush{}})

	_, err = d.Store(it)
	require.NoError(t, err)
	require.NoError(t, it.Err())
	require.Len(t, batches, 1)

	var marks []*sheets.GridRange
	var inserts []*sheets.DimensionRange
	for _, req := range batches[0].Requests {
		if req.InsertDimension != nil {
			inserts = append(inserts, req.InsertDimension.Range)
		} else if r := req.UpdateCells.Range; r.EndColumnIndex != 0 {
			marks = append(marks, r)
		}
	}
	require.Equal(t, []*sheets.DimensionRange{
		{SheetId: sheetID, Dimension: "ROWS", StartIndex: 2, EndIndex: 3},
	}, inserts)
	require.Equal(t, []*sheets.GridRange{
		{SheetId: sheetID, StartRowIndex: 1, EndRowIndex: 2, StartColumnIndex: opCol, EndColumnIndex: opCol + 1},
		{SheetId: sheetID, StartRowIndex: 3, EndRowIndex: 4, StartColumnIndex: opCol, EndColumnIndex: opCol + 1},
	}, marks)

	// Marks follow the inserts, which shift the rows they address.
	var last = batches[0].Requests[len(batches[0].Requests)-1]
	require.NotNil(t, last.UpdateCells)
	require.Equal(t, int64(opCol), last.UpdateCells.Range.StartColumnIndex)

	var keys []string
	for _, row := range d.bindings[0].rows {
		keys = append(keys, row.PackedKey)
	}
	require.Equal(t, []string{string(key("a")), string(key("b")), string(key("c")), string(key("e"))}, keys)
	require.Empty(t, d.truncations)
}

// storeStream replays reqs to a StoreIterator, then ends the stores.
type storeStream struct {
	reqs []*pm.Request
}

func (s *storeStream) Send(*pm.Response) error { return nil }

func (s *storeStream) RecvMsg(r *pm.Request) error {
	if len(s.reqs) == 0 {
		*r = pm.Request{StartCommit: &pm.Request_StartCommit{}}
		return nil
	}
	*r = *s.reqs[0]
	s.reqs = s.reqs[1:]
	return nil
}
