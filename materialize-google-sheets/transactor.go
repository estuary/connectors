package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"go.gazette.dev/core/message"
	"google.golang.org/api/sheets/v4"
)

const (
	// Maximum number of cells allowed for any single binding to either read from the Store iterator
	// or hold in-memory in the binding's rows.
	cellsLimit = 1000000

	// Accommodate API rate limits.
	transactionDelay = 2 * time.Second
)

func checkCellCount(rows int, cellsPerRow int) error {
	if rows*cellsPerRow > cellsLimit {
		return fmt.Errorf(
			"Maximum cells limit of %d exceeded for this materialization. If you are materializing "+
				"a collection with a large number of unique keys, consider creating a derivation to "+
				"transform your data into a collection with fewer unique keys.",
			cellsLimit,
		)
	}

	return nil
}

type transactor struct {
	bindings []transactorBinding
	client   *sheets.Service
	// Round is a monotonic counter of the current transaction number,
	// which is persisted into / recovered from the driver checkpoint.
	round int64
	// Spreadsheet to which we're materializing.
	spreadsheetId string
	be            *m.BindingEvents

	// truncateAfterBackfill permits marking the rows published before a
	// completed backfill deleted.
	truncateAfterBackfill bool
	// truncations maps a binding index to the boundary of a completed
	// backfill, for marking within this transaction's stores.
	truncations map[int]time.Time
}

type transactorRow struct {
	PackedKey string
	Doc       json.RawMessage
	Round     int64
}

type transactorBinding struct {
	rows []transactorRow
	// Fields materialized by this binding.
	Fields pf.FieldSelection
	// Sheet ID of the user-facing sheet for this binding.
	UserSheetId int64
	// User-facing sheet name for this binding.
	UserSheetName string
	// opColumn is the sheet column of the selected field projecting
	// /_meta/op, or -1 if the binding excludes it.
	opColumn int64
}

func (b transactorBinding) columnCount() int {
	// Note: Additional column for materialization record data.
	return len(b.Fields.Keys) + len(b.Fields.Values) + 1
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Flush(_ context.Context, completes map[int]time.Time) error {
	t.truncations = make(map[int]time.Time, len(completes))
	for binding, boundary := range completes {
		var b = &t.bindings[binding]
		if !t.truncateAfterBackfill {
			log.WithFields(log.Fields{
				"sheet":    b.UserSheetName,
				"boundary": boundary,
			}).Info("rows published before the backfill were not marked deleted because feature flags disable it")
			continue
		} else if b.opColumn < 0 {
			t.be.Warn(b.UserSheetName, fmt.Sprintf("Rows published before the backfill of sheet %q were not marked deleted because the binding excludes the _meta/op field, which must be included to mark them.", b.UserSheetName))
			continue
		}
		t.truncations[binding] = boundary
	}
	return nil
}

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	it.WaitForAcknowledged()

	for it.Next() {
		var needle = string(it.PackedKey)
		var rows = d.bindings[it.Binding].rows

		var ind = sort.Search(len(rows), func(i int) bool {
			return rows[i].PackedKey >= needle
		})

		if ind == len(rows) || rows[ind].PackedKey != needle || rows[ind].Round == 0 {
			// Key is not matched or is a rolled-back placeholder.
		} else if err := loaded(it.Binding, rows[ind].Doc); err != nil {
			return fmt.Errorf("sending loaded: %w", err)
		}
	}

	return it.Err()
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	started := time.Now()

	// The commit of this transaction within the recovery log will permanently
	// increment the current `round`. On recovery, the next connector will
	// examine `round` to determine whether this in-progress transaction committed
	// or not, and will restore either the effects of this transaction or the prior
	// one depending on the restored `round`.
	d.round++

	type storedRow struct {
		RowState
		cells []*sheets.CellData
	}
	var stores = make([][]storedRow, len(d.bindings))

	// Gather all of the stored rows on a per-binding basis.
	for it.Next(false) {
		// Verify that we don't read an excessive amount of data from the store iterator, which
		// would indicate we are reading from a high cardinality collection that will not fit into a
		// reasonable amount of connector memory.
		if err := checkCellCount(len(stores[it.Binding]), d.bindings[it.Binding].columnCount()); err != nil {
			return nil, err
		}

		// Marshal key and value fields into cells of the row.
		// cells[0] is a placeholder for internal state that's written later.
		var cells = make([]*sheets.CellData, 1, 1+len(it.Key)+len(it.Values))
		for _, e := range it.Key {
			cells = append(cells, valueToCell(e))
		}
		for _, e := range it.Values {
			cells = append(cells, valueToCell(e))
		}

		stores[it.Binding] = append(stores[it.Binding], storedRow{
			RowState: RowState{
				PackedKey: it.PackedKey,
				NextRound: d.round,
				NextDoc:   it.RawJSON,
			},
			cells: cells,
		})
	}

	// batchRequests collects all sub-requests made to each sheet.
	var batchRequests []*sheets.Request
	// rowStats maps a binding index to what this transaction does to its
	// sheet, reported once the batch is applied.
	var rowStats = make(map[int]m.RowStats, len(d.bindings))

	for bindInd := range d.bindings {
		var stores = stores[bindInd]
		var boundary, truncating = d.truncations[bindInd]

		if len(stores) == 0 && !truncating {
			continue
		}

		// Ensure `stored` is in sorted order.
		// A precondition is that `prevRows` is already sorted,
		// as its always produced through a merge-join.
		sort.Slice(stores, func(i, j int) bool { return bytes.Compare(stores[i].PackedKey, stores[j].PackedKey) == -1 })

		// We'll do an ordered merge of `prev` and `stores`, producing new rows into `next`.
		var pi, si int
		var prev = d.bindings[bindInd].rows
		var next = make([]transactorRow, 0, len(prev)+len(stores))

		// As we go we'll build up batches of requests which first add rows as required
		// for keys, and which then update cells to stored values using their after-add
		// adjusted row indexes.
		var addRows, updateCells []*sheets.Request

		// A store over a rolled-back placeholder row counts as an insert, as
		// Load reported no document for its key.
		var inserted, updated int64

		for pi != len(prev) || si != len(stores) {
			// Verify that our in-memory view of the sheet has not grown excessively large would
			// indicate we are reading from a high cardinality collection that will not fit into a
			// reasonable amount of connector memory.
			if err := checkCellCount(len(next)+len(stores), d.bindings[bindInd].columnCount()); err != nil {
				return nil, err
			}

			// Compare next `prev` vs `stores`.
			var cmp int
			if si == len(stores) {
				cmp = -1 // Implicit less.
			} else if pi == len(prev) {
				cmp = 1 // Implicit greater.
			} else if prev[pi].PackedKey < string(stores[si].PackedKey) {
				cmp = -1
			} else if prev[pi].PackedKey > string(stores[si].PackedKey) {
				cmp = 1
			}

			if cmp == -1 {
				// Prev row is not changed by this round.
				next = append(next, prev[pi])
				pi++
				continue
			}

			// Row is being updated or inserted.
			// Its sheet row index is 1-indexed due to its header.
			var rowInd = int64(len(next) + 1)
			var s = stores[si]
			si++

			next = append(next, transactorRow{
				PackedKey: string(s.PackedKey),
				Doc:       s.NextDoc,
				Round:     s.NextRound,
			})

			// Does a `prev` row corresponding with this store document exist?
			// If so include this prior version in the state.
			if cmp == 0 && prev[pi].Round != 0 {
				updated++
			} else {
				inserted++
			}

			if cmp == 0 {
				s.PrevDoc = prev[pi].Doc
				s.PrevRound = prev[pi].Round
				pi++
			} else if l := len(addRows); l != 0 && addRows[l-1].InsertDimension.Range.EndIndex == rowInd {
				// We're inserting a new row, and we can extend a current run of added rows.
				addRows[l-1].InsertDimension.Range.EndIndex++
			} else {
				// We must start a new run of added rows.
				addRows = append(addRows, &sheets.Request{
					InsertDimension: &sheets.InsertDimensionRequest{
						Range: &sheets.DimensionRange{
							SheetId:    d.bindings[bindInd].UserSheetId,
							Dimension:  "ROWS",
							StartIndex: rowInd,
							EndIndex:   rowInd + 1,
						},
					},
				})
			}

			// Marshal internal prev / next document state into cells[0].
			var col0, err = json.Marshal(s)
			if err != nil {
				panic(err)
			}
			s.cells[0] = valueToCell(col0)

			// We're updating a row. Can we extend a current row run?
			if l := len(updateCells); l != 0 && updateCells[l-1].UpdateCells.Range.EndRowIndex == rowInd {
				updateCells[l-1].UpdateCells.Rows = append(
					updateCells[l-1].UpdateCells.Rows,
					&sheets.RowData{Values: s.cells})
				updateCells[l-1].UpdateCells.Range.EndRowIndex++
			} else {
				// We must start a new run of updated rows.
				updateCells = append(updateCells, &sheets.Request{
					UpdateCells: &sheets.UpdateCellsRequest{
						Range: &sheets.GridRange{
							SheetId:       d.bindings[bindInd].UserSheetId,
							StartRowIndex: rowInd,
							EndRowIndex:   rowInd + 1,
						},
						Rows:   []*sheets.RowData{{Values: s.cells}},
						Fields: "userEnteredValue",
					},
				})
			}

		} // Done with merge of `prev` and `stored` into `next`.

		batchRequests = append(batchRequests, addRows...)
		batchRequests = append(batchRequests, updateCells...)
		var stats = m.ExactRowStats(inserted, updated, 0)

		// Marks follow the inserts and updates, so their row indexes address
		// the sheet as `next` lays it out.
		if truncating {
			var b = &d.bindings[bindInd]
			marks, marked, err := markStaleRowsDeleted(next, d.round, boundary, b.UserSheetId, b.opColumn)
			if err != nil {
				return nil, fmt.Errorf("marking rows of sheet %q deleted after backfill: %w", b.UserSheetName, err)
			}
			batchRequests = append(batchRequests, marks...)
			stats = stats.WithTruncated(marked)
		}

		d.bindings[bindInd].rows = next
		rowStats[bindInd] = stats
	}

	// InsertDimension shifts rows down, so repeating a batch that may have been
	// applied would insert its rows twice.
	if err := batchUpdate(
		it.Context(),
		requestRetry,
		d.client,
		d.spreadsheetId,
		batchRequests,
	); err != nil {
		return nil, err
	}

	for bindInd, stats := range rowStats {
		var b = &d.bindings[bindInd]
		if stats.Truncated != nil {
			log.WithFields(log.Fields{
				"sheet":    b.UserSheetName,
				"boundary": d.truncations[bindInd],
				"marked":   *stats.Truncated,
			}).Info("marked rows published before the backfill deleted")
		}
		d.be.ReportRowStats(it.Round, []string{b.UserSheetName}, stats)
	}
	clear(d.truncations)

	// Ensure transactions are spread out enough to not exceed google sheets rate limits.
	<-time.After(transactionDelay - time.Since(started))

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return &pf.ConnectorState{
			UpdatedJson: json.RawMessage(fmt.Sprintf("{\"round\":%v}", d.round)),
			MergePatch:  false,
		}, nil
	}, nil
}

// markStaleRowsDeleted returns the UpdateCells requests that set the opColumn
// cell of each row published before boundary to "d", and how many rows
// they mark. Only that cell changes: the row and its document are kept. The
// sheet row of rows[i] is i+1, below the header. Rows stored in round,
// rolled-back placeholders, and rows whose document is already a deletion are
// not marked.
func markStaleRowsDeleted(rows []transactorRow, round int64, boundary time.Time, sheetID, opColumn int64) ([]*sheets.Request, int64, error) {
	var requests []*sheets.Request
	var marked int64

	for i, row := range rows {
		if row.Round == 0 || row.Round == round {
			continue
		}
		meta, err := readDocMeta(row.Doc)
		if err != nil {
			return nil, 0, err
		} else if meta.op == "d" || !meta.publishedAt.Before(boundary) {
			continue
		}
		marked++

		// One request covers each run of adjacent marked rows: a row directly
		// below the previous request's range extends it rather than starting
		// a new one.
		var sheetRow = int64(i + 1)
		var cells = &sheets.RowData{Values: []*sheets.CellData{valueToCell("d")}}
		if l := len(requests); l != 0 && requests[l-1].UpdateCells.Range.EndRowIndex == sheetRow {
			requests[l-1].UpdateCells.Rows = append(requests[l-1].UpdateCells.Rows, cells)
			requests[l-1].UpdateCells.Range.EndRowIndex++
			continue
		}
		requests = append(requests, &sheets.Request{
			UpdateCells: &sheets.UpdateCellsRequest{
				Range: &sheets.GridRange{
					SheetId:          sheetID,
					StartRowIndex:    sheetRow,
					EndRowIndex:      sheetRow + 1,
					StartColumnIndex: opColumn,
					EndColumnIndex:   opColumn + 1,
				},
				Rows:   []*sheets.RowData{cells},
				Fields: "userEnteredValue",
			},
		})
	}

	return requests, marked, nil
}

// docMeta is the part of a document's /_meta that marking stale rows needs.
type docMeta struct {
	publishedAt time.Time
	op          string
}

// readDocMeta returns the publication time carried by the /_meta/uuid of doc
// along with its /_meta/op.
func readDocMeta(doc json.RawMessage) (docMeta, error) {
	var parsed struct {
		Meta struct {
			UUID string `json:"uuid"`
			Op   string `json:"op"`
		} `json:"_meta"`
	}
	if err := json.Unmarshal(doc, &parsed); err != nil {
		return docMeta{}, fmt.Errorf("parsing document: %w", err)
	} else if parsed.Meta.UUID == "" {
		return docMeta{}, fmt.Errorf("document has no /_meta/uuid to compare with the backfill boundary")
	}

	id, err := uuid.Parse(parsed.Meta.UUID)
	if err != nil {
		return docMeta{}, fmt.Errorf("parsing /_meta/uuid %q: %w", parsed.Meta.UUID, err)
	}
	return docMeta{publishedAt: message.GetClock(id).AsTime(), op: parsed.Meta.Op}, nil
}

func valueToCell(e tuple.TupleElement) *sheets.CellData {
	switch ee := e.(type) {
	case nil:
		return &sheets.CellData{}
	case []byte:
		var s = string(ee) // This is a JSON array or object.
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{StringValue: &s},
		}
	case string:
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{StringValue: &ee},
		}
	case int64:
		var f = float64(ee)
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{NumberValue: &f},
		}
	case uint64:
		var f = float64(ee)
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{NumberValue: &f},
		}
	case float32:
		var f = float64(ee)
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{NumberValue: &f},
		}
	case float64:
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{NumberValue: &ee},
		}
	case bool:
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{BoolValue: &ee},
		}
	default:
		panic(fmt.Sprintf("unhandled type %#v", e))
	}
}

// Destroy is a no-op.
func (d *transactor) Destroy() {}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}
