package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/api/sheets/v4"
)

const (
	// Maximum number of cells allowed for any single binding to either read from the Store iterator
	// or hold in-memory in the binding's rows.
	cellsLimit = 1000000
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
}

func (b transactorBinding) columnCount() int {
	// Note: Additional column for materialization record data.
	return len(b.Fields.Keys) + len(b.Fields.Values) + 1
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		var needle = string(it.Key.Pack())
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

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
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
	for it.Next() {
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
				PackedKey: it.Key.Pack(),
				NextRound: d.round,
				NextDoc:   it.RawJSON,
			},
			cells: cells,
		})
	}

	// batchRequests collects all sub-requests made to each sheet.
	var batchRequests []*sheets.Request

	for bindInd := range d.bindings {
		var stores = stores[bindInd]

		if len(stores) == 0 {
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

		d.bindings[bindInd].rows = next
		batchRequests = append(batchRequests, addRows...)
		batchRequests = append(batchRequests, updateCells...)
	}

	if err := batchRequestWithRetry(
		it.Context(),
		d.client,
		d.spreadsheetId,
		batchRequests,
	); err != nil {
		return nil, err
	}

	return func(_ context.Context, _ []byte, _ <-chan struct{}) (*pf.DriverCheckpoint, pf.OpFuture) {
		return &pf.DriverCheckpoint{
			DriverCheckpointJson: json.RawMessage(fmt.Sprintf("{\"round\":%v}", d.round)),
			Rfc7396MergePatch:    false,
		}, nil
	}, nil
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
