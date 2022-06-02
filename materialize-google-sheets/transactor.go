package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/sheets/v4"
)

type transactor struct {
	bindings []transactorBinding
	client   *sheets.Service
	// Round is a monotonic counter of the current transaction number,
	// which is persisted into / recovered from the driver checkpoint.
	round int64
	// Spreadsheet to which we're materializing.
	spreadsheetId string
}

type transactorBinding struct {
	// All documents of this materialization binding.
	Docs []json.RawMessage
	// Fields materialized by this binding.
	Fields pf.FieldSelection
	// Column index of the `flow_internal` sheet for even/odd rounds,
	// into which documents are persisted.
	StateColumn [2]int64
	// Sheet ID of the `flow_internal` sheet.
	StateSheetId int64
	// Sheet ID of the user-facing sheet for this binding.
	UserSheetId int64
	// User-facing sheet name for this binding.
	UserSheetName string
}

func (d *transactor) Load(it *pm.LoadIterator, _, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		// We always return all documents of all bindings each transaction.
		// and don't much care which specific keys are involved.
	}
	if it.Err() != nil {
		return it.Err()
	}

	for ind, binding := range d.bindings {
		for _, doc := range binding.Docs {
			if err := loaded(ind, doc); err != nil {
				return fmt.Errorf("sending loaded: %w", err)
			}
		}
	}
	return nil
}

func (d *transactor) Prepare(_ context.Context, _ pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	// The commit of this transaction within the recovery log will permanently
	// increment the current `round`. On recovery, the next connector will
	// examine `round` to determine whether this in-progress transaction committed
	// or not, and will restore either the effects of this transaction or the prior
	// one depending on the restored `round`.
	d.round++

	return pf.DriverCheckpoint{
		DriverCheckpointJson: json.RawMessage(fmt.Sprintf("{\"round\":%v}", d.round)),
		Rfc7396MergePatch:    false,
	}, nil
}

func (d *transactor) Store(it *pm.StoreIterator) error {
	// We'll rebuild the full data-grid of each user-facing sheet from the StoreIterator.
	var userSheets = make([][]*sheets.RowData, len(d.bindings))

	for ind := range d.bindings {
		d.bindings[ind].Docs = d.bindings[ind].Docs[:0] // Truncate.

		// Marshal row of column headers.
		var headers []*sheets.CellData
		for i := range d.bindings[ind].Fields.Keys {
			headers = append(headers, &sheets.CellData{UserEnteredValue: &sheets.ExtendedValue{
				StringValue: &d.bindings[ind].Fields.Keys[i]}})
		}
		for i := range d.bindings[ind].Fields.Values {
			headers = append(headers, &sheets.CellData{UserEnteredValue: &sheets.ExtendedValue{
				StringValue: &d.bindings[ind].Fields.Values[i]}})
		}
		userSheets[ind] = append(userSheets[ind], &sheets.RowData{Values: headers})
	}

	for it.Next() {
		d.bindings[it.Binding].Docs = append(d.bindings[it.Binding].Docs, it.RawJSON)

		// Build sheet row update.
		var row = make([]*sheets.CellData, len(it.Key)+len(it.Values))

		for _, e := range it.Key {
			row = append(row, valueToCell(e))
		}
		for _, e := range it.Values {
			row = append(row, valueToCell(e))
		}

		userSheets[it.Binding] = append(userSheets[it.Binding], &sheets.RowData{Values: row})
	}

	var requests []*sheets.Request
	for ind, binding := range d.bindings {

		// Update to user-facing sheet.
		requests = append(requests, &sheets.Request{
			UpdateCells: &sheets.UpdateCellsRequest{
				Range: &sheets.GridRange{
					SheetId: binding.UserSheetId,
				},
				Rows:   userSheets[ind],
				Fields: "userEnteredValue",
			},
		})

		// We also must update documents tracked in `flow_internal`.
		// Marshal the column header...
		var header = fmt.Sprintf("%s:%d", binding.UserSheetName, d.round)
		var rows = []*sheets.RowData{
			{
				Values: []*sheets.CellData{
					{UserEnteredValue: &sheets.ExtendedValue{StringValue: &header}},
				},
			},
		}
		// ...then marshal current documents states.
		for _, rawJSON := range binding.Docs {
			var s = string(rawJSON)
			rows = append(rows, &sheets.RowData{
				Values: []*sheets.CellData{
					{UserEnteredValue: &sheets.ExtendedValue{StringValue: &s}},
				},
			})
		}

		// Update (only) the current (even vs odd) column of this binding.
		requests = append(requests, &sheets.Request{
			UpdateCells: &sheets.UpdateCellsRequest{
				Range: &sheets.GridRange{
					SheetId:          binding.StateSheetId,
					StartColumnIndex: binding.StateColumn[d.round%2],
					EndColumnIndex:   binding.StateColumn[d.round%2] + 1,
				},
				Rows:   rows,
				Fields: "userEnteredValue",
			},
		})
	}

	// Apply all subRequests in a single batch, wrapped in a retry loop
	// which handles rate limit errors from the sheets API.
	for attempt := 1; true; attempt++ {
		var _, err = d.client.Spreadsheets.BatchUpdate(d.spreadsheetId,
			&sheets.BatchUpdateSpreadsheetRequest{Requests: requests}).
			Context(it.Context()).
			Do()

		if err == nil {
			return nil
		}

		// If we encounter a rate limit error, retry after exponential back-off.
		if e, ok := err.(*googleapi.Error); ok && e.Code == 429 {
			var dur = time.Duration(1<<attempt) * time.Second
			log.Println("rate limit exceeded. backing off for ", dur)
			time.Sleep(dur)
			continue
		}

		// All other errors bail out.
		return fmt.Errorf("while updating sheets: %w", err)
	}
	panic("not reached")
}

func valueToCell(e tuple.TupleElement) *sheets.CellData {
	if e == nil {
		return &sheets.CellData{}
	} else if b, ok := e.([]byte); ok {
		var s = string(b) // This is a JSON array or object.
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{StringValue: &s},
		}
	} else {
		var s = fmt.Sprintf("%v", e) // This is a JSON scalar.
		return &sheets.CellData{
			UserEnteredValue: &sheets.ExtendedValue{StringValue: &s},
		}
	}
}

// Commit is a no-op because the recovery log is authoritative.
func (d *transactor) Commit(ctx context.Context) error {
	return nil
}

// Acknowledge is a no-op because we have already written documents into
// `flow_internal` (keyed by even/odd round) and fields into the user-facing
// sheet.
//
// We'll Recover the correct even/odd set of documents depending on the persisted
// `round`, and we over-write the entirety of the user-facing sheet each transaction.
// This WOULD NOT be safe to do if we were applying partial updates to the user-facing
// sheet, as it could reflect a transaction which ultimately failed to commit and was
// rolled back. But, since we simply overwrite its contents anyway, we don't care.
func (d *transactor) Acknowledge(context.Context) error {
	return nil
}

// Destroy is a no-op.
func (d *transactor) Destroy() {}
