package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/sheets/v4"
)

// buildTransactorBindings fetches documents of each binding for the committed `round`,
// and returns bindings ready for use within the transactor.
func buildTransactorBindings(
	bindings []*pf.MaterializationSpec_Binding,
	loadRound int64,
	states []SheetState,
) ([]transactorBinding, error) {

	var out []transactorBinding
	for bindInd, binding := range bindings {
		var state = states[bindInd]

		var rows []transactorRow
		for rowInd, row := range state.Rows {

			var err error
			switch {
			case rowInd != 0 && bytes.Compare(state.Rows[rowInd-1].PackedKey, state.Rows[rowInd].PackedKey) != -1:
				err = fmt.Errorf("row keys are not in ascending order")
			case row.NextRound != 0 && row.NextRound <= row.PrevRound:
				err = fmt.Errorf("NextRound must be greater than PrevRound")
			case row.PrevRound > loadRound:
				err = fmt.Errorf("PrevRound must have been committed (less or equal to loadRound)")
			case row.NextRound != 0 && row.NextRound <= loadRound:
				// NextRound has already committed and is used.
				rows = append(rows, transactorRow{
					PackedKey: string(row.PackedKey),
					Doc:       row.NextDoc,
					Round:     row.NextRound,
				})
			case row.PrevRound != 0:
				// Restore PrevRound.
				rows = append(rows, transactorRow{
					PackedKey: string(row.PackedKey),
					Doc:       row.PrevDoc,
					Round:     row.PrevRound,
				})
			case row.PrevRound == 0:
				// This row was introduced by NextRound, which is rolled back.
				rows = append(rows, transactorRow{
					PackedKey: string(row.PackedKey),
					Doc:       nil,
					Round:     0,
				})
			default:
				panic("not reached")
			}

			if err != nil {
				return nil, fmt.Errorf(
					"sheet %q row %d with row state PrevRound=%d, NextRound=%d, and loadRound=%d: %w",
					state.SheetName, rowInd, row.PrevRound, row.NextRound, loadRound, err)
			}
		}

		out = append(out, transactorBinding{
			rows:          rows,
			Fields:        binding.FieldSelection,
			UserSheetId:   state.SheetID,
			UserSheetName: state.SheetName,
		})
	}

	return out, nil
}

// SheetState is the recovered state of a materialized sheet.
type SheetState struct {
	SheetID   int64
	SheetName string
	Rows      []RowState
}

// RowState is the recovered state of a materialized sheet row.
// It represents up to two versions of a document: its value as-of
// a `PrevRound` and as-of a `NextRound`.
//
// On recovery a recovered document is selected depending on whether
// `NextRound` is known to have committed, or to have been rolled-back.
type RowState struct {
	PackedKey []byte          `json:"k"`
	PrevRound int64           `json:"pr,omitempty"`
	PrevDoc   json.RawMessage `json:"pd,omitempty"`
	NextRound int64           `json:"nr"`
	NextDoc   json.RawMessage `json:"nd,omitempty"`
}

// loadSheetStates loads the SheetStates of all named `sheetNames`.
func loadSheetStates(
	bindings []*pf.MaterializationSpec_Binding,
	client *sheets.Service,
	spreadsheetID string,
) ([]SheetState, error) {

	var sheetNames, sheetRanges []string
	for _, binding := range bindings {
		sheetNames = append(sheetNames, binding.ResourcePath[0])
		// Fetch from A2 to skip the (empty) header.
		sheetRanges = append(sheetRanges, fmt.Sprintf("'%s'!A2:A", binding.ResourcePath[0]))
	}

	var resp, err = client.Spreadsheets.
		Get(spreadsheetID).
		Ranges(sheetRanges...).
		Fields("sheets(properties(title,sheetId),data(startColumn,startRow,rowData(values(userEnteredValue))))").
		Do()

	if err != nil {
		return nil, fmt.Errorf("fetching sheet states: %w", err)
	} else if l1, l2 := len(resp.Sheets), len(sheetNames); l1 != l2 {
		return nil, fmt.Errorf("wrong number of sheets: %d but expected one each for %v",
			l1, sheetNames)
	}

	var states []SheetState

	for sheetInd, sheet := range resp.Sheets {
		var state = SheetState{
			SheetName: sheet.Properties.Title,
			SheetID:   sheet.Properties.SheetId,
		}

		if state.SheetName != sheetNames[sheetInd] {
			return nil, fmt.Errorf("response sheets in unexpected order: %s but expected %s",
				state.SheetName, sheetNames[sheetInd])
		} else if ll := len(sheet.Data); ll != 1 {
			return nil, fmt.Errorf("wrong number of sheet data grids: %d but expected 1", ll)
		}

		// Recover document states from this sheet column.
		for rowInd, row := range sheet.Data[0].RowData {
			var rowState RowState

			if len(row.Values) == 0 {
				return nil, fmt.Errorf("row %d of range %s is missing", rowInd, sheetRanges[sheetInd])
			} else if uev := row.Values[0].UserEnteredValue; uev == nil || uev.StringValue == nil {
				return nil, fmt.Errorf("row %d of range %s is not a string", rowInd, sheetRanges[sheetInd])
			} else if err := json.Unmarshal([]byte(*uev.StringValue), &rowState); err != nil {
				return nil, fmt.Errorf("row %d of range %s is not a valid state: %w", rowInd, sheetRanges[sheetInd], err)
			} else {
				state.Rows = append(state.Rows, rowState)
			}
		}
		states = append(states, state)
	}

	return states, nil
}

// loadSheetIDMapping retrieves a mapping of sheet titles to their integer API IDs.
func loadSheetIDMapping(client *sheets.Service, spreadsheetID string) (map[string]int64, error) {
	var metadata, err = client.Spreadsheets.
		Get(spreadsheetID).
		Fields("sheets(properties(title,sheetId))").
		Do()
	if err != nil {
		return nil, fmt.Errorf("fetching sheet metadata: %w", err)
	}

	var out = make(map[string]int64)
	for _, sheet := range metadata.Sheets {
		out[sheet.Properties.Title] = sheet.Properties.SheetId
	}

	return out, nil
}

// batchRequestWithRetry applies all `requests`` in a single batch,
// wrapped in a retry loop which handles rate limit errors from the sheets API.
func batchRequestWithRetry(
	ctx context.Context,
	client *sheets.Service,
	spreadsheetID string,
	requests []*sheets.Request,
) error {
	for attempt := 1; true; attempt++ {
		var _, err = client.Spreadsheets.BatchUpdate(spreadsheetID,
			&sheets.BatchUpdateSpreadsheetRequest{Requests: requests}).
			Context(ctx).
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
		return err
	}
	panic("not reached")
}

// Example: https://docs.google.com/spreadsheets/d/1s7S1Abp8kAJEkReV10omef_ETZXKB2vHKPook49HpFk/edit#gid=1649530432
const sheetsLinkRe = `^https://docs.google.com/spreadsheets/d/([\w\d_\-]+)/`

func parseSheetsID(sheetsURL string) (string, error) {
	var matches = regexp.MustCompile(sheetsLinkRe).FindStringSubmatch(sheetsURL)

	if len(matches) != 2 || len(matches[1]) == 0 {
		return "", fmt.Errorf("invalid Google Sheets URL: %s", sheetsURL)
	}
	return matches[1], nil
}
