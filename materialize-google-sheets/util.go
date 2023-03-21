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
				err = fmt.Errorf("PrevRound must have been committed (less or equal to loadRound). This most commonly happens if there are multiple materializations writing to the same sheet. To ensure consistency, each sheet must be written to by a single materialization only, this includes materializations that have been disabled.")
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

func writeSheetHeaders(ctx context.Context, client *sheets.Service, spreadsheetID string, bindings []transactorBinding) error {
	var actions []*sheets.Request
	for _, binding := range bindings {
		var headers = []*sheets.CellData{{}}
		for i := range binding.Fields.Keys {
			headers = append(headers, &sheets.CellData{
				UserEnteredValue: &sheets.ExtendedValue{StringValue: &binding.Fields.Keys[i]},
			})
		}
		for i := range binding.Fields.Values {
			headers = append(headers, &sheets.CellData{
				UserEnteredValue: &sheets.ExtendedValue{StringValue: &binding.Fields.Values[i]},
			})
		}
		actions = append(actions,
			// Resize horizontally to the correct number of fields, and freeze the header row
			&sheets.Request{
				UpdateSheetProperties: &sheets.UpdateSheetPropertiesRequest{
					Fields: "gridProperties(columnCount,frozenRowCount)",
					Properties: &sheets.SheetProperties{
						SheetId: binding.UserSheetId,
						GridProperties: &sheets.GridProperties{
							ColumnCount:    int64(len(headers)),
							FrozenRowCount: 1,
						},
					},
				},
			},
			// Write header names to row 0
			&sheets.Request{
				UpdateCells: &sheets.UpdateCellsRequest{
					Range: &sheets.GridRange{
						SheetId:       binding.UserSheetId,
						StartRowIndex: 0,
						EndRowIndex:   1,
					},
					Rows:   []*sheets.RowData{{Values: headers}},
					Fields: "userEnteredValue",
				},
			},
			// Hide the first column, which contains Flow internal data
			&sheets.Request{
				UpdateDimensionProperties: &sheets.UpdateDimensionPropertiesRequest{
					Range: &sheets.DimensionRange{
						SheetId:    binding.UserSheetId,
						Dimension:  "COLUMNS",
						StartIndex: 0,
						EndIndex:   1,
					},
					Properties: &sheets.DimensionProperties{
						HiddenByUser: true,
					},
					Fields: "hiddenByUser",
				},
			})

	}
	return batchRequestWithRetry(ctx, client, spreadsheetID, actions)
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

const sentinelValue = `{}`

// loadSheetStates loads the SheetStates of all named `sheetNames`.
// each row in a sheet has a zero-indexed cell where RowState is written to,
// this zero-indexed cell is not visible to users when they look at the sheet
// but we have it available when using the API
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

	// Arrange returned sheet data in the same order as configured bindings. A quadratic loop here
	// should be fine since the number of bindings & sheets will not be very large.
	var responseSheets []*sheets.Sheet
	for _, bindingSheetName := range sheetNames {
		var matched bool
		for _, sheet := range resp.Sheets {
			if sheet.Properties.Title == bindingSheetName {
				responseSheets = append(responseSheets, sheet)
				matched = true
				break
			}
		}

		if !matched {
			return nil, fmt.Errorf("sheet response missing expected sheet %q", bindingSheetName)
		}
	}

	var states []SheetState

	for sheetInd, sheet := range responseSheets {
		var state = SheetState{
			SheetName: sheet.Properties.Title,
			SheetID:   sheet.Properties.SheetId,
		}

		if ll := len(sheet.Data); ll != 1 {
			return nil, fmt.Errorf("wrong number of sheet data grids: %d but expected 1", ll)
		}

		// Recover document states from this sheet column.
		for rowInd, row := range sheet.Data[0].RowData {
			var rowState RowState

			if len(row.Values) == 0 {
				return nil, fmt.Errorf("row %d of range %s is missing", rowInd, sheetRanges[sheetInd])
			} else if uev := row.Values[0].UserEnteredValue; uev == nil || uev.StringValue == nil {
				return nil, fmt.Errorf("row %d of range %s is not a string", rowInd, sheetRanges[sheetInd])
			} else if *uev.StringValue == sentinelValue {
				break // Sentinel value indicating the end of the materialization data
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

func writeSheetSentinels(ctx context.Context, client *sheets.Service, spreadsheetID string, states []SheetState) error {
	var updates []*sheets.Request
	for _, state := range states {
		// The sentinel should come after 1 header row and N data rows
		var sentinelIndex = 1 + len(state.Rows)
		updates = append(updates, &sheets.Request{
			UpdateCells: &sheets.UpdateCellsRequest{
				Range: &sheets.GridRange{
					SheetId:       state.SheetID,
					StartRowIndex: int64(sentinelIndex),
					EndRowIndex:   int64(sentinelIndex + 1),
				},
				Rows: []*sheets.RowData{{
					Values: []*sheets.CellData{valueToCell(sentinelValue)},
				}},
				Fields: "userEnteredValue",
			},
		})
	}
	return batchRequestWithRetry(ctx, client, spreadsheetID, updates)
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

// batchRequestWithRetry applies all `requests` in a single batch,
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
