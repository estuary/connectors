package sheets_util

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/api/sheets/v4"
)

// LoadSheetIDMapping retrieves a mapping of sheet titles to their integer API IDs.
func LoadSheetIDMapping(client *sheets.Service, sheetID string) (map[string]int64, error) {
	var metadata, err = client.Spreadsheets.
		Get(sheetID).
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

type States struct {
	SheetID int64
	Columns []StateColumn
}

type StateColumn struct {
	ColInd        int64
	Round         int64
	UserSheetName string
	Docs          []json.RawMessage
}

// LoadInternalStates fetches and parses the `flow_internal` sheet.
func LoadInternalStates(
	client *sheets.Service,
	spreadsheetID string,
) (*States, error) {

	var resp, err = client.Spreadsheets.
		Get(spreadsheetID).
		Ranges("flow_internal").
		Fields("sheets(properties(sheetId),data(startColumn,startRow,rowData(values(userEnteredValue))))").
		Do()

	if err != nil {
		return nil, fmt.Errorf("fetching flow_internal: %w", err)
	} else if ll := len(resp.Sheets); ll != 1 {
		return nil, fmt.Errorf("wrong number of sheets: %d", ll)
	} else if ll = len(resp.Sheets[0].Data); ll != 1 {
		return nil, fmt.Errorf("wrong number of sheet data grids: %d", ll)
	}

	// We expect a single contiguous data grid in the response,
	// where the first row is headers of the applicable sheet & round for each column.
	var rowsIn = resp.Sheets[0].Data[0].RowData
	var headers []*sheets.CellData

	if len(rowsIn) != 0 {
		headers, rowsIn = rowsIn[0].Values, rowsIn[1:]
	}

	var all []StateColumn

	for colInd, header := range headers {
		if header.UserEnteredValue == nil {
			// Skip columns without a header.
			continue
		}
		var sheetName, round, err = parseHeader(header)
		if err != nil {
			return nil, fmt.Errorf("parsing column %d: %w", colInd, err)
		}

		var col = StateColumn{
			ColInd:        int64(colInd),
			Round:         round,
			UserSheetName: sheetName,
			Docs:          nil,
		}

		// Recover document states from this sheet column.
		for rowInd, row := range rowsIn {
			if colInd >= len(row.Values) {
				continue
			} else if uev := row.Values[colInd].UserEnteredValue; uev == nil {
				continue
			} else if uev.StringValue == nil {
				return nil, fmt.Errorf("row %d column %d is not a string", rowInd, colInd)
			} else {
				col.Docs = append(col.Docs, json.RawMessage(*uev.StringValue))
			}
		}

		all = append(all, col)
	}

	return &States{
		SheetID: resp.Sheets[0].Properties.SheetId,
		Columns: all,
	}, nil
}

func parseHeader(cell *sheets.CellData) (sheet string, round int64, err error) {
	if cell.UserEnteredValue.StringValue == nil {
		return "", -1, fmt.Errorf("cell value not a string")
	}
	var v = *cell.UserEnteredValue.StringValue

	var ind = strings.LastIndexByte(v, ':')
	if ind <= 0 {
		return "", -1, fmt.Errorf("malformed cell header %s", v)
	}
	round, err = strconv.ParseInt(v[ind+1:], 10, 64)
	if err != nil {
		return "", -1, fmt.Errorf("parsing round from header %q: %w", v, err)
	}

	return v[:ind], round, nil
}
