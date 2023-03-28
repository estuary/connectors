package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

func main() {
	saKey, ok := os.LookupEnv("GCP_SERVICE_ACCOUNT_KEY")
	if !ok {
		log.Fatal("missing GCP_SERVICE_ACCOUNT_KEY environment variable")
	}
	spreadsheetID, ok := os.LookupEnv("SPREADSHEET_ID")
	if !ok {
		log.Fatal("missing SPREADSHEET_ID environment variable")
	}
	sheetName, ok := os.LookupEnv("SHEET_NAME")
	if !ok {
		log.Fatal("missing SHEET_NAME environment variable")
	}
	sheetCmd, ok := os.LookupEnv("SHEET_COMMAND")
	if !ok {
		log.Fatal("missing SHEET_COMMAND environment variable")
	}

	var client, err = sheets.NewService(context.Background(),
		option.WithCredentialsJSON([]byte(saKey)))
	if err != nil {
		log.Fatal(fmt.Errorf("building sheets service: %w", err))
	}

	if sheetCmd == "fetch" {
		// Fetch all rows, and all columns except the internal state column (A).
		resp, err := client.Spreadsheets.
			Get(spreadsheetID).
			Ranges(fmt.Sprintf("'%s'!B:Z", sheetName)).
			Fields("sheets(data(startColumn,startRow,rowData(values(userEnteredValue))))").
			Do()

		if err != nil {
			log.Fatal(fmt.Errorf("fetching sheet: %w", err))
		}

		var enc = json.NewEncoder(os.Stdout)
		for _, row := range resp.Sheets[0].Data[0].RowData {
			enc.Encode(row)
		}
	}

	if sheetCmd == "cleanup" {
		resp, err := client.Spreadsheets.
			Get(spreadsheetID).
			Fields("sheets(properties)").
			Do()

		if err != nil {
			log.Fatal(fmt.Errorf("fetching sheets: %w", err))
		}

		// Remove all but the first sheet (which is not used).
		var requests []*sheets.Request
		for _, sheet := range resp.Sheets[1:] {
			requests = append(requests, &sheets.Request{
				DeleteSheet: &sheets.DeleteSheetRequest{
					SheetId: sheet.Properties.SheetId,
				},
			})
		}

		_, err = client.Spreadsheets.BatchUpdate(spreadsheetID, &sheets.BatchUpdateSpreadsheetRequest{
			Requests: requests,
		}).Do()

		if err != nil {
			log.Fatal(fmt.Errorf("removing sheets: %w", err))
		}
	}
}
