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

	var client, err = sheets.NewService(context.Background(),
		option.WithCredentialsJSON([]byte(saKey)))
	if err != nil {
		log.Fatal(fmt.Errorf("building sheets service: %w", err))
	}

	resp, err := client.Spreadsheets.
		Get(spreadsheetID).
		Ranges(sheetName).
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
