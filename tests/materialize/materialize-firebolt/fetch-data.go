package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/firebolt-db/firebolt-go-sdk"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var deleteTableFlag = flag.Bool("delete", false, "delete the table instead of dumping its contents")

func deleteTable(db *sql.DB, tableName string) error {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	return err
}

func writeOutput(queriedRows []map[string]any) error {
	return json.NewEncoder(os.Stdout).Encode(queriedRows)
}

func parseRows(rows *sql.Rows) ([]map[string]any, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("reading columns: %w", err)
	}

	data := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range data {
		ptrs[i] = &data[i]
	}

	queriedRows := []map[string]any{}

	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		row := make(map[string]any)
		for idx := range data {
			d := data[idx]
			if t, ok := d.(time.Time); ok {
				// Go JSON encoding apparently doesn't like timestamps with years 9999.
				d = t.UTC().String()
			}
			row[cols[idx]] = d
		}

		queriedRows = append(queriedRows, row)
	}
	return queriedRows, nil
}

func main() {
	flag.Parse()

	clientId, ok := os.LookupEnv("FIREBOLT_CLIENT_ID")
	if !ok {
		log.Fatal("missing FIREBOLT_CLIENT_ID environment variable")
	}

	clientSecret, ok := os.LookupEnv("FIREBOLT_CLIENT_SECRET")
	if !ok {
		log.Fatal("missing FIREBOLT_CLIENT_SECRET environment variable")
	}

	accountName, ok := os.LookupEnv("FIREBOLT_ACCOUNT")
	if !ok {
		log.Fatal("missing FIREBOLT_ACCOUNT environment variable")
	}

	databaseName, ok := os.LookupEnv("FIREBOLT_DATABASE")
	if !ok {
		log.Fatal("missing FIREBOLT_DATABASE environment variable")
	}

	engineName, ok := os.LookupEnv("FIREBOLT_ENGINE")
	if !ok {
		log.Fatal("missing FIREBOLT_ENGINE environment variable")
	}

	tables := flag.Args()
	if len(tables) != 1 {
		log.Fatal("must provide table name as an argument")
	}

	dsn := fmt.Sprintf("firebolt:///%s?account_name=%s&client_id=%s&client_secret=%s&engine=%s", databaseName, accountName, clientId, clientSecret, engineName)

	// opening the firebolt driver
	db, err := sql.Open("firebolt", dsn)
	if err != nil {
		log.Fatal(fmt.Errorf("error during opening a driver: %v", err))
	}
	defer db.Close()

	if *deleteTableFlag {
		if err := deleteTable(db, tables[0]); err != nil {
			log.Fatal(fmt.Errorf("deleting table: %w", err))
		}
	} else {
		rows, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" ORDER BY ID, FLOW_PUBLISHED_AT`, tables[0]))
		if err != nil {
			log.Fatal(fmt.Errorf("running query: %w", err))
		}
		defer rows.Close()

		queriedRows, err := parseRows(rows)
		if err != nil {
			log.Fatal(fmt.Errorf("parsing rows: %w", err))
		}
		rows.Close()

		err = writeOutput(queriedRows)
		if err != nil {
			log.Fatal(fmt.Errorf("writing output: %w", err))
		}
	}
}
