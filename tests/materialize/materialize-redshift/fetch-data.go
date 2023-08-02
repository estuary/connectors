package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"

	stdsql "database/sql"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {
	address, ok := os.LookupEnv("REDSHIFT_ADDRESS")
	if !ok {
		log.Fatal("missing REDSHIFT_ADDRESS environment variable")
	}

	user, ok := os.LookupEnv("REDSHIFT_USER")
	if !ok {
		log.Fatal("missing REDSHIFT_USER environment variable")
	}

	password, ok := os.LookupEnv("REDSHIFT_PASSWORD")
	if !ok {
		log.Fatal("missing REDSHIFT_PASSWORD environment variable")
	}

	database, ok := os.LookupEnv("REDSHIFT_DATABASE")
	if !ok {
		log.Fatal("missing REDSHIFT_DATABASE environment variable")
	}

	tables := os.Args[1:]
	if len(tables) != 1 {
		log.Fatal("must provide table name as an argument")
	}

	uri := url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(user, password),
		Path:   "/" + database,
	}

	db, err := stdsql.Open("pgx", uri.String())
	if err != nil {
		log.Fatal(fmt.Errorf("connecting to db: %w", err))
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY ID", tables[0]))
	if err != nil {
		log.Fatal(fmt.Errorf("running query: %w", err))
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(fmt.Errorf("reading columns: %w", err))
	}
	row := make([]interface{}, len(cols))

	out := json.NewEncoder(os.Stdout)
	for rows.Next() {
		for idx := range row {
			row[idx] = &row[idx]
		}

		if err := rows.Scan(row...); err != nil {
			log.Fatal(fmt.Errorf("scanning query result: %w", err))
		}

		jsonlRow := make(map[string]interface{})
		for idx, col := range cols {
			jsonlRow[col] = row[idx]
		}

		if err := out.Encode(jsonlRow); err != nil {
			log.Fatal(fmt.Errorf("encoding row result: %w", err))
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(fmt.Errorf("error from returned rows: %w", err))
	}
}
