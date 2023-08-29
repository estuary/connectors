package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

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

	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM "%s" ORDER BY ID`, tables[0]))
	if err != nil {
		log.Fatal(fmt.Errorf("running query: %w", err))
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(fmt.Errorf("reading columns: %w", err))
	}

	data := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range data {
		ptrs[i] = &data[i]
	}

	queriedRows := []map[string]any{}

	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			log.Fatal("scanning row: %w", err)
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
	rows.Close()

	if err := json.NewEncoder(os.Stdout).Encode(queriedRows); err != nil {
		log.Fatal(fmt.Errorf("writing output: %w", err))
	}
}
