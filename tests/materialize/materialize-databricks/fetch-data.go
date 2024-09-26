package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"

	_ "github.com/databricks/databricks-sql-go"
)

const defaultPort = "443"

func mustDSN() string {
	var accessToken = os.Getenv("DATABRICKS_ACCESS_TOKEN")
	var catalog = os.Getenv("DATABRICKS_CATALOG")
	var address = os.Getenv("DATABRICKS_HOST_NAME")
	if !strings.Contains(address, ":") {
		address = address + ":" + defaultPort
	}
	var schema = os.Getenv("DATABRICKS_SCHEMA")
	var httpPath = os.Getenv("DATABRICKS_HTTP_PATH")

	var params = make(url.Values)
	params.Add("catalog", catalog)
	params.Add("schema", schema)
	params.Add("userAgentEntry", "Estuary Technologies Flow")

	var uri = url.URL{
		Host:     address,
		Path:     httpPath,
		User:     url.UserPassword("token", accessToken),
		RawQuery: params.Encode(),
	}

	return strings.TrimLeft(uri.String(), "/")
}

var runQuery = flag.Bool("run-query", false, "just run a query and exit")

func main() {
	flag.Parse()

	args := flag.Args()
	if len(args) != 1 {
		log.Fatal("must provide a single argument")
	}

	ctx := context.Background()

	db, err := sql.Open("databricks", mustDSN())
	if err != nil {
		log.Fatal(fmt.Errorf("sql.Open: %w", err))
	}
	defer db.Close()

	if *runQuery {
		if _, err := db.ExecContext(ctx, args[0]); err != nil {
			fmt.Println(fmt.Errorf("could not exec query %s: %w", args[0], err))
			os.Exit(1)
		}
		os.Exit(0)
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id, flow_published_at;", args[0])

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(fmt.Errorf("queryContext %q: %w", query, err))
	}

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(fmt.Errorf("getting columns: %w", err))
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(fmt.Errorf("getting column types: %w", err))
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

		for idx, val := range data {
			switch v := val.(type) {
			case float64:
				if math.IsNaN(v) {
					val = "NaN"
				} else if math.IsInf(v, +1) {
					val = "Infinity"
				} else if math.IsInf(v, -1) {
					val = "-Infinity"
				}
			}
			if v, ok := val.(string); ok && colTypes[idx].DatabaseTypeName() == "DECIMAL" {
				if num, err := strconv.ParseUint(v, 10, 64); err == nil {
					val = num
				}
			}
			row[cols[idx]] = val
		}

		queriedRows = append(queriedRows, row)
	}
	rows.Close()

	if err := json.NewEncoder(os.Stdout).Encode(queriedRows); err != nil {
		log.Fatal(fmt.Errorf("writing output: %w", err))
	}
}
