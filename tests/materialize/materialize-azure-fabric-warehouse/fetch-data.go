package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/microsoft/go-mssqldb/azuread"
)

var deleteTable = flag.Bool("delete", false, "delete the table instead of dumping its contents")
var deleteCheckpoint = flag.Bool("delete-checkpoint", false, "remove the stored materialization checkpoint")

func main() {
	flag.Parse()

	tables := flag.Args()
	if len(tables) != 1 {
		log.Fatal("must provide table name as an argument")
	}

	connectionString, ok := os.LookupEnv("FABRIC_WAREHOUSE_CONNECTION_STRING")
	if !ok {
		log.Fatal("missing FABRIC_WAREHOUSE_CONNECTION_STRING environment variable")
	}

	clientID, ok := os.LookupEnv("FABRIC_WAREHOUSE_CLIENT_ID")
	if !ok {
		log.Fatal("missing FABRIC_WAREHOUSE_CLIENT_ID environment variable")
	}

	clientSecret, ok := os.LookupEnv("FABRIC_WAREHOUSE_CLIENT_SECRET")
	if !ok {
		log.Fatal("missing FABRIC_WAREHOUSE_CLIENT_SECRET environment variable")
	}

	warehouse, ok := os.LookupEnv("FABRIC_WAREHOUSE_WAREHOUSE")
	if !ok {
		log.Fatal("missing FABRIC_WAREHOUSE_WAREHOUSE environment variable")
	}

	schema, ok := os.LookupEnv("FABRIC_WAREHOUSE_SCHEMA")
	if !ok {
		log.Fatal("missing FABRIC_WAREHOUSE_SCHEMA environment variable")
	}

	db, err := sql.Open(
		azuread.DriverName,
		fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%d;database=%s;fedauth=ActiveDirectoryServicePrincipal",
			connectionString, clientID, clientSecret, 1433, warehouse,
		))
	if err != nil {
		log.Fatal(fmt.Errorf("connecting to db: %w", err))
	}
	defer db.Close()

	if *deleteTable {
		if _, err := db.Exec(fmt.Sprintf(`DROP TABLE %q.%q`, schema, tables[0])); err != nil {
			fmt.Println(fmt.Errorf("could not drop table %s: %w", tables[0], err))
		}
		os.Exit(0)
	} else if *deleteCheckpoint {
		if _, err := db.Exec(fmt.Sprintf("delete from %s.flow_checkpoints_v1 where materialization='tests/materialize-azure-fabric-warehouse/materialize'", schema)); err != nil {
			fmt.Println(fmt.Errorf("could not delete checkpoint: %w", err))
		}
		os.Exit(0)
	}

	rows, err := db.Query(fmt.Sprintf(`SELECT * FROM %q.%q ORDER BY id, flow_published_at`, schema, tables[0]))
	if err != nil {
		log.Fatal(fmt.Errorf("running query: %w", err))
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(fmt.Errorf("reading columns: %w", err))
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(fmt.Errorf("reading column types: %w", err))
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
			} else if bytes, ok := d.([]byte); ok {
				// Check the database type name
				typeName := colTypes[idx].DatabaseTypeName()
				if typeName == "DECIMAL" || typeName == "NUMERIC" {
					d = string(bytes)
				}
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
