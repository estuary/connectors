package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strings"

	sf "github.com/snowflakedb/gosnowflake"
)

func mustDSN() string {
	var maxStatementCount string = "0"

	conf := sf.Config{
		Params: map[string]*string{
			// By default Snowflake expects the number of statements to be provided
			// with every request. By setting this parameter to zero we are allowing a
			// variable number of statements to be executed in a single request
			"MULTI_STATEMENT_COUNT": &maxStatementCount,
		},
	}

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"SNOWFLAKE_HOST", &conf.Host},
		{"SNOWFLAKE_ACCOUNT", &conf.Account},
		{"SNOWFLAKE_USER", &conf.User},
		{"SNOWFLAKE_PASSWORD", &conf.Password},
		{"SNOWFLAKE_DATABASE", &conf.Database},
		{"SNOWFLAKE_SCHEMA", &conf.Schema},
		{"SNOWFLAKE_WAREHOUSE", &conf.Warehouse},
	} {
		if v, exists := os.LookupEnv(prop.key); exists {
			*prop.dest = v
		}
	}

	if v, exists := os.LookupEnv("SNOWFLAKE_PRIVATE_KEY"); exists {
		conf.Authenticator = sf.AuthTypeJwt
		var pkString = strings.ReplaceAll(v, "\\n", "\n")
		var block, _ = pem.Decode([]byte(pkString))
		if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			panic(fmt.Errorf("parsing private key: %w", err))
		} else {
			conf.PrivateKey = key.(*rsa.PrivateKey)
		}
	}

	dsn, err := sf.DSN(&conf)
	if err != nil {
		panic(err)
	}

	return dsn
}

var deleteTable = flag.Bool("delete", false, "delete the table instead of dumping its contents")
var deleteSpecs = flag.Bool("delete-specs", false, "stored materialize checkpoint and specs")

func main() {
	flag.Parse()

	tables := flag.Args()
	if len(tables) != 1 {
		log.Fatal("must provide single table name as an argument")
	}

	ctx := context.Background()

	db, err := sql.Open("snowflake", mustDSN())
	if err != nil {
		log.Fatal(fmt.Errorf("opening snowflake: %w", err))
	} else if err := db.PingContext(ctx); err != nil {
		log.Fatal(fmt.Errorf("connecting to snowflake: %w", err))
	}

	// Handle cleanup cases of for dropping a table and deleting the stored materialization spec &
	// checkpoint if flags were provided.
	if *deleteTable {
		query := fmt.Sprintf("DROP TABLE %s;", tables[0])
		if _, err := db.ExecContext(ctx, query); err != nil {
			fmt.Println(fmt.Errorf("could not drop table %s: %w", tables[0], err))
		}
		fmt.Printf("dropped table %s\n", tables[0])
		os.Exit(0)
	} else if *deleteSpecs {
		query := "delete from FLOW_MATERIALIZATIONS_V2 where MATERIALIZATION='tests/materialize-snowflake/materialize';"
		if _, err := db.ExecContext(ctx, query); err != nil {
			fmt.Println(fmt.Errorf("could not delete stored materialization spec/checkpoint: %w", err))
		}
		fmt.Println("deleted stored materialization spec & checkpoint")
		os.Exit(0)
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id, flow_published_at;", tables[0])

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(fmt.Errorf("queryContext %q: %w", query, err))
	}

	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(fmt.Errorf("getting columns: %w", err))
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
			row[cols[idx]] = val
		}

		queriedRows = append(queriedRows, row)
	}
	rows.Close()

	if err := json.NewEncoder(os.Stdout).Encode(queriedRows); err != nil {
		log.Fatal(fmt.Errorf("writing output: %w", err))
	}
}
