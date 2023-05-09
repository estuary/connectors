package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"

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
	} {
		*prop.dest = os.Getenv(prop.key)
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
		query := "delete from FLOW_CHECKPOINTS_V1 where MATERIALIZATION='tests/materialize-snowflake/materialize';delete from FLOW_MATERIALIZATIONS_V2 where MATERIALIZATION='tests/materialize-snowflake/materialize';"
		if _, err := db.ExecContext(ctx, query); err != nil {
			fmt.Println(fmt.Errorf("could not delete stored materialization spec/checkpoint: %w", err))
		}
		fmt.Println("deleted stored materialization spec & checkpoint")
		os.Exit(0)
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id;", tables[0])

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(fmt.Errorf("queryContext: %w", err))
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

	queriedRows := [][]interface{}{}

	for rows.Next() {
		if err = rows.Scan(ptrs...); err != nil {
			log.Fatal("scanning row: %w", err)
		}
		queriedRows = append(queriedRows, append([]interface{}{}, data...))
	}
	rows.Close()

	var enc = json.NewEncoder(os.Stdout)
	for _, row := range queriedRows {
		enc.Encode(row)
	}
}
