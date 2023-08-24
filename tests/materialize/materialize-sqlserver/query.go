package main

import (
	"os"
	"context"
	"net/url"
	"encoding/json"
	"io"

	stdsql "database/sql"
	_ "github.com/microsoft/go-mssqldb"
)

func main() {
	var host = os.Getenv("SQLSERVER_HOST")
	var port = os.Getenv("SQLSERVER_PORT")
	var dbName = os.Getenv("SQLSERVER_DATABASE")
	var user = os.Getenv("SQLSERVER_USER")
	var password = os.Getenv("SQLSERVER_PASSWORD")

	var address = host + ":" + port

	var params = make(url.Values)
	params.Add("app name", "Flow Test")
	params.Add("encrypt", "true")
	params.Add("TrustServerCertificate", "true")
	params.Add("database", dbName)

	var uri = url.URL{
		Scheme: "sqlserver",
		Host:   address,
		User:   url.UserPassword(user, password),
		RawQuery: params.Encode(),
	}

	var db, err = stdsql.Open("sqlserver", uri.String())
	if err != nil {
		panic(err)
	}

	var ctx = context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		panic(err)
	}


	userQuery, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}

	rows, err := conn.QueryContext(ctx, string(userQuery))
	if err != nil {
		panic(err)
	}

	cols, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	var results = make([]map[string]any, 0)

	for rows.Next() {
		var items = make([]any, len(cols))
		var itemsPtrs = make([]any, len(cols))
		for i := range items {
			itemsPtrs[i] = &items[i]
		}

		if err := rows.Scan(itemsPtrs...); err != nil {
			panic(err)
		}

		var m = make(map[string]any)
		for i, col := range cols {
			m[col] = items[i]
		}

		results = append(results, m)
	}

	j, err := json.Marshal(results)
	if err != nil {
		panic(err)
	}

	os.Stdout.Write(j)
	os.Stdout.Write([]byte("\n"))
}
