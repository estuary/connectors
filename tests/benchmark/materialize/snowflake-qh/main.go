// snowflake-qh prints, as TSV, every MERGE the configured user ran in a time
// window, with its elapsed, execution, queued and lock-blocked times and bytes
// scanned, read from INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER.
//
//	go run ./tests/benchmark/materialize/snowflake-qh <sops config> \
//	    "2026-10-15 01:30:00 +0000" "2026-10-15 02:15:00 +0000"
//
// The config is the connector's sops-encrypted endpoint config. The window
// bounds are local-timezone timestamps with a UTC offset.
package main

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/pem"
	"fmt"
	"os"
	"os/exec"
	"strings"

	sf "github.com/snowflakedb/gosnowflake/v2"
	"gopkg.in/yaml.v3"
)

type cfg struct {
	Host, Account, Database, Schema, Warehouse string
	Credentials                                struct {
		User       string `yaml:"user"`
		PrivateKey string `yaml:"private_key"`
	}
}

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: snowflake-qh <sops-config.yaml> <window-start> <window-end>")
		os.Exit(2)
	}
	raw, err := exec.Command("sops", "-d", os.Args[1]).Output()
	if err != nil {
		panic(err)
	}
	var c cfg
	if err := yaml.Unmarshal(raw, &c); err != nil {
		panic(err)
	}
	block, _ := pem.Decode([]byte(c.Credentials.PrivateKey))
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		panic(err)
	}
	dsn, err := sf.DSN(&sf.Config{
		Account: c.Account, Host: c.Host, User: c.Credentials.User,
		Database: c.Database, Schema: c.Schema, Warehouse: c.Warehouse,
		Authenticator: sf.AuthTypeJwt, PrivateKey: key.(*rsa.PrivateKey),
	})
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		panic(err)
	}
	// Window as "YYYY-MM-DD HH:MM:SS +0000" strings: start, end.
	start, end := os.Args[2], os.Args[3]
	rows, err := db.QueryContext(context.Background(), `
SELECT QUERY_TAG, QUERY_ID, START_TIME, TOTAL_ELAPSED_TIME, COMPILATION_TIME, EXECUTION_TIME,
       QUEUED_OVERLOAD_TIME, TRANSACTION_BLOCKED_TIME,
       BYTES_SCANNED, SUBSTR(QUERY_TEXT, 1, 40)
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER(
       USER_NAME => '`+c.Credentials.User+`',
       END_TIME_RANGE_START => TO_TIMESTAMP_LTZ('`+start+`'),
       END_TIME_RANGE_END => TO_TIMESTAMP_LTZ('`+end+`'), RESULT_LIMIT => 10000))
WHERE QUERY_TEXT ILIKE '%MERGE INTO%'
ORDER BY QUERY_TAG, START_TIME`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	fmt.Println(strings.Join(cols, "\t"))
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			panic(err)
		}
		var out []string
		for _, v := range vals {
			out = append(out, fmt.Sprint(v))
		}
		fmt.Println(strings.Join(out, "\t"))
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}
