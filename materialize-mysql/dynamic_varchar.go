package main

import (
	"context"
	stdsql "database/sql"
	sql "github.com/estuary/connectors/materialize-sql"
	"fmt"
	log "github.com/sirupsen/logrus"
)

// getVarcharDetails queries the database for VARCHAR type columns to determine their actual maximum
// lengths and character set. The result map has keys that are table identifiers, and each one of those has a value
// that is a map of column identifiers to the details of the column.
func getVarcharDetails(ctx context.Context, dialect sql.Dialect, dbName string, conn *stdsql.Conn) (map[string]map[string]int, error) {
	out := make(map[string]map[string]int)

	q := `
	SELECT
		c.table_schema,
		c.table_name,
		c.column_name,
		c.character_maximum_length
	FROM information_schema.columns c
	WHERE
		c.table_schema = ? AND
		c.data_type = 'varchar';
	`

	n := 0
	rows, err := conn.QueryContext(ctx, q, dbName)

	if err != nil {
		return nil, fmt.Errorf("querying string column lengths: %w", err)
	}
	for rows.Next() {
		var schema, tableName, columnName string
		var maxLength int
		if err := rows.Scan(&schema, &tableName, &columnName, &maxLength); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		tableIdentifer := dialect.Identifier(tableName)
		columnIdentifier := dialect.Identifier(columnName)

		log.WithFields(log.Fields{
			"table":  tableIdentifer,
			"column": columnIdentifier,
			"length": maxLength,
		}).Debug("queried VARCHAR column length")

		if out[tableIdentifer] == nil {
			out[tableIdentifer] = make(map[string]int)
		}
		out[tableIdentifer][columnIdentifier] = maxLength
		n++
	}

	log.WithFields(log.Fields{
		"tables":  len(out),
		"columns": n,
		"result":  out,
	}).Info("queried VARCHAR column lengths")

	return out, nil
}
