package main

import (
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	stdsql "database/sql"
)

type colDetails struct {
	maxLength int
}

// getVarcharDetails queries the database for VARCHAR type columns to determine their actual maximum
// lengths and character set. The result map has keys that are table identifiers, and each one of those has a value
// that is a map of column identifiers to the details of the column. Since Redshift always
// converts all letters to lowercase for table or column identifiers, the returned identifiers are
// also all lowercase.
func getVarcharDetails(ctx context.Context, dbName string, conn *stdsql.Conn) (map[string]map[string]colDetails, error) {
	out := make(map[string]map[string]colDetails)

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

		tableIdentifer := mysqlDialect.Identifier(tableName)
		columnIdentifier := mysqlDialect.Identifier(columnName)

		log.WithFields(log.Fields{
			"table":  tableIdentifer,
			"column": columnIdentifier,
			"length": maxLength,
		}).Debug("queried VARCHAR column length")

		if out[tableIdentifer] == nil {
			out[tableIdentifer] = make(map[string]colDetails)
		}
		out[tableIdentifer][columnIdentifier] = colDetails{
			maxLength: maxLength,
		}
		n++
	}

	log.WithFields(log.Fields{
		"tables":  len(out),
		"columns": n,
		"result": out,
	}).Info("queried VARCHAR column lengths")

	return out, nil
}
