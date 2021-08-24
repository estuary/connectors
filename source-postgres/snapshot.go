package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

func streamSnapshotEvents(ctx context.Context, conn *pgx.Conn, namespace, table string, handler ChangeEventHandler) error {
	log.Printf("streamSnapshotEvents()")
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT * FROM %s.%s;`, namespace, table))
	if err != nil {
		return errors.Wrap(err, "unable to execute query")
	}
	defer rows.Close()
	cols := rows.FieldDescriptions()
	for rows.Next() {
		fields := make(map[string]interface{})
		vals, err := rows.Values()
		if err != nil {
			return errors.Wrap(err, "unable to get row values")
		}
		for idx, val := range vals {
			colName := string(cols[idx].Name)
			fields[colName] = val
		}
		if err := handler(namespace, table, "Insert", fields); err != nil {
			return errors.Wrap(err, "error handling change event")
		}
	}
	return nil
}
