package main

import (
	"context"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
)

// ctEnabledTables returns a cached set of stream IDs for tables that have Change Tracking enabled.
// The cache is populated on first call and reused for subsequent calls within the same capture.
func (db *sqlserverDatabase) ctEnabledTables(ctx context.Context) (map[sqlcapture.StreamID]bool, error) {
	if db.ctEnabledTablesCache != nil {
		return db.ctEnabledTablesCache, nil
	}

	var query = `
		SELECT s.name AS schema_name, t.name AS table_name
		FROM sys.change_tracking_tables ctt
		JOIN sys.tables t ON ctt.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id;
	`
	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error listing CT-enabled tables: %w", err)
	}
	defer rows.Close()

	var result = make(map[sqlcapture.StreamID]bool)
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		result[sqlcapture.JoinStreamID(schemaName, tableName)] = true
	}

	db.ctEnabledTablesCache = result
	return result, nil
}

// isTableCTEnabled checks whether Change Tracking is enabled for a specific table.
// Uses the cached result from ctEnabledTables.
func (db *sqlserverDatabase) isTableCTEnabled(ctx context.Context, schema, table string) (bool, error) {
	tables, err := db.ctEnabledTables(ctx)
	if err != nil {
		return false, err
	}
	return tables[sqlcapture.JoinStreamID(schema, table)], nil
}
