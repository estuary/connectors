package main

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"
)

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *sqlserverDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

func (db *sqlserverDatabase) createWatermarksTable(ctx context.Context) error {
	var tableName = db.config.Advanced.WatermarksTable
	log.WithField("table", tableName).Debug("ensuring watermarks table exists")

	const queryPattern = `IF OBJECT_ID('%s', 'U') IS NULL
	                      BEGIN
	                        CREATE TABLE %s(slot INTEGER PRIMARY KEY, watermark TEXT);
							INSERT INTO %s VALUES (0, '');
						  END`

	var query = fmt.Sprintf(queryPattern, tableName, tableName, tableName)
	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()
	return nil
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *sqlserverDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	log.WithField("watermark", watermark).Debug("writing watermark value")
	var query = fmt.Sprintf(`UPDATE %s SET watermark = @p1 WHERE slot = 0;`, db.config.Advanced.WatermarksTable)

	var _, err = db.conn.ExecContext(ctx, query, watermark)
	if err != nil {
		return fmt.Errorf("error writing watermark update: %w", err)
	}
	return nil
}
