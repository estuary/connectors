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
	const queryPattern = `BEGIN
	                        CREATE TABLE %s(slot INTEGER PRIMARY KEY, watermark TEXT);
							INSERT INTO %s VALUES (0, '');
						  END`
	rows, err := db.conn.QueryContext(ctx, fmt.Sprintf(queryPattern, tableName, tableName))
	rows.Close()
	if err != nil {
		return fmt.Errorf("error creating watermarks table: %w", err)
	}
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
