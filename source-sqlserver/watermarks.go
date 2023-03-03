package main

import (
	"context"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *sqlserverDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

func (db *sqlserverDatabase) createWatermarksTable(ctx context.Context) error {
	var tableName = db.config.Advanced.WatermarksTable
	rows, err := db.conn.QueryContext(ctx, fmt.Sprintf(`CREATE TABLE %s(slot INTEGER PRIMARY KEY, watermark TEXT);`, tableName))
	if err != nil {
		return fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()
	return nil
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *sqlserverDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	var table = db.config.Advanced.WatermarksTable
	var query = strings.ReplaceAll(`
    BEGIN TRAN
        UPDATE <watermarks> SET watermark = @p1 WHERE slot = 0;
        IF (@@ROWCOUNT = 0) INSERT INTO <watermarks> VALUES (0, @p1);
    COMMIT
    `, "<watermarks>", table)
	log.WithFields(log.Fields{"table": table, "watermark": watermark}).Debug("writing watermark value")
	if _, err := db.conn.ExecContext(ctx, query, watermark); err != nil {
		return fmt.Errorf("error writing watermark update: %w", err)
	}
	return nil
}
