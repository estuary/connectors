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
	if ok, err := db.watermarksTableExists(ctx); err != nil {
		return err
	} else if ok {
		log.WithField("table", tableName).Debug("watermarks table already exists")
		return nil
	}

	log.WithField("table", tableName).Debug("creating watermarks table")
	const queryPattern = `BEGIN
	                        CREATE TABLE %s(slot INTEGER PRIMARY KEY, watermark TEXT);
							INSERT INTO %s VALUES (0, '');
						  END`

	var query = fmt.Sprintf(queryPattern, tableName, tableName)
	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()
	return nil
}

func (db *sqlserverDatabase) watermarksTableExists(ctx context.Context) (bool, error) {
	var tableName = db.config.Advanced.WatermarksTable
	log.WithField("table", tableName).Debug("checking if watermarks table exists")

	rows, err := db.conn.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s`, tableName))
	if err != nil {
		return false, fmt.Errorf("error reading from watermarks table: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		return true, nil
	}
	return false, nil
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
