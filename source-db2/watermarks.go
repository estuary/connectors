package main

import (
	"context"
	"fmt"
)

// WriteWatermark upserts a fence into the registered watermarks table.
func (db *db2Database) WriteWatermark(ctx context.Context, watermark string) error {
	var streamID = db.WatermarksTable()
	var query = fmt.Sprintf(`
MERGE INTO %s AS T
  USING (VALUES (CAST(? AS VARCHAR(1000)), CAST(? AS VARCHAR(1000)))) AS S (SLOT, WATERMARK)
  ON T.SLOT = S.SLOT
  WHEN MATCHED THEN UPDATE SET WATERMARK = S.WATERMARK
  WHEN NOT MATCHED THEN INSERT (SLOT, WATERMARK) VALUES (S.SLOT, S.WATERMARK)`, quoteTableName(streamID.Schema, streamID.Table))
	if _, err := db.conn.ExecContext(ctx, query, db.name, watermark); err != nil {
		return fmt.Errorf("error writing watermark to %s: %w", streamID, err)
	}
	return nil
}
