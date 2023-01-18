package main

import "context"

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *sqlserverDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *sqlserverDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	// TODO(wgd): Implement watermark writes
	panic("NOT YET IMPLEMENTED: WriteWatermark")
}
