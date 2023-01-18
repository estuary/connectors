package main

import (
	"context"

	"github.com/estuary/connectors/sqlcapture"
)

// ShouldBackfill returns true if a given table's contents should be backfilled.
func (db *sqlserverDatabase) ShouldBackfill(streamID string) bool {
	// TODO(wgd): Implement backfills
	return false
}

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from the `resumeAfter` row key if non-nil.
func (db *sqlserverDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, keyColumns []string, resumeAfter []byte) ([]*sqlcapture.ChangeEvent, error) {
	// TODO(wgd): Implement backfills
	panic("NOT YET IMPLEMENTED: ScanTableChunk")
}
