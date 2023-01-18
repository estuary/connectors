package main

import (
	"context"

	"github.com/estuary/connectors/sqlcapture"
)

// ReplicationStream constructs a new ReplicationStream object, from which
// a neverending sequence of change events can be read.
func (db *sqlserverDatabase) ReplicationStream(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
	// TODO(wgd): Implement replication
	panic("NOT YET IMPLEMENTED: ReplicationStream")
}
