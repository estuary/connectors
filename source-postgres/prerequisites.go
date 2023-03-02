package main

import (
	"context"
)

func (db *postgresDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	for _, prereq := range []func(ctx context.Context) error{
		db.prerequisiteLogicalReplication,
		db.prerequisiteReplicationUser,
		db.prerequisiteReplicationSlot,
		db.prerequisitePublication,
		db.prerequisiteWatermarksTable,
	} {
		if err := prereq(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (db *postgresDatabase) prerequisiteLogicalReplication(ctx context.Context) error {
	// TODO: Verify that 'wal_level = LOGICAL'
	return nil
}

func (db *postgresDatabase) prerequisiteReplicationUser(ctx context.Context) error {
	// TODO: Verify that the capture user has REPLICATION permission
	return nil
}

func (db *postgresDatabase) prerequisiteReplicationSlot(ctx context.Context) error {
	// TODO: Verify that the replication slot exists
	return nil
}

func (db *postgresDatabase) prerequisitePublication(ctx context.Context) error {
	// TODO: Verify that the publication exists
	return nil
}

func (db *postgresDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	// TODO: Verify that the watermarks table exists and is writable
	return nil
}

func (db *postgresDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	// TODO(wgd): Verify that the table is present in the publication
	return nil
}
