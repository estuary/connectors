package main

import "context"

func (db *sqlserverDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	for _, prereq := range []func(ctx context.Context) error{
		db.prerequisiteCDCEnabled,
		db.prerequisiteWatermarksTable,
	} {
		if err := prereq(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (db *sqlserverDatabase) prerequisiteCDCEnabled(ctx context.Context) error {
	// TODO: Verify that CDC is enabled on the database and the agent is running
	return nil
}

func (db *sqlserverDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	// TODO: Verify that the watermarks table exists
	return nil
}

func (db *sqlserverDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	// TODO: Verify that there's a change table corresponding to this table
	return nil
}
