package main

import "context"

func (db *mysqlDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	for _, prereq := range []func(ctx context.Context) error{
		db.prerequisiteBinlogFormat,
		db.prerequisiteBinlogExpiry,
		db.prerequisiteWatermarksTable,
		db.prerequisiteUserPermissions,
	} {
		if err := prereq(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (db *mysqlDatabase) prerequisiteBinlogFormat(ctx context.Context) error {
	// TODO: Verify that 'binlog_format' is set to 'ROW'
	return nil
}

func (db *mysqlDatabase) prerequisiteBinlogExpiry(ctx context.Context) error {
	// TODO: Move the binlog expiry checking code here
	return nil
}

func (db *mysqlDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	// TODO: Verify that the watermarks table exists
	return nil
}

func (db *mysqlDatabase) prerequisiteUserPermissions(ctx context.Context) error {
	// TODO: Verify that the user has REPLICATION CLIENT and REPLICATION SLAVE permissions
	return nil
}

func (db *mysqlDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	return nil
}
