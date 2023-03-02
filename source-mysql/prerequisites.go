package main

import (
	"context"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

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
	var results, err = db.conn.Execute(`SELECT @@GLOBAL.binlog_format;`)
	if err != nil || len(results.Values) == 0 {
		return fmt.Errorf("unable to query 'binlog_format' system variable: %w", err)
	}
	var format = string(results.Values[0][0].AsString())
	if format != "ROW" {
		return fmt.Errorf("system variable 'binlog_format' must be set to \"ROW\": current binlog_format = %q", format)
	}
	return nil
}

func (db *mysqlDatabase) prerequisiteBinlogExpiry(ctx context.Context) error {
	// TODO: Move the binlog expiry checking code here
	return nil
}

func (db *mysqlDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	var table = db.config.Advanced.WatermarksTable
	var logEntry = logrus.WithField("table", table)

	// If we can successfully write a watermark then we're satisfied here
	if err := db.WriteWatermark(ctx, "existence-check"); err == nil {
		logEntry.Debug("watermarks table already exists")
		return nil
	}

	// If we can create the watermarks table and then write a watermark, that also works
	logEntry.Info("watermarks table doesn't exist, attempting to create it")
	// TODO(wgd): Try executing a `CREATE DATABASE IF NOT EXISTS %s;` when necessary?
	if _, err := db.conn.Execute(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot INTEGER PRIMARY KEY, watermark TEXT);", table)); err != nil {
		logEntry.WithField("err", err).Error("failed to create watermarks table")
	} else if err := db.WriteWatermark(ctx, "existence-check"); err == nil {
		logEntry.Info("successfully created watermarks table")
		return nil
	}

	return fmt.Errorf("user %q cannot write to the watermarks table %q", db.config.User, table)
}

func (db *mysqlDatabase) prerequisiteUserPermissions(ctx context.Context) error {
	// The SHOW MASTER STATUS command requires REPLICATION CLIENT or SUPER privileges,
	// and thus serves as an easy way to test whether the user is authorized for CDC.
	var results, err = db.conn.Execute("SHOW MASTER STATUS;")
	if err != nil {
		return fmt.Errorf("user %q needs the REPLICATION CLIENT permission", db.config.User)
	}
	results.Close()

	// The SHOW SLAVE HOSTS command (called SHOW REPLICAS in newer versions, but we're
	// using the deprecated form here for compatibility with old MySQL releases) requires
	// the REPLICATION SLAVE permission, which we need for CDC.
	results, err = db.conn.Execute("SHOW SLAVE HOSTS;")
	if err != nil {
		return fmt.Errorf("user %q needs the REPLICATION SLAVE permission", db.config.User)
	}
	results.Close()
	return nil
}

func (db *mysqlDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	results, err := db.conn.Execute(fmt.Sprintf(`SELECT * FROM %s.%s LIMIT 0;`, schema, table))
	if err != nil {
		var streamID = sqlcapture.JoinStreamID(schema, table)
		return fmt.Errorf("user %q cannot read from table %q", db.config.User, streamID)
	}
	results.Close()
	return nil
}
