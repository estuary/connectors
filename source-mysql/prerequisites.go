package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
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
	if err != nil {
		return fmt.Errorf("unable to query 'binlog_format' system variable: %w", err)
	} else if len(results.Values) != 1 || len(results.Values[0]) != 1 {
		return fmt.Errorf("unable to query 'binlog_format' system variable: malformed response")
	}
	var format = string(results.Values[0][0].AsString())
	if format != "ROW" {
		return fmt.Errorf("system variable 'binlog_format' must be set to \"ROW\": current binlog_format = %q", format)
	}
	return nil
}

func (db *mysqlDatabase) prerequisiteBinlogExpiry(ctx context.Context) error {
	// This check can be manually disabled by the user. It's dangerous, but
	// might be desired in some edge cases.
	if db.config.Advanced.SkipBinlogRetentionCheck {
		return nil
	}

	// Sanity-check binlog retention and error out if it's insufficiently long.
	expiryTime, err := getBinlogExpiry(db.conn)
	if err != nil {
		return fmt.Errorf("error querying binlog expiry time: %w", err)
	}
	if expiryTime < minimumExpiryTime {
		return fmt.Errorf("binlog retention period is too short (go.estuary.dev/PoMlNf): server reports %s but at least %s is required (and 30 days is preferred wherever possible)", expiryTime.String(), minimumExpiryTime.String())
	}
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

	// Try to create the watermarks database if it doesn't already exist. WatermarksTable from the
	// configuration has already been validated as being in the fully-qualified form of
	// <database>.<table>.
	if _, err := db.conn.Execute(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", strings.Split(table, ".")[0])); err != nil {
		// It is entirely possible that the watermarks database already exists but we don't have
		// permission to create databases. In this case we will get an "Access Denied" error here.
		logEntry.WithField("err", err).Debug("failed to create watermarks database")
	} else if _, err := db.conn.Execute(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot INTEGER PRIMARY KEY, watermark TEXT);", table)); err != nil {
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

func getBinlogExpiry(conn *client.Conn) (time.Duration, error) {
	// When running on Amazon RDS MySQL there's an RDS-specific configuration
	// for binlog retention, so that takes precedence if it exists.
	rdsRetentionHours, err := queryNumericVariable(conn, `SELECT name, value FROM mysql.rds_configuration WHERE name = 'binlog retention hours';`)
	if err == nil {
		return time.Duration(rdsRetentionHours) * time.Hour, nil
	}

	// The newer 'binlog_expire_logs_seconds' variable takes priority if it exists and is nonzero.
	expireLogsSeconds, err := queryNumericVariable(conn, `SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';`)
	if err == nil && expireLogsSeconds > 0 {
		return time.Duration(expireLogsSeconds * float64(time.Second)), nil
	}

	// And as the final resort we'll check 'expire_logs_days' if 'seconds' was zero or nonexistent.
	expireLogsDays, err := queryNumericVariable(conn, `SHOW VARIABLES LIKE 'expire_logs_days';`)
	if err != nil {
		return 0, err
	}
	if expireLogsDays > 0 {
		return time.Duration(expireLogsDays) * 24 * time.Hour, nil
	}

	// If both 'binlog_expire_logs_seconds' and 'expire_logs_days' are set to zero
	// MySQL will not automatically purge binlog segments. For simplicity we just
	// represent that as a 'one year' expiry time, since all we need the value for
	// is to make sure it's not too short.
	return 365 * 24 * time.Hour, nil
}

func queryNumericVariable(conn *client.Conn, query string) (float64, error) {
	var results, err = conn.Execute(query)
	if err != nil {
		return 0, fmt.Errorf("error executing query %q: %w", query, err)
	}
	if len(results.Values) == 0 {
		return 0, fmt.Errorf("no results from query %q", query)
	}

	// Return the second column of the first row. It has to be the second
	// column because that's how the `SHOW VARIABLES LIKE` query does it.
	var value = &results.Values[0][1]
	switch value.Type {
	case mysql.FieldValueTypeNull:
		return 0, nil
	case mysql.FieldValueTypeString:
		var n, err = strconv.ParseFloat(string(value.AsString()), 64)
		if err != nil {
			return 0, fmt.Errorf("couldn't parse string value as number: %w", err)
		}
		return n, nil
	case mysql.FieldValueTypeUnsigned:
		return float64(value.AsUint64()), nil
	case mysql.FieldValueTypeSigned:
		return float64(value.AsInt64()), nil
	}
	return value.AsFloat64(), nil
}
