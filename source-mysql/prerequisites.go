package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

func (db *mysqlDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	// Our version checking may have been overly conservative, so let's err in the
	// other direction for a while and disengage the check entirely.
	if err := db.prerequisiteVersion(ctx); err != nil {
		logrus.WithField("err", err).Warn("database version may be insufficient")
	}

	for _, prereq := range []func(ctx context.Context) error{
		db.prerequisiteBinlogEnabled,
		db.prerequisiteBinlogFormat,
		db.prerequisiteBinlogExpiry,
		db.prerequisiteUserPermissions,
	} {
		if err := prereq(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

const (
	// TODO(whb): MySQL 5.7 current does not appear to work with our connector, see
	// https://github.com/estuary/connectors/issues/682.
	mysqlReqMajorVersion = 8
	mysqlReqMinorVersion = 0

	mariadbReqMajorVersion = 10
	mariadbReqMinorVersion = 3
)

func (db *mysqlDatabase) prerequisiteVersion(_ context.Context) error {
	var minMajor, minMinor = mysqlReqMajorVersion, mysqlReqMinorVersion
	if db.versionProduct == "MariaDB" {
		minMajor, minMinor = mariadbReqMajorVersion, mariadbReqMinorVersion
	}

	if !sqlcapture.ValidVersion(db.versionMajor, db.versionMinor, minMajor, minMinor) {
		return fmt.Errorf(
			"minimum supported %s version is %d.%d: attempted to capture from database version %d.%d",
			db.versionProduct,
			minMajor, minMinor,
			db.versionMajor, db.versionMinor,
		)
	}
	return nil
}

func (db *mysqlDatabase) prerequisiteBinlogEnabled(ctx context.Context) error {
	var results, err = db.conn.Execute(`SHOW VARIABLES LIKE 'log_bin';`)
	if err != nil {
		return fmt.Errorf("unable to query 'log_bin' system variable: %w", err)
	} else if len(results.Values) != 1 || len(results.Values[0]) != 2 {
		return fmt.Errorf("unable to query 'log_bin' system variable: malformed response")
	}
	var value = string(results.Values[0][1].AsString())
	logrus.WithField("log_bin", value).Info("queried system variable")
	if value != "ON" {
		return fmt.Errorf("binary logging is not enabled: system variable 'log_bin' = %q", value)
	}
	return nil
}

func (db *mysqlDatabase) prerequisiteBinlogFormat(ctx context.Context) error {
	var results, err = db.conn.Execute(`SELECT @@GLOBAL.binlog_format;`)
	if err != nil {
		return fmt.Errorf("unable to query 'binlog_format' system variable: %w", err)
	} else if len(results.Values) != 1 || len(results.Values[0]) != 1 {
		return fmt.Errorf("unable to query 'binlog_format' system variable: malformed response")
	}
	var format = string(results.Values[0][0].AsString())
	logrus.WithField("binlog_format", format).Info("queried system variable")
	if format != "ROW" {
		return fmt.Errorf("system variable 'binlog_format' must be set to \"ROW\": current binlog_format = %q", format)
	}
	return nil
}

func (db *mysqlDatabase) prerequisiteBinlogExpiry(ctx context.Context) error {
	// This check can be manually disabled by the user. It's dangerous, but
	// might be desired in some edge cases.
	if db.config.Advanced.SkipBinlogRetentionCheck {
		logrus.Info("skipping binlog retention sanity check")
		return nil
	}

	// Sanity-check binlog retention and error out if it's insufficiently long.
	expiryTime, err := getBinlogExpiry(db.conn)
	logrus.WithField("expiry", expiryTime.String()).Info("queried binlog expiry time")
	if err != nil {
		return fmt.Errorf("error querying binlog expiry time: %w", err)
	}
	if expiryTime < minimumExpiryTime {
		return fmt.Errorf("binlog retention period is too short (go.estuary.dev/PoMlNf): server reports %s but at least %s is required (and 30 days is preferred wherever possible)", expiryTime.String(), minimumExpiryTime.String())
	}
	return nil
}

func (db *mysqlDatabase) prerequisiteUserPermissions(ctx context.Context) error {
	// The SHOW MASTER STATUS / SHOW BINARY LOG STATUS command requires REPLICATION CLIENT
	// or SUPER privileges, and thus serves as an easy way to test whether the user is
	// authorized for CDC.
	if _, err := db.queryBinlogStatus(); err != nil {
		logrus.WithField("err", err).Info("failed to query binlog status")
		return fmt.Errorf("user %q needs the REPLICATION CLIENT permission", db.config.User)
	}
	return nil
}

func (db *mysqlDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	var streamID = sqlcapture.JoinStreamID(schema, table)

	results, err := db.conn.Execute(fmt.Sprintf("SELECT * FROM `%s`.`%s` LIMIT 0;", schema, table))
	if err != nil {
		return fmt.Errorf("user %q cannot read from table %q", db.config.User, streamID)
	}
	results.Close()

	// If this table contains any DATETIME columns, fail validation if the database timezone
	// couldn't be determined.
	// Requirement: (*db).connect has already been called to initialize the datetimeLocation field.
	if db.datetimeLocation == nil {
		results, err := db.conn.Execute(fmt.Sprintf(`
			SELECT column_name
			FROM information_schema.columns
			WHERE data_type='datetime' AND table_schema='%s' AND table_name='%s';
		`, schema, table))
		if err != nil {
			return fmt.Errorf("db.datetimeLocation not set and could not validate that table %q does not contain DATETIME columns: %w", streamID, err)
		}

		var datetimeCols []string
		for _, row := range results.Values {
			datetimeCols = append(datetimeCols, string(row[0].AsString()))
		}
		results.Close()

		if len(datetimeCols) > 0 {
			return fmt.Errorf("system variable 'time_zone' must be set or capture configured with a valid timezone to capture datetime columns [%s] from table %q", strings.Join(datetimeCols, ", "), streamID)
		}
	}

	return nil
}

func getBinlogExpiry(conn mysqlClient) (time.Duration, error) {
	// When running on Amazon RDS MySQL there's an RDS-specific configuration
	// for binlog retention, so that takes precedence if it exists.
	rdsRetentionHours, err := queryNumericVariable(conn, `SELECT name, value FROM mysql.rds_configuration WHERE name = 'binlog retention hours';`)
	logrus.WithFields(logrus.Fields{"hours": rdsRetentionHours, "err": err}).Debug("queried RDS-specific binlog retention setting")
	if err == nil {
		return time.Duration(rdsRetentionHours) * time.Hour, nil
	}

	// The newer 'binlog_expire_logs_seconds' variable takes priority if it exists and is nonzero.
	expireLogsSeconds, err := queryNumericVariable(conn, `SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';`)
	logrus.WithFields(logrus.Fields{"seconds": expireLogsSeconds, "err": err}).Debug("queried MySQL variable 'binlog_expire_logs_seconds'")
	if err == nil && expireLogsSeconds > 0 {
		return time.Duration(expireLogsSeconds * float64(time.Second)), nil
	}

	// And as the final resort we'll check 'expire_logs_days' if 'seconds' was zero or nonexistent.
	expireLogsDays, err := queryNumericVariable(conn, `SHOW VARIABLES LIKE 'expire_logs_days';`)
	logrus.WithFields(logrus.Fields{"days": expireLogsDays, "err": err}).Debug("queried MySQL variable 'expire_logs_days'")
	if err == nil && expireLogsDays > 0 {
		return time.Duration(expireLogsDays) * 24 * time.Hour, nil
	}

	// If both 'binlog_expire_logs_seconds' and 'expire_logs_days' are set to zero
	// MySQL will not automatically purge binlog segments. For simplicity we just
	// represent that as a 'one year' expiry time, since all we need the value for
	// is to make sure it's not too short.
	return 365 * 24 * time.Hour, nil
}

func queryNumericVariable(conn mysqlClient, query string) (float64, error) {
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
