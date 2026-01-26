package main

import (
	"context"
	"fmt"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

func (db *oracleDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	for _, prereq := range []func(context.Context, string, string) error{
		db.prerequisiteSupplementalLogging,
	} {
		if err := prereq(ctx, "", ""); err != nil {
			errs = append(errs, err)
		}
	}

	if err := db.prerequisiteArchiveLogRetention(ctx); err != nil {
		errs = append(errs, err)
	}

	return errs
}

func (db *oracleDatabase) prerequisiteArchiveLogRetention(ctx context.Context) error {
	if db.featureFlags["skip_archive_retention_check"] {
		log.Warn("skipping archive log retention check due to feature flag")
		return nil
	}

	if err := db.switchToCDB(ctx); err != nil {
		return err
	}
	var row = db.conn.QueryRowContext(ctx, "select created from v$database")
	var databaseAge time.Time
	if err := row.Scan(&databaseAge); err != nil {
		return fmt.Errorf("querying database age from DBA_OBJECTS: %w", err)
	}

	log.WithFields(log.Fields{
		"created": databaseAge,
	}).Debug("database age")

	if time.Since(databaseAge) < (time.Hour * 24) {
		log.Warn("database age is less than 24 hours, skipping retention checks")
		return nil
	}

	row = db.conn.QueryRowContext(ctx, "SELECT MIN(FIRST_TIME) FROM V$ARCHIVED_LOG A WHERE A.NAME IS NOT NULL")
	var minTimestamp time.Time
	if err := row.Scan(&minTimestamp); err != nil {
		return fmt.Errorf("querying minimum archived log timestamp from V$ARCHIVED_LOG: %w", err)
	}

	if time.Since(minTimestamp) < (time.Hour * 24) {
		return fmt.Errorf("archived log retention is less than 24 hours. Please increase archived log retention to at least 24 hours")
	}

	if time.Since(minTimestamp) < (time.Hour * 24 * 7) {
		log.WithFields(log.Fields{
			"retention": time.Since(minTimestamp),
		}).Warn("archive log retention is less than 7 days, we highly recommend a higher log retention to ensure data consistency.")
	}

	return nil
}

func (db *oracleDatabase) prerequisiteSupplementalLogging(ctx context.Context, owner, tableName string) error {
	if err := db.switchToCDB(ctx); err != nil {
		return err
	}
	var row = db.conn.QueryRowContext(ctx, "SELECT SUPPLEMENTAL_LOG_DATA_ALL, SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")
	var all, min string
	if err := row.Scan(&all, &min); err != nil {
		return fmt.Errorf("querying supplemental_log_data from V$DATABASE: %w", err)
	}

	if all != "YES" {
		if min != "YES" {
			var enableCommand = "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;"
			if db.IsRDS() {
				enableCommand = "BEGIN rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD', p_type   => 'ALL'); END;"
			}
			return fmt.Errorf("supplemental logging not enabled. Please enable supplemental logging using `%s`", enableCommand)
		}

		if owner != "" && tableName != "" {
			if err := db.switchToPDB(ctx); err != nil {
				return err
			}
			row = db.conn.QueryRowContext(ctx, "SELECT LOG_GROUP_TYPE FROM ALL_LOG_GROUPS WHERE OWNER=:1 AND TABLE_NAME=:2", owner, tableName)
			var logType string

			if err := row.Scan(&logType); err != nil {
				return fmt.Errorf("querying log_group_type from all_log_groups for %q.%q: %w", owner, tableName, err)
			}

			if logType != "ALL COLUMN LOGGING" {
				log.WithFields(log.Fields{
					"owner":   owner,
					"table":   tableName,
					"logType": logType,
				}).Warn("supplemental logging (ALL) COLUMNS not enabled on table, we can only capture columns with explicit supplemental logging enabled.")
			}
		}
	}

	if err := db.switchToPDB(ctx); err != nil {
		return err
	}

	return nil
}

// Logminer has a limit on how long table and column names can be
// see https://docs.oracle.com/en/database/oracle/oracle-database/23/sutil/oracle-logminer-utility.html#GUID-7594F0D7-0ACD-46E6-BD61-2751136ECDB4
const maximumNameLength = 30

const queryColumnNames = `SELECT column_name FROM all_tab_columns WHERE owner=:1 AND table_name=:2`

func (db *oracleDatabase) prerequisiteTableAndColumnNameLengths(ctx context.Context, owner, tableName string) error {
	if len(tableName) > maximumNameLength {
		return fmt.Errorf("table names cannot be longer than 30 characters: %q.%q", owner, tableName)
	}

	if rows, err := db.conn.QueryContext(ctx, queryColumnNames, owner, tableName); err != nil {
		return fmt.Errorf("querying columns for table %q.%q: %w", owner, tableName, err)
	} else {
		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				return fmt.Errorf("scanning column name for table %q.%q: %w", owner, tableName, err)
			}

			if len(colName) > maximumNameLength {
				log.WithFields(log.Fields{
					"owner":      owner,
					"table":      tableName,
					"columnName": colName,
				}).Warn("column names longer than 30 characters are not supported for capturing, column will be ignored.")
			}
		}
	}

	return nil
}

func (db *oracleDatabase) SetupTablePrerequisites(ctx context.Context, tables []sqlcapture.TableID) map[sqlcapture.TableID]error {
	var errs = make(map[sqlcapture.TableID]error)
	for _, table := range tables {
		if err := db.setupTablePrerequisite(ctx, table.Schema, table.Table); err != nil {
			errs[table] = err
		}
	}
	return errs
}

func (db *oracleDatabase) setupTablePrerequisite(ctx context.Context, owner, tableName string) error {
	if err := db.prerequisiteSupplementalLogging(ctx, owner, tableName); err != nil {
		return err
	} else if err := db.prerequisiteTableAndColumnNameLengths(ctx, owner, tableName); err != nil {
		return err
	}

	return nil
}
