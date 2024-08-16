package main

import (
	"context"
	"fmt"
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

	return errs
}

func (db *oracleDatabase) prerequisiteSupplementalLogging(ctx context.Context, owner, tableName string) error {
	var row = db.conn.QueryRowContext(ctx, "SELECT SUPPLEMENTAL_LOG_DATA_ALL, SUPPLEMENTAL_LOG_DATA_MIN FROM V$DATABASE")
	var all, min string
	if err := row.Scan(&all, &min); err != nil {
		return fmt.Errorf("querying supplemental_log_data from V$DATABASE: %w", err)
	}

	if all != "YES" {
		if min != "YES" {
			return fmt.Errorf("supplemental logging not enabled. Please enable supplemental logging using `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`")
		}

		if owner != "" && tableName != "" {
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

func (db *oracleDatabase) SetupTablePrerequisites(ctx context.Context, owner, tableName string) error {
	if err := db.prerequisiteSupplementalLogging(ctx, owner, tableName); err != nil {
		return err
	} else if err := db.prerequisiteTableAndColumnNameLengths(ctx, owner, tableName); err != nil {
		return err
	}

	return nil
}
