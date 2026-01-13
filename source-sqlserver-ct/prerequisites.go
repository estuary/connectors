package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

func (db *sqlserverDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	if err := db.prerequisiteVersion(ctx); err != nil {
		log.WithField("err", err).Debug("server version prerequisite failed")
	}

	var checks = []func(ctx context.Context) error{
		db.prerequisiteCTEnabled,
		db.prerequisiteConnectionEncryption,
	}

	for _, prereq := range checks {
		if err := prereq(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

const (
	reqMajorVersion = 14
	reqMinorVersion = 0
	reqRelease      = "SQL Server 2017" // Used for error messaging only; not part of version checks
)

// Per https://learn.microsoft.com/en-us/sql/t-sql/functions/version-transact-sql-configuration-functions,
// the product version reported by @@VERSION is incorrect for Azure SQL Database, Azure SQL Managed
// Instance and Azure Synapse Analytics. These are managed services and are always up-to-date, so we
// will not bother checking them. They have EngineEdition's of '5' for Azure SQL Database, '8' for
// Azure SQL Managed Instance, and '6' or '11' for Azure Synapse.
var managedEditions = map[int]bool{
	5:  true,
	6:  true,
	8:  true,
	11: true,
}

func (db *sqlserverDatabase) prerequisiteVersion(ctx context.Context) error {
	var engineEdition int
	var version string

	if err := db.conn.QueryRowContext(ctx, `SELECT SERVERPROPERTY('EngineEdition');`).Scan(&engineEdition); err != nil {
		log.WithField("err", err).Warn("unable to query 'EngineEdition' server property")
	} else if managedEditions[engineEdition] {
		log.WithFields(log.Fields{
			"engineEdition": engineEdition,
		}).Info("skipping database version check for Azure managed database")
		return nil
	} else if err := db.conn.QueryRowContext(ctx, `SELECT SERVERPROPERTY('productversion');`).Scan(&version); err != nil {
		log.WithField("err", err).Warn("unable to query 'productversion' server property")
	} else if len(version) == 0 {
		log.Warn("'productversion' server property query result was empty")
	} else if major, minor, err := sqlcapture.ParseVersion(version); err != nil {
		log.WithFields(log.Fields{"version": version, "err": err}).Warn("unable to parse server version")
	} else if !sqlcapture.ValidVersion(major, minor, reqMajorVersion, reqMinorVersion) {
		// Return an error only if the actual version could be definitively determined to be less
		// than required.
		return fmt.Errorf(
			"minimum supported SQL Server version is %d.%d (%s): attempted to capture from database version %d.%d",
			reqMajorVersion,
			reqMinorVersion,
			reqRelease,
			major,
			minor,
		)
	} else {
		log.WithFields(log.Fields{
			"version": version,
			"major":   major,
			"minor":   minor,
		}).Info("queried database version")
		return nil
	}

	// Catch-all trailing log message for cases where the server version could not be determined.
	log.Warn(fmt.Sprintf(
		"attempting to capture from unknown database version: minimum supported SQL Server version is %d.%d (%s)",
		reqMajorVersion,
		reqMinorVersion,
		reqRelease,
	))

	return nil
}

func (db *sqlserverDatabase) prerequisiteCTEnabled(ctx context.Context) error {
	var logEntry = log.WithField("db", db.config.Database)
	if ctEnabled, err := isCTEnabled(ctx, db.conn); err != nil {
		return err
	} else if ctEnabled {
		logEntry.Debug("Change Tracking already enabled on database")
		return nil
	}

	logEntry.Info("Change Tracking not enabled, attempting to enable it")
	var query = fmt.Sprintf(`ALTER DATABASE [%s] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);`, db.config.Database)
	if _, err := db.conn.ExecContext(ctx, query); err == nil {
		if ctEnabled, err := isCTEnabled(ctx, db.conn); err != nil {
			return err
		} else if ctEnabled {
			logEntry.Info("successfully enabled Change Tracking on database")
			return nil
		}
	} else {
		logEntry.WithField("err", err).Error("unable to enable Change Tracking")
	}

	return fmt.Errorf("Change Tracking is not enabled on database %q and user %q cannot enable it", db.config.Database, db.config.User)
}

func isCTEnabled(ctx context.Context, conn *sql.DB) (bool, error) {
	// CHANGE_TRACKING_CURRENT_VERSION() returns NULL if CT is not enabled on the database.
	var version sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT CHANGE_TRACKING_CURRENT_VERSION();`).Scan(&version); err != nil {
		return false, fmt.Errorf("unable to query Change Tracking status: %w", err)
	}
	return version.Valid, nil
}

func (db *sqlserverDatabase) prerequisiteConnectionEncryption(ctx context.Context) error {
	var sessionId int
	var encryptOption bool
	if err := db.conn.QueryRowContext(ctx, `SELECT @@SPID, encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID;`).Scan(&sessionId, &encryptOption); err != nil {
		log.WithField("err", err).Debug("unable to query connection encryption option")
	} else {
		log.WithFields(log.Fields{
			"encrypted": encryptOption,
			"sessionId": sessionId,
		}).Info("connection encryption option")
	}

	return nil
}

func (db *sqlserverDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	if err := db.prerequisiteTableExists(ctx, schema, table); err != nil {
		return err
	}
	return db.prerequisiteTableCTEnabled(ctx, schema, table)
}

func (db *sqlserverDatabase) prerequisiteTableExists(ctx context.Context, schema, table string) error {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var objectName = fmt.Sprintf("[%s].[%s]", schema, table)
	var objectID sql.NullInt64
	if err := db.conn.QueryRowContext(ctx, `SELECT OBJECT_ID(@p1, 'U');`, objectName).Scan(&objectID); err != nil {
		return fmt.Errorf("unable to check if table %q exists: %w", streamID, err)
	}
	if !objectID.Valid {
		return fmt.Errorf("table %q does not exist", streamID)
	}
	return nil
}

func (db *sqlserverDatabase) prerequisiteTableCTEnabled(ctx context.Context, schema, table string) error {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var logEntry = log.WithField("table", streamID)

	// Check if Change Tracking is already enabled for this table.
	if ctEnabled, err := db.isTableCTEnabled(ctx, schema, table); err != nil {
		return fmt.Errorf("unable to query Change Tracking status for table %q: %w", streamID, err)
	} else if ctEnabled {
		logEntry.Debug("Change Tracking already enabled for table")
		return nil
	}

	// Check if the table has a primary key (required for Change Tracking).
	if hasPK, err := tableHasPrimaryKey(ctx, db.conn, schema, table); err != nil {
		return fmt.Errorf("unable to query primary key for table %q: %w", streamID, err)
	} else if !hasPK {
		return fmt.Errorf("table %q does not have a primary key, which is required for Change Tracking", streamID)
	}

	// Attempt to enable Change Tracking on the table.
	logEntry.Info("Change Tracking not enabled for table, attempting to enable it")
	var query = fmt.Sprintf(`ALTER TABLE [%s].[%s] ENABLE CHANGE_TRACKING;`, schema, table)
	if _, err := db.conn.ExecContext(ctx, query); err == nil {
		logEntry.Info("successfully enabled Change Tracking for table")
		return nil
	} else {
		logEntry.WithField("err", err).Error("unable to enable Change Tracking for table")
	}

	return fmt.Errorf("Change Tracking is not enabled for table %q and user %q cannot enable it", streamID, db.config.User)
}

func tableHasPrimaryKey(ctx context.Context, conn *sql.DB, schema, table string) (bool, error) {
	var count int
	var query = `
		SELECT COUNT(*)
		FROM sys.indexes i
		WHERE i.object_id = OBJECT_ID(@p1)
		  AND i.is_primary_key = 1;
	`
	var objectName = fmt.Sprintf("[%s].[%s]", schema, table)
	if err := conn.QueryRowContext(ctx, query, objectName).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
