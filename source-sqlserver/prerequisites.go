package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	mssqldb "github.com/microsoft/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

func (db *sqlserverDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	if err := db.prerequisiteVersion(ctx); err != nil {
		log.WithField("err", err).Debug("server version prerequisite failed")
	}

	var checks = []func(ctx context.Context) error{
		db.prerequisiteCDCEnabled,
		db.prerequisiteChangeTableCleanup,
		db.prerequisiteCaptureInstanceManagement,
		db.prerequisiteMaximumLSN,
		db.prerequisiteConnectionEncryption,
	}

	// In non-read-only mode we still need to verify watermarks table usability.
	// In read-only non-replica mode we need to verify that sys.dm_cdc_log_scan_sessions is accessible.
	// In read-only replica mode we have no additional requirements.
	var isReplica, _ = isReplicaDatabase(ctx, db.conn)
	if !db.featureFlags["read_only"] {
		checks = append(checks,
			db.prerequisiteWatermarksTable,
			db.prerequisiteWatermarksCaptureInstance,
		)
	} else if isReplica || db.featureFlags["replica_fencing"] {
		// No additional requirements for read-only replica mode.
	} else {
		checks = append(checks,
			db.prerequisiteViewCDCScanHistory,
		)
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
		log.Warn(fmt.Errorf("unable to query 'EngineEdition' server property: %w", err))
	} else if managedEditions[engineEdition] {
		log.WithFields(log.Fields{
			"engineEdition": engineEdition,
		}).Info("skipping database version check for Azure managed database")
		return nil
	} else if err := db.conn.QueryRowContext(ctx, `SELECT SERVERPROPERTY('productversion');`).Scan(&version); err != nil {
		log.Warn(fmt.Errorf("unable to query 'productversion' server property: %w", err))
	} else if len(version) == 0 {
		log.Warn("'productversion' server property query result was empty")
	} else if major, minor, err := sqlcapture.ParseVersion(version); err != nil {
		log.Warn(fmt.Errorf("unable to parse server version from '%s': %w", version, err))
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

func (db *sqlserverDatabase) prerequisiteCDCEnabled(ctx context.Context) error {
	var logEntry = log.WithField("db", db.config.Database)
	if cdcEnabled, err := isCDCEnabled(ctx, db.conn, db.config.Database); err != nil {
		return err
	} else if cdcEnabled {
		logEntry.Debug("CDC already enabled on database")
		return nil
	}

	logEntry.Info("CDC not enabled, attempting to enable it")
	if _, err := db.conn.ExecContext(ctx, `EXEC sys.sp_cdc_enable_db;`); err == nil {
		if cdcEnabled, err := isCDCEnabled(ctx, db.conn, db.config.Database); err != nil {
			return err
		} else if cdcEnabled {
			logEntry.Info("successfully enabled CDC on database")
			return nil
		}
	} else {
		logEntry.WithField("err", err).Error("unable to enable CDC")
	}

	return fmt.Errorf("CDC is not enabled on database %q and user %q cannot enable it", db.config.Database, db.config.User)
}

func isCDCEnabled(ctx context.Context, conn *sql.DB, dbName string) (bool, error) {
	var cdcEnabled bool
	if err := conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT is_cdc_enabled FROM sys.databases WHERE name = '%s';`, dbName)).Scan(&cdcEnabled); err != nil {
		return false, fmt.Errorf("unable to query CDC status of database %q: %w", dbName, err)
	}
	return cdcEnabled, nil
}

func (db *sqlserverDatabase) prerequisiteChangeTableCleanup(ctx context.Context) error {
	// If automatic change-table cleanup is not in use there is nothing to verify here.
	if !db.config.Advanced.AutomaticChangeTableCleanup {
		return nil
	}

	if hasPermission, err := currentUserHasDBOwner(ctx, db.conn); err != nil {
		return fmt.Errorf("error querying 'db_owner' role membership: %w", err)
	} else if !hasPermission {
		return fmt.Errorf("user %q does not have the \"db_owner\" role which is required for automatic change table cleanup", db.config.User)
	}
	return nil
}

func (db *sqlserverDatabase) prerequisiteCaptureInstanceManagement(ctx context.Context) error {
	// If automatic capture instance management is not in use there is nothing to verify here.
	if !db.config.Advanced.AutomaticCaptureInstances {
		return nil
	}

	if hasPermission, err := currentUserHasDBOwner(ctx, db.conn); err != nil {
		return fmt.Errorf("error querying 'db_owner' role membership: %w", err)
	} else if !hasPermission {
		return fmt.Errorf("user %q does not have the \"db_owner\" role which is required for automatic change table cleanup", db.config.User)
	}
	return nil
}

func (db *sqlserverDatabase) prerequisiteConnectionEncryption(ctx context.Context) error {
	var sessionId int
	if err := db.conn.QueryRowContext(ctx, `SELECT @@SPID;`).Scan(&sessionId); err != nil {
		log.Warn(fmt.Errorf("unable to query @@SPID: %w", err))
	}

	var encryptOption bool
	if err := db.conn.QueryRowContext(ctx, `SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID;`).Scan(&encryptOption); err != nil {
		log.WithFields(log.Fields{
			"err":       err,
			"sessionId": sessionId,
		}).Debug("unable to query connection encryption option")
	} else {
		log.WithFields(log.Fields{
			"encrypted": encryptOption,
			"sessionId": sessionId,
		}).Info("connection encryption option")
	}

	return nil
}

func currentUserHasDBOwner(ctx context.Context, conn *sql.DB) (bool, error) {
	var isMember *int
	if err := conn.QueryRowContext(ctx, "SELECT IS_ROLEMEMBER('db_owner');").Scan(&isMember); err != nil {
		return false, err
	} else if isMember == nil {
		return false, fmt.Errorf("unexpected null result") // should never happen
	} else if *isMember == 1 {
		return true, nil
	}
	return false, nil
}

func (db *sqlserverDatabase) prerequisiteViewCDCScanHistory(ctx context.Context) error {
	var _, err = latestCDCLogScanSession(ctx, db.conn)
	if err != nil {
		log.WithField("err", err).Error("failed to list CDC log scan sessions")
		var msErr mssqldb.Error
		if errors.As(err, &msErr) && msErr.SQLErrorNumber() == 297 { // Error 297 = "The user does not have permission to perform this action"
			return fmt.Errorf("user %q needs the VIEW DATABASE STATE permission", db.config.User)
		}
		return fmt.Errorf("failed to query sys.dm_cdc_log_scan_sessions: %w", err)
	}
	return nil
}

func (db *sqlserverDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	var table = db.config.Advanced.WatermarksTable
	var logEntry = log.WithField("table", table)

	if err := db.WriteWatermark(ctx, "existence-check"); err == nil {
		logEntry.Debug("watermarks table already exists")
		return nil
	}

	// If we can create the watermarks table and then write a watermark, that also works
	logEntry.Info("watermarks table doesn't exist, attempting to create it")
	if err := db.createWatermarksTable(ctx); err == nil {
		if err := db.WriteWatermark(ctx, "existence-check"); err == nil {
			logEntry.Info("successfully created watermarks table")
			return nil
		} else {
			logEntry.WithField("err", err).Error("watermark write failed")
		}
	} else {
		logEntry.WithField("err", err).Error("failed to create watermarks table")
	}

	// Otherwise this is a failure
	return fmt.Errorf("user %q cannot write to the watermarks table %q", db.config.User, table)
}

func (db *sqlserverDatabase) prerequisiteWatermarksCaptureInstance(ctx context.Context) error {
	var schema, table = splitStreamID(db.config.Advanced.WatermarksTable)
	return db.prerequisiteTableCaptureInstance(ctx, schema, table)
}

func splitStreamID(streamID string) (string, string) {
	var bits = strings.SplitN(streamID, ".", 2)
	return bits[0], bits[1]
}

func (db *sqlserverDatabase) prerequisiteMaximumLSN(ctx context.Context) error {
	var maxLSN []byte
	// Retry loop with a 1s delay between retries, in case the watermarks table
	// capture instance was the first one created on this database.
	for retries := 0; retries < 10; retries++ {
		if err := db.conn.QueryRowContext(ctx, `SELECT sys.fn_cdc_get_max_lsn();`).Scan(&maxLSN); err != nil {
			return fmt.Errorf("error querying the current LSN: %w", err)
		}
		if len(maxLSN) > 0 {
			log.WithField("retries", retries).Debug("got current CDC LSN")
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("the agent process may not be running: maximum CDC LSN is currently unset")
}

func (db *sqlserverDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	return db.prerequisiteTableCaptureInstance(ctx, schema, table)
}

func (db *sqlserverDatabase) prerequisiteTableCaptureInstance(ctx context.Context, schema, table string) error {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var logEntry = log.WithField("table", streamID)

	// TODO(wgd): It's inefficient to redo the 'list instances' work for each table,
	// consider caching this across all prerequisite validation calls.
	var captureInstances, err = cdcListCaptureInstances(ctx, db.conn)
	if err != nil {
		return fmt.Errorf("unable to query capture instances for table %q: %w", streamID, err)
	}

	// If the table has at least one preexisting capture instance then we're happy
	var instancesForStream = captureInstances[streamID]
	if len(instancesForStream) > 0 {
		logEntry.WithField("instances", instancesForStream).Debug("table has capture instances")
		return nil
	}

	// Otherwise we attempt to create one
	if instanceName, err := cdcCreateCaptureInstance(ctx, db.conn, schema, table, db.config.User, db.config.Advanced.Filegroup, db.config.Advanced.RoleName); err == nil {
		logEntry.WithField("instance", instanceName).Info("enabled cdc for table")
		return nil
	}
	return fmt.Errorf("table %q has no capture instances and user %q cannot create one", streamID, db.config.User)
}
