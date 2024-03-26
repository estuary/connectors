package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

func (db *sqlserverDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	//if err := db.prerequisiteVersion(ctx); err != nil {
	//	// Return early if the database version is incompatible with the connector since additional
	//	// errors will be of minimal use.
	//	errs = append(errs, err)
	//	return errs
	//}

	for _, prereq := range []func(ctx context.Context) error{
		db.prerequisiteCDCEnabled,
		db.prerequisiteWatermarksTable,
		db.prerequisiteWatermarksCaptureInstance,
		db.prerequisiteMaximumLSN,
	} {
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

func (db *sqlserverDatabase) prerequisiteMaximumLSN(ctx context.Context) error {
	// By writing a watermark here we ensure that there is at least one change event for
	// the agent process to observe, and thus the "get max LSN" query should eventually
	// yield a non-empty result. We ignore errors here, because the watermarks table
	// prerequisite is responsible for reporting those.
	_ = db.WriteWatermark(ctx, "dummy-value")

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

	// TODO(wgd): It's rather inefficient to redo the 'list instances' work for each table.
	var captureInstances, err = listCaptureInstances(ctx, db.conn)
	if err != nil {
		return fmt.Errorf("unable to query capture instances for table %q: %w", streamID, err)
	}

	// If the table has at least one preexisting capture instance then we're happy
	var instanceNames = captureInstances[streamID]
	if len(instanceNames) > 0 {
		logEntry.WithField("instances", instanceNames).Debug("table has capture instances")
		return nil
	}

	// Otherwise we attempt to create one
	const query = `EXEC sys.sp_cdc_enable_table @source_schema = @p1, @source_name = @p2, @role_name = @p3, @capture_instance = @p4;`
	var instanceName = fmt.Sprintf("%s_%s", schema, table)
	if _, err := db.conn.ExecContext(ctx, query, schema, table, db.config.User, instanceName); err == nil {
		logEntry.WithField("instance", instanceName).Info("enabled cdc for table")
		return nil
	}
	return fmt.Errorf("table %q has no capture instances and user %q cannot create one", streamID, db.config.User)
}

// listCaptureInstances queries SQL Server system tables and returns a map from stream IDs
// to the capture instance name, if a capture instance exists which matches the configured
// naming pattern.
func listCaptureInstances(ctx context.Context, conn *sql.DB) (map[string][]string, error) {
	log.Trace("listing capture instances")
	// This query will enumerate all "capture instances" currently present, along with the
	// schema/table names identifying the source table.
	const query = `SELECT sch.name, tbl.name, ct.capture_instance
	                 FROM cdc.change_tables AS ct
					 JOIN sys.tables AS tbl ON ct.source_object_id = tbl.object_id
					 JOIN sys.schemas AS sch ON tbl.schema_id = sch.schema_id;`
	var rows, err = conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error listing CDC instances: %w", err)
	}
	defer rows.Close()

	// Process result rows from the above query
	var captureInstances = make(map[string][]string)
	for rows.Next() {
		var schemaName, tableName, instanceName string
		if err := rows.Scan(&schemaName, &tableName, &instanceName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		// In SQL Server, every source table may have up to two "capture instances" associated with it.
		// If a capture instance exists which satisfies the configured naming pattern, then that's one
		// that we should use (and thus if the pattern is "flow_<schema>_<table>" we can be fairly sure
		// not to collide with any other uses of CDC on this database).
		var streamID = sqlcapture.JoinStreamID(schemaName, tableName)
		log.WithFields(log.Fields{
			"stream":   streamID,
			"instance": instanceName,
		}).Trace("discovered capture instance")
		captureInstances[streamID] = append(captureInstances[streamID], instanceName)
	}
	return captureInstances, nil
}
