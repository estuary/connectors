package main

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/estuary/connectors/go/capture/sqlserver/backfill"
	"github.com/estuary/connectors/go/capture/sqlserver/version"
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

func (db *sqlserverDatabase) prerequisiteVersion(ctx context.Context) error {
	info, err := version.Query(ctx, db.conn)
	if err != nil {
		log.WithField("err", err).Warn("unable to query server version")
		return nil
	}

	log.WithFields(log.Fields{
		"engineEdition": info.EngineEditionName,
		"edition":       info.Edition,
		"version":       info.ProductVersion,
		"productLevel":  info.ProductLevel,
		"major":         info.MajorVersion,
		"minor":         info.MinorVersion,
	}).Info("queried database version")

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
	var query = fmt.Sprintf(`ALTER DATABASE %s SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);`, backfill.QuoteIdentifier(db.config.Database))
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

func (db *sqlserverDatabase) SetupTablePrerequisites(ctx context.Context, tables []sqlcapture.TableID) map[sqlcapture.TableID]error {
	var errs = make(map[sqlcapture.TableID]error)

	// Query CT-enabled tables once upfront for efficiency
	ctEnabledTables, err := db.ctEnabledTables(ctx)
	if err != nil {
		for _, table := range tables {
			errs[table] = fmt.Errorf("unable to query CT-enabled tables: %w", err)
		}
		return errs
	}

	for _, table := range tables {
		if err := db.prerequisiteTableCTEnabled(ctx, ctEnabledTables, table.Schema, table.Table); err != nil {
			errs[table] = err
		}
	}
	return errs
}

func (db *sqlserverDatabase) prerequisiteTableCTEnabled(ctx context.Context, ctEnabledTables map[sqlcapture.StreamID]bool, schema, table string) error {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var logEntry = log.WithField("table", streamID)

	// Check if Change Tracking is already enabled for this table. If so, we're done
	// (and the table must necessarily exist if CT is enabled on it).
	if ctEnabledTables[streamID] {
		logEntry.Debug("Change Tracking already enabled for table")
		return nil
	}

	// Check if the table exists.
	if exists, err := tableExists(ctx, db.conn, schema, table); err != nil {
		return fmt.Errorf("unable to check if table %q exists: %w", streamID, err)
	} else if !exists {
		return fmt.Errorf("table %q does not exist", streamID)
	}

	// Check if the table has a primary key (required for Change Tracking).
	if hasPK, err := tableHasPrimaryKey(ctx, db.conn, schema, table); err != nil {
		return fmt.Errorf("unable to query primary key for table %q: %w", streamID, err)
	} else if !hasPK {
		return fmt.Errorf("table %q does not have a primary key, which is required for Change Tracking", streamID)
	}

	// Attempt to enable Change Tracking on the table.
	logEntry.Info("Change Tracking not enabled for table, attempting to enable it")
	var query = fmt.Sprintf(`ALTER TABLE %s ENABLE CHANGE_TRACKING;`, backfill.QuoteTableName(schema, table))
	if _, err := db.conn.ExecContext(ctx, query); err == nil {
		logEntry.Info("successfully enabled Change Tracking for table")
		return nil
	} else {
		logEntry.WithField("err", err).Error("unable to enable Change Tracking for table")
	}

	return fmt.Errorf("Change Tracking is not enabled for table %q and user %q cannot enable it", streamID, db.config.User)
}

func tableExists(ctx context.Context, conn *sql.DB, schema, table string) (bool, error) {
	var objectName = backfill.QuoteTableName(schema, table)
	var objectID sql.NullInt64
	if err := conn.QueryRowContext(ctx, `SELECT OBJECT_ID(@p1, 'U');`, objectName).Scan(&objectID); err != nil {
		return false, err
	}
	return objectID.Valid, nil
}

func tableHasPrimaryKey(ctx context.Context, conn *sql.DB, schema, table string) (bool, error) {
	var count int
	var query = `
		SELECT COUNT(*)
		FROM sys.indexes i
		WHERE i.object_id = OBJECT_ID(@p1)
		  AND i.is_primary_key = 1;
	`
	var objectName = backfill.QuoteTableName(schema, table)
	if err := conn.QueryRowContext(ctx, query, objectName).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
