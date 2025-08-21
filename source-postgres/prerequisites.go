package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

func (db *postgresDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	if err := db.prerequisiteVersion(ctx); err != nil {
		// Return early if the database version is incompatible with the connector since additional
		// errors will be of minimal use.
		errs = append(errs, err)
		return errs
	}

	var checks = []func(ctx context.Context) error{
		db.prerequisiteLogicalReplication,
		db.prerequisiteReplicationUser,
		db.prerequisiteReplicationSlot,
		db.prerequisitePublication,
	}

	if !db.config.Advanced.ReadOnlyCapture {
		// We only care about the watermarks table when not in read-only mode
		checks = append(checks,
			db.prerequisiteWatermarksTable,
			db.prerequisiteWatermarksInPublication,
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
	reqMajorVersion = 10
	reqMinorVersion = 0
)

func (db *postgresDatabase) prerequisiteVersion(ctx context.Context) error {
	var version string
	if err := db.conn.QueryRow(ctx, `SHOW server_version`).Scan(&version); err != nil {
		logrus.Warn(fmt.Errorf("unable to query 'server_version' system variable: %w", err))
	} else if len(version) == 0 {
		logrus.Warn("'server_version' system variable query result was empty")
	} else if major, minor, err := sqlcapture.ParseVersion(version); err != nil {
		logrus.Warn(fmt.Errorf("unable to parse server version from '%s': %w", version, err))
	} else if !sqlcapture.ValidVersion(major, minor, reqMajorVersion, reqMinorVersion) {
		// Return an error only if the actual version could be definitively determined to be less
		// than required.
		return fmt.Errorf(
			"minimum supported Postgres version is %d.%d: attempted to capture from database version %d.%d",
			reqMajorVersion,
			reqMinorVersion,
			major,
			minor,
		)
	} else {
		logrus.WithFields(logrus.Fields{
			"version": version,
			"major":   major,
			"minor":   minor,
		}).Info("queried database version")
		return nil
	}

	// Catch-all trailing log message for cases where the server version could not be determined.
	logrus.Warn(fmt.Sprintf(
		"attempting to capture from unknown database version: minimum supported Postgres version is %d.%d",
		reqMajorVersion,
		reqMinorVersion,
	))

	return nil
}

func (db *postgresDatabase) prerequisiteLogicalReplication(ctx context.Context) error {
	var level string
	if err := db.conn.QueryRow(ctx, `SHOW wal_level;`).Scan(&level); err != nil {
		return fmt.Errorf("unable to query 'wal_level' system variable: %w", err)
	} else if level != "logical" {
		return fmt.Errorf("logical replication isn't enabled: current wal_level = %q", level)
	}
	return nil
}

func (db *postgresDatabase) prerequisiteReplicationUser(ctx context.Context) error {
	// As a first resort, check if the user has the REPLICATION role. This check
	// covers all managed Postgres providers except for RDS.
	var rolreplication bool
	if err := db.conn.QueryRow(ctx, fmt.Sprintf(`SELECT rolreplication FROM pg_catalog.pg_roles WHERE rolname = '%s'`, db.config.User)).Scan(&rolreplication); err != nil {
		return fmt.Errorf("error querying REPLICATION role for user %q: %w", db.config.User, err)
	}
	if rolreplication {
		return nil
	}

	// If that check fails then the user doesn't have REPLICATION, but maybe we're
	// on RDS which uses membership in the `rds_replication` role. Check that too.
	// Note that this query will result in an error on non-RDS Postgres instances,
	// so we ignore errors from this query.
	var rdsreplication bool
	if err := db.conn.QueryRow(ctx, fmt.Sprintf(`SELECT pg_has_role('%s', 'rds_replication', 'member');`, db.config.User)).Scan(&rdsreplication); err == nil {
		if rdsreplication {
			return nil
		}
	}

	return fmt.Errorf("user %q must have the REPLICATION role (or 'rds_replication' on RDS)", db.config.User)
}

func (db *postgresDatabase) prerequisiteReplicationSlot(ctx context.Context) error {
	// If the replication slot already exists in our configured database then we're satisfied.
	var slotName = db.config.Advanced.SlotName
	var configuredDatabase = db.config.Database
	var logEntry = logrus.WithFields(logrus.Fields{
		"slot":     slotName,
		"database": configuredDatabase,
	})
	var slotDatabase string
	if err := db.conn.QueryRow(ctx, fmt.Sprintf(`SELECT database FROM pg_catalog.pg_replication_slots WHERE slot_name = '%s' AND slot_type = 'logical';`, slotName)).Scan(&slotDatabase); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("error querying replication slots: %w", err)
		}
	}
	if slotDatabase == configuredDatabase {
		logEntry.Debug("replication slot exists")
		return nil
	}

	// Slot exists, but not in our database.
	if slotDatabase != "" {
		return fmt.Errorf(
			"replication slot %q exists in database %q, but the configured database is %q: consider using a different slot name or removing the existing slot",
			slotName,
			slotDatabase,
			configuredDatabase,
		)
	}

	// Slot does not exist in any database, and the 'no_create_replication_slot' flag is set.
	// In this case we know that the slot won't be created automatically later on, so we ought
	// to present the user with a nice validation error here.
	if !db.featureFlags["create_replication_slot"] {
		return fmt.Errorf("replication slot %q does not exist and the 'no_create_replication_slot' feature flag is set", slotName)
	}

	// Slot does not exist in any database. This is fine because we'll autocreate it when
	// the actual replication process starts, and we've already verified that the user has
	// the REPLICATION role which is all they need to be able to create the slot.
	return nil
}

func (db *postgresDatabase) prerequisitePublication(ctx context.Context) error {
	var pubName = db.config.Advanced.PublicationName
	var logEntry = logrus.WithField("publication", pubName)

	var count int
	if err := db.conn.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM pg_catalog.pg_publication WHERE pubname = '%s';`, pubName)).Scan(&count); err != nil {
		return fmt.Errorf("error querying publications: %w", err)
	}
	if count == 1 {
		logEntry.Debug("publication exists")
		return nil
	}

	logEntry.Info("attempting to create publication")
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION "%s";`, pubName)); err != nil {
		return fmt.Errorf("publication %q doesn't exist and couldn't be created", pubName)
	}

	// We attempt to set the `publish_via_partition_root` flag when creating the publication ourselves,
	// unless the user has enabled CaptureAsPartitions mode.
	// If the user already created the publication we won't try and set the flag, that's their job.
	// The main reason this might fail is if we're running against a pre-v13 database, which doesn't
	// have this flag, so we log but ignore any errors here.
	if !db.config.Advanced.CaptureAsPartitions {
		if _, err := db.conn.Exec(ctx, fmt.Sprintf(`ALTER PUBLICATION "%s" SET (publish_via_partition_root = true)`, pubName)); err != nil {
			logEntry.WithField("err", err).Warn("unable to set publish_via_partition_root flag (this is normal for versions < 13)")
		}
	} else {
		logEntry.Info("skipping publish_via_partition_root flag for partition-level capture")
	}

	return nil
}

func (db *postgresDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	var table = db.config.Advanced.WatermarksTable
	var logEntry = logrus.WithField("table", table)

	// If we can successfully write a watermark then we're satisfied here
	var err = db.WriteWatermark(ctx, "existence-check")
	if err == nil {
		logEntry.Debug("watermarks table already exists")
		return nil
	}
	logEntry.WithField("err", err).Warn("error writing to watermarks table")

	// If we can create the watermarks table and then write a watermark, that also works
	logEntry.Info("attempting to create watermarks table")
	if _, err := db.conn.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot TEXT PRIMARY KEY, watermark TEXT);", table)); err == nil {
		if err := db.WriteWatermark(ctx, "existence-check"); err == nil {
			logEntry.Info("successfully created watermarks table")
			return nil
		}
	} else {
		logEntry.WithField("err", err).Error("failed to create watermarks table")
	}

	// Otherwise this is a failure
	return fmt.Errorf("user %q cannot write to the watermarks table %q", db.config.User, table)
}

func (db *postgresDatabase) prerequisiteWatermarksInPublication(ctx context.Context) error {
	// The watermarks table must be present in the publication. This assumes that the watermarks
	// table and publication have already attempted to be created if they don't exist. If either the
	// watermarks table or publication doesn't exist another error will be generated here which is a
	// bit redundant.
	//
	// (*Config).Validate() has previously verified that this value contains a period. The first
	// part is the schema and the second part is the table.
	tableParts := strings.Split(db.config.Advanced.WatermarksTable, ".")
	return db.addTableToPublication(ctx, tableParts[0], tableParts[1])
}

func (db *postgresDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	var rows, err = db.conn.Query(ctx, fmt.Sprintf(`SELECT * FROM "%s"."%s" LIMIT 0;`, schema, table))
	rows.Close()
	if err != nil {
		var streamID = sqlcapture.JoinStreamID(schema, table)
		return fmt.Errorf("user %q cannot read from table %q", db.config.User, streamID)
	}

	return db.addTableToPublication(ctx, schema, table)
}

// addTableToPublication adds a table to a publication if it isn't already part of that publication.
func (db *postgresDatabase) addTableToPublication(ctx context.Context, schema string, table string) error {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var pubName = db.config.Advanced.PublicationName
	var logEntry = logrus.WithFields(logrus.Fields{
		"publication": pubName,
		"schema":      schema,
		"table":       table,
	})

	// If the table is already published, do nothing.
	if pub, err := db.isTablePublished(ctx, schema, table); err != nil {
		return fmt.Errorf("error checking publication status for table %q: %w", streamID, err)
	} else if pub {
		logEntry.Debug("table is already part of publication")
		return nil
	}

	// Otherwise add the table to the publication
	logEntry.Info("attempting to add table to publication")
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`ALTER PUBLICATION "%s" ADD TABLE "%s"."%s";`, pubName, schema, table)); err != nil {
		return fmt.Errorf("table %q is not in publication %s and couldn't be added automatically", streamID, pubName)
	}
	db.tablesPublished[streamID] = true // Probably unnecessary but good hygiene
	logEntry.Info("added table to publication")

	return nil
}

// isTablePublished checks whether a particular table is published via the named publication. It caches
// the database query results so that only a single round-trip is required no matter how many tables we
// end up checking.
func (db *postgresDatabase) isTablePublished(ctx context.Context, schema string, table string) (bool, error) {
	if db.tablesPublished == nil {
		var tablesPublished, err = listPublishedTables(ctx, db.conn, db.config.Advanced.PublicationName)
		if err != nil {
			return false, err
		}
		db.tablesPublished = tablesPublished
	}

	var streamID = sqlcapture.JoinStreamID(schema, table)
	return db.tablesPublished[streamID], nil
}
