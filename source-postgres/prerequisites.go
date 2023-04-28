package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

func (db *postgresDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error

	for _, prereq := range []func(ctx context.Context) error{
		db.prerequisiteLogicalReplication,
		//db.prerequisiteReplicationUser,
		db.prerequisiteReplicationSlot,
		db.prerequisitePublication,
		db.prerequisiteWatermarksTable,
		db.prerequisiteWatermarksInPublication,
	} {
		if err := prereq(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
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
	var query = fmt.Sprintf(`SELECT rolreplication FROM pg_catalog.pg_roles WHERE rolname = '%s'`, db.config.User)
	var replication bool
	if err := db.conn.QueryRow(ctx, query).Scan(&replication); err != nil {
		return fmt.Errorf("error querying REPLICATION role for user %q: %w", db.config.User, err)
	}
	if !replication {
		return fmt.Errorf("user %q must have the REPLICATION role", db.config.User)
	}
	return nil
}

func (db *postgresDatabase) prerequisiteReplicationSlot(ctx context.Context) error {
	// If the replication slot already exists then we're satisfied
	var slotName = db.config.Advanced.SlotName
	var logEntry = logrus.WithField("slot", slotName)
	var count int
	if err := db.conn.QueryRow(ctx, fmt.Sprintf(`SELECT COUNT(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = '%s' AND slot_type = 'logical';`, slotName)).Scan(&count); err != nil {
		return fmt.Errorf("error querying replication slots: %w", err)
	}
	if count == 1 {
		logEntry.Debug("replication slot exists")
		return nil
	}

	// Otherwise try and create it
	logEntry.Info("attempting to create replication slot")
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`SELECT pg_create_logical_replication_slot('%s', 'pgoutput');`, slotName)); err != nil {
		return fmt.Errorf("replication slot %q doesn't exist and couldn't be created", slotName)
	}

	logEntry.Info("created replication slot")
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

	return nil
}

func (db *postgresDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	var table = db.config.Advanced.WatermarksTable
	var logEntry = logrus.WithField("table", table)

	// If we can successfully write a watermark then we're satisfied here
	if err := db.WriteWatermark(ctx, "existence-check"); err == nil {
		logEntry.Debug("watermarks table already exists")
		return nil
	}

	// If we can create the watermarks table and then write a watermark, that also works
	logEntry.Info("watermarks table doesn't exist, attempting to create it")
	var _, err = db.conn.Exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot TEXT PRIMARY KEY, watermark TEXT);", table))
	if err == nil {
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

	var pubName = db.config.Advanced.PublicationName
	var watermarks = db.config.Advanced.WatermarksTable

	// (*Config).Validate() has previously verified that this value contains a period. The first
	// part is the schema and the second part is the table.
	tableParts := strings.Split(watermarks, ".")

	return db.addTableToPublication(ctx, pubName, tableParts[0], tableParts[1])
}

func (db *postgresDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	var rows, err = db.conn.Query(ctx, fmt.Sprintf(`SELECT * FROM "%s"."%s" LIMIT 0;`, schema, table))
	rows.Close()
	if err != nil {
		var streamID = sqlcapture.JoinStreamID(schema, table)
		return fmt.Errorf("user %q cannot read from table %q", db.config.User, streamID)
	}

	return db.addTableToPublication(ctx, db.config.Advanced.PublicationName, schema, table)
}

// addTableToPublication adds a table to a publication if it isn't already part of that publication.
func (db *postgresDatabase) addTableToPublication(ctx context.Context, pubName string, schema string, table string) error {
	var logEntry = logrus.WithFields(logrus.Fields{
		"publication": pubName,
		"schema":      schema,
		"table":       table,
	})

	// The table may have already been added to the publication by a previous invocation, or the
	// publication was created as FOR ALL TABLES. In either case it will show up in this query, and
	// we won't try to add it to the publication again.
	var count int
	if err := db.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM pg_catalog.pg_publication_tables 
		WHERE
			pubname = '%s' AND
			schemaname = '%s' AND
			tablename = '%s';
		`,
		pubName,
		schema,
		table,
	)).Scan(&count); err != nil {
		return fmt.Errorf("error querying publications: %w", err)
	}
	if count == 1 {
		logEntry.Debug("table is already part of publication")
		return nil
	}

	logEntry.Info("attempting to add table to publication")

	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`ALTER PUBLICATION "%s" ADD TABLE "%s"."%s";`, pubName, schema, table)); err != nil {
		return fmt.Errorf("could not add table %s.%s to publication %s", schema, table, pubName)
	}

	logEntry.Info("added table to publication")

	return nil
}
