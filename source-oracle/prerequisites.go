package main

import (
	"context"
	//"github.com/estuary/connectors/sqlcapture"
	//"github.com/sirupsen/logrus"
)

func (db *oracleDatabase) SetupPrerequisites(ctx context.Context) []error {
	var errs []error
	return errs
}

const (
	reqMajorVersion = 10
	reqMinorVersion = 0
)

func (db *oracleDatabase) prerequisiteVersion(ctx context.Context) error {
	return nil
}

func (db *oracleDatabase) prerequisiteLogicalReplication(ctx context.Context) error {
	return nil
}

func (db *oracleDatabase) prerequisiteReplicationSlot(ctx context.Context) error {
	return nil
}

func (db *oracleDatabase) prerequisitePublication(ctx context.Context) error {
	return nil
}

func (db *oracleDatabase) prerequisiteWatermarksTable(ctx context.Context) error {
	return nil
}

func (db *oracleDatabase) SetupTablePrerequisites(ctx context.Context, schema, table string) error {
	return nil
}

// addTableToPublication adds a table to a publication if it isn't already part of that publication.
func (db *oracleDatabase) addTableToPublication(ctx context.Context, pubName string, schema string, table string) error {
	return nil
}

// isTablePublished checks whether a particular table is published via the named publication. It caches
// the database query results so that only a single round-trip is required no matter how many tables we
// end up checking.
func (db *oracleDatabase) isTablePublished(ctx context.Context, pubName string, schema string, table string) (bool, error) {
	return false, nil
}
