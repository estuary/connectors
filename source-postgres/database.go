package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

type replicationSlotInfo struct {
	SlotName          string
	Database          string
	Plugin            string
	SlotType          string
	Active            bool
	RestartLSN        *pglogrepl.LSN
	ConfirmedFlushLSN *pglogrepl.LSN
	WALStatus         string
}

// queryReplicationSlotInfo returns information about the named replication slot, if it exists.
// If the slot doesn't exist then a nil pointer is returned, but without an error. An error is
// only returned if the query itself fails.
func queryReplicationSlotInfo(ctx context.Context, conn *pgxpool.Pool, slotName string) (*replicationSlotInfo, error) {
	var info replicationSlotInfo
	// This query employs a somewhat ugly hack in which row_to_json(), JSON indexing, and the
	// COALESCE() function are combined to implement a "select column if it exists" behavior.
	// This is necessary because the 'wal_status' column was only added in Postgres 13, and
	// while we definitely want it if it's available we also need to support older versions
	// where the column doesn't exist.
	var query = `SELECT slot_name, database, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn, coalesce(row_to_json(s)->>'wal_status'::text, 'unknown') as wal_status FROM pg_catalog.pg_replication_slots s WHERE slot_name = $1`
	if err := conn.QueryRow(ctx, query, slotName).Scan(&info.SlotName, &info.Database, &info.Plugin, &info.SlotType, &info.Active, &info.RestartLSN, &info.ConfirmedFlushLSN, &info.WALStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("error querying replication slots: %w", err)
	}
	return &info, nil
}

// listPublishedTables returns a map from stream IDs to whether they are present in the specified publication.
func listPublishedTables(ctx context.Context, conn *pgxpool.Pool, publicationName string) (map[sqlcapture.StreamID]bool, error) {
	var rows, err = conn.Query(ctx, `SELECT schemaname, tablename FROM pg_catalog.pg_publication_tables WHERE pubname = $1`, publicationName)
	if err != nil {
		return nil, fmt.Errorf("error listing tables in publication %q: %w", publicationName, err)
	}
	defer rows.Close()

	var publicationStatus = make(map[sqlcapture.StreamID]bool)
	for rows.Next() {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return nil, fmt.Errorf("error listing tables in publication %q: %w", publicationName, err)
		}
		publicationStatus[sqlcapture.JoinStreamID(schema, table)] = true
	}
	return publicationStatus, nil
}

// recreateReplicationSlot attempts to drop and then recreate a replication slot with the specified name.
func recreateReplicationSlot(ctx context.Context, conn *pgxpool.Pool, slotName string) error {
	var logEntry = logrus.WithField("slot", slotName)
	logEntry.Info("attempting to drop replication slot")
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SELECT pg_drop_replication_slot('%s');`, slotName)); err != nil {
		// Not a fatal error because we don't want a failure to drop a nonexistent slot
		// to prevent the subsequent attempt to create it.
		logEntry.WithField("err", err).Debug("failed to drop replication slot")
	}
	logEntry.Info("attempting to create replication slot")
	if _, err := conn.Exec(ctx, fmt.Sprintf(`SELECT pg_create_logical_replication_slot('%s', 'pgoutput');`, slotName)); err != nil {
		return fmt.Errorf("replication slot %q couldn't be created", slotName)
	}
	logEntry.Info("created replication slot")
	return nil
}

// queryLatestServerLSN returns the latest server WAL LSN.
func queryLatestServerLSN(ctx context.Context, conn *pgxpool.Pool) (pglogrepl.LSN, error) {
	// When querying a read-only standby server we use 'pg_last_wal_replay_lsn()'
	// and otherwise for normal standalone servers we use 'pg_current_wal_flush_lsn()'.
	var query = `
	  SELECT CASE
	    WHEN pg_is_in_recovery()
		THEN pg_last_wal_replay_lsn()
		ELSE pg_current_wal_flush_lsn()
	  END;
	`
	var currentLSN pglogrepl.LSN
	if err := conn.QueryRow(ctx, query).Scan(&currentLSN); err != nil {
		return 0, fmt.Errorf("error querying current WAL LSN: %w", err)
	}
	return currentLSN, nil
}
