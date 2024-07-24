package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

type replicationSlotInfo struct {
	SlotName          string
	Database          string
	Plugin            string
	SlotType          string
	RestartLSN        pglogrepl.LSN
	ConfirmedFlushLSN pglogrepl.LSN
	WALStatus         string
}

// queryReplicationSlotInfo returns information about the named replication slot, if it exists.
// If the slot doesn't exist then a nil pointer is returned, but without an error. An error is
// only returned if the query itself fails.
func queryReplicationSlotInfo(ctx context.Context, conn *pgx.Conn, slotName string) (*replicationSlotInfo, error) {
	var info replicationSlotInfo
	// This query employs a somewhat ugly hack in which row_to_json(), JSON indexing, and the
	// COALESCE() function are combined to implement a "select column if it exists" behavior.
	// This is necessary because the 'wal_status' column was only added in Postgres 13, and
	// while we definitely want it if it's available we also need to support older versions
	// where the column doesn't exist.
	var query = `SELECT slot_name, database, plugin, slot_type, restart_lsn, confirmed_flush_lsn, coalesce(row_to_json(s)->>'wal_status'::text, 'unknown') as wal_status FROM pg_catalog.pg_replication_slots s WHERE slot_name = $1`
	if err := conn.QueryRow(ctx, query, slotName).Scan(&info.SlotName, &info.Database, &info.Plugin, &info.SlotType, &info.RestartLSN, &info.ConfirmedFlushLSN, &info.WALStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("error querying replication slots: %w", err)
	}
	return &info, nil
}
