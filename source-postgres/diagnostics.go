package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

func (db *postgresDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		log.WithField("query", q).Info("running diagnostics query")
		var result, err = db.conn.Query(ctx, q)
		if err != nil {
			log.WithFields(log.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
			return
		}
		defer result.Close()

		var numResults int
		var keys = result.FieldDescriptions()
		for result.Next() {
			numResults++
			var row, err = result.Values()
			if err != nil {
				log.WithField("err", err).Error("unable to process result row")
				continue
			}

			var logFields = log.Fields{}
			for idx, val := range row {
				logFields[string(keys[idx].Name)] = val
			}
			log.WithFields(logFields).Info("got diagnostic row")
		}
		if numResults == 0 {
			log.WithField("query", q).Info("no results")
		}
	}

	if !db.config.Advanced.ReadOnlyCapture {
		query("SELECT * FROM " + db.WatermarksTable().String() + ";")
	}
	query("SELECT * FROM pg_replication_slots;")
	query("SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() ELSE pg_current_wal_flush_lsn() END;")
	return nil
}

func (db *postgresDatabase) PeriodicChecks(ctx context.Context) error {
	// Logging the current XID allows us to reliably map a timestamp to
	// an approximate database XID using the task logs.
	queryAndLogCurrentXID(ctx, db.conn)

	// Query the replication slot information and warn if there's a significant gap
	// between confirmed_flush_lsn and restart_lsn.
	var logSlot = log.WithField("slot", db.config.Advanced.SlotName)
	if slotInfo, err := queryReplicationSlotInfo(ctx, db.conn, db.config.Advanced.SlotName); err != nil {
		logSlot.WithError(err).Warn("error checking replication slot info")
	} else if slotInfo == nil {
		logSlot.Warn("missing replication slot info")
	} else if slotInfo.ConfirmedFlushLSN == nil {
		logSlot.Warn("replication slot has no confirmed_flush_lsn (and is likely still being created but blocked on a long-running transaction)")
	} else if slotInfo.RestartLSN == nil {
		logSlot.Warn("replication slot has no restart_lsn (this should be impossible)")
	} else if confirmationGap := *slotInfo.ConfirmedFlushLSN - *slotInfo.RestartLSN; confirmationGap > 0x100000000 {
		logSlot.WithField("gap", confirmationGap).Warn("replication slot restart_lsn is lagging far behind confirmed_flush_lsn (probable long-running transaction)")
	}

	return nil
}

func queryAndLogCurrentXID(ctx context.Context, conn *pgxpool.Pool) {
	var xid uint64
	const query = "SELECT txid_snapshot_xmin(txid_current_snapshot())"
	if err := conn.QueryRow(ctx, query).Scan(&xid); err == nil {
		log.WithField("xid", xid).Info("current transaction ID")
	} else {
		log.WithError(err).Warn("error querying current transaction ID")
	}
}
