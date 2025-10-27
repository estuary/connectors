package main

import (
	"context"
	"fmt"
	"time"

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
		logSlot.WithFields(log.Fields{
			"confirmed": *slotInfo.ConfirmedFlushLSN,
			"restart":   *slotInfo.RestartLSN,
			"gap":       confirmationGap,
		}).Warn("replication slot restart_lsn is lagging far behind confirmed_flush_lsn (probable long-running transaction)")
	}

	// Query for any transactions which have been open for longer than 30 minutes,
	// as these are generally going to cause problems with WAL retention.
	if longTxns, err := listLongRunningTransactions(ctx, db.conn, 30*time.Minute); err != nil {
		log.WithError(err).Debug("unable to query long-running transactions")
	} else if len(longTxns) > 0 {
		for _, txn := range longTxns {
			logFields := log.Fields{
				"pid":      txn.PID,
				"duration": txn.Duration.String(),
				"query":    txn.Query,
				"state":    txn.State,
			}
			if txn.Username != nil {
				logFields["username"] = *txn.Username
			}
			if txn.ApplicationName != nil {
				logFields["application_name"] = *txn.ApplicationName
			}
			log.WithFields(logFields).Warn("detected long-running transaction")
		}
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

type longRunningTransaction struct {
	PID             int
	Duration        time.Duration
	Username        *string
	ApplicationName *string
	Query           string
	State           string
}

const queryLongRunningTransactions = `
    SELECT pid, now() - pg_stat_activity.xact_start AS duration, usename, application_name, query, state
      FROM pg_stat_activity
      WHERE pg_stat_activity.xact_start IS NOT NULL
        AND (now() - pg_stat_activity.xact_start) > $1::interval;`

func listLongRunningTransactions(ctx context.Context, conn *pgxpool.Pool, minDuration time.Duration) ([]longRunningTransaction, error) {
	// Convert duration to PostgreSQL interval format (e.g., "1800 seconds")
	var interval = fmt.Sprintf("%d seconds", int(minDuration.Seconds()))
	var rows, err = conn.Query(ctx, queryLongRunningTransactions, interval)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []longRunningTransaction
	for rows.Next() {
		var txn longRunningTransaction
		if err := rows.Scan(&txn.PID, &txn.Duration, &txn.Username, &txn.ApplicationName, &txn.Query, &txn.State); err != nil {
			return nil, err
		}
		transactions = append(transactions, txn)
	}
	return transactions, rows.Err()
}
