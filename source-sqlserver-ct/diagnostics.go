package main

import (
	"context"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

func (db *sqlserverDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		var logEntry = log.WithField("query", q)
		logEntry.Info("running diagnostics query")

		var rows, err = db.conn.QueryContext(ctx, q)
		if err != nil {
			logEntry.WithField("err", err).Error("unable to execute diagnostics query")
			return
		}
		defer rows.Close()

		cnames, err := rows.Columns()
		if err != nil {
			logEntry.WithField("err", err).Error("error processing query result")
			return
		}
		var vals = make([]any, len(cnames))
		var vptrs = make([]any, len(vals))
		for idx := range vals {
			vptrs[idx] = &vals[idx]
		}

		var numResults int
		for rows.Next() {
			numResults++
			if err := rows.Scan(vptrs...); err != nil {
				logEntry.WithField("err", err).Error("error scanning result row")
				continue
			}
			var logFields = log.Fields{}
			for idx, name := range cnames {
				logFields[name] = vals[idx]
			}
			log.WithFields(logFields).Info("got diagnostic row")
		}
		if err := rows.Err(); err != nil {
			logEntry.WithField("err", err).Error("error iterating diagnostic rows")
		}
		if numResults == 0 {
			logEntry.Info("no results")
		}
	}

	query("SELECT * FROM sys.dm_server_services;")
	query("SELECT * FROM sys.change_tracking_databases;")
	query("SELECT * FROM sys.change_tracking_tables;")
	return nil
}

func (db *sqlserverDatabase) PeriodicChecks(ctx context.Context) error {
	// Intentionally empty. This method is part of the sqlcapture.Database interface
	// and is called periodically during capture, but Change Tracking does not require
	// any periodic maintenance or status checks.
	return nil
}

// logBackfillQuery logs the backfill query for a table the first time it is scanned during a
// connector invocation.
//
// In other connectors the analogous logic runs an EXPLAIN, and we were going to do the same
// thing here with `SET SHOWPLAN`, but this was such an absolute pain that we gave up. Turns
// out that query parameters via go-mssqldb don't play nicely with SHOWPLAN. Also SHOWPLAN
// requires a special permission which we likely wouldn't have anyway, so it just wasn't
// worth implementing a workaround at this time.
func (db *sqlserverDatabase) logBackfillQuery(streamID sqlcapture.StreamID, query string, args []any) {
	// Only log the backfill query once per table per connector invocation.
	if db.loggedBackfill == nil {
		db.loggedBackfill = make(map[sqlcapture.StreamID]struct{})
	}
	if _, ok := db.loggedBackfill[streamID]; ok {
		return
	}
	db.loggedBackfill[streamID] = struct{}{}

	log.WithFields(log.Fields{"id": streamID, "query": query, "args": args}).Info("backfill query")
}
