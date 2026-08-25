package main

import (
	"context"

	"github.com/sirupsen/logrus"
)

// ReplicationDiagnostics is called when replication appears not to be progressing. For CockroachDB
// the most useful things to surface are the current cluster timestamp (to compare against the
// connector's cursor) and the state of any changefeed jobs the connector might be running.
func (db *cockroachdbDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		logrus.WithField("query", q).Info("running diagnostics query")
		var result, err = db.conn.Query(ctx, q)
		if err != nil {
			logrus.WithFields(logrus.Fields{
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
				logrus.WithField("err", err).Error("unable to process result row")
				continue
			}

			var logFields = logrus.Fields{}
			for idx, val := range row {
				logFields[string(keys[idx].Name)] = val
			}
			logrus.WithFields(logFields).Info("got diagnostic row")
		}
		if numResults == 0 {
			logrus.WithField("query", q).Info("no results")
		}
	}

	query("SELECT cluster_logical_timestamp();")
	query("SELECT job_id, status, running_status FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' LIMIT 20;")
	return nil
}

// PeriodicChecks has nothing to do for CockroachDB. Unlike PostgreSQL there's no replication slot
// whose disk usage we need to keep an eye on.
func (db *cockroachdbDatabase) PeriodicChecks(ctx context.Context) error {
	return nil
}
