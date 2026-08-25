package main

import (
	"context"

	"github.com/sirupsen/logrus"
)

// ReplicationDiagnostics is called when replication appears not to be progressing. For CockroachDB
// the most useful things to surface are the current cluster timestamp (to compare against the
// connector's cursor) and the state of any changefeed jobs the connector might be running.
func (db *cockroachdbDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var hlc string
	if err := db.conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&hlc); err != nil {
		logrus.WithField("err", err).Warn("diagnostics: unable to query cluster logical timestamp")
	} else {
		logrus.WithField("clusterTimestamp", hlc).Info("replication diagnostics: current cluster timestamp")
	}

	var rows, err = db.conn.Query(ctx, "SELECT job_id, status, running_status FROM [SHOW CHANGEFEED JOBS] WHERE status = 'running' LIMIT 20")
	if err != nil {
		logrus.WithField("err", err).Debug("diagnostics: unable to list changefeed jobs")
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var jobID int64
		var status, runningStatus *string
		if err := rows.Scan(&jobID, &status, &runningStatus); err != nil {
			return nil
		}
		logrus.WithFields(logrus.Fields{
			"jobID":         jobID,
			"status":        derefString(status),
			"runningStatus": derefString(runningStatus),
		}).Info("replication diagnostics: changefeed job")
	}
	return nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
