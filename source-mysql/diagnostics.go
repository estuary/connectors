package main

import (
	"context"

	log "github.com/sirupsen/logrus"
)

func (db *mysqlDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		log.WithField("query", q).Info("running diagnostics query")
		var result, err = db.conn.Execute(q)
		if err != nil {
			log.WithFields(log.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
			return
		}
		defer result.Close()

		if len(result.Values) == 0 {
			log.WithField("query", q).Info("no results")
		}
		for _, row := range result.Values {
			var logFields = log.Fields{}
			for idx, column := range row {
				var key = string(result.Fields[idx].Name)
				var val = column.Value()
				if bs, ok := val.([]byte); ok {
					val = string(bs)
				}
				logFields[key] = val
			}
			log.WithFields(logFields).Info("got diagnostic row")
		}
	}

	query("SELECT @@GLOBAL.log_bin;")
	query("SELECT @@GLOBAL.binlog_format;")
	query("SHOW PROCESSLIST;")
	query("SHOW BINARY LOGS;")
	var newspeakQueries = db.versionProduct == "MySQL" && ((db.versionMajor == 8 && db.versionMinor >= 4) || db.versionMajor > 8)
	if newspeakQueries {
		query("SHOW BINARY LOG STATUS;")
		query("SHOW REPLICAS;")
	} else {
		query("SHOW MASTER STATUS;")
		query("SHOW SLAVE HOSTS;")
	}
	return nil
}

func (db *mysqlDatabase) PeriodicChecks(ctx context.Context) error {
	return nil
}
