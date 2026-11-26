package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

func (rs *db2ReplicationStream) progress(ctx context.Context) (log.Fields, error) {
	rs.mu.Lock()
	var watermarkAge time.Duration
	if !rs.watermarkWritten.IsZero() {
		watermarkAge = rs.db.now().Sub(rs.watermarkWritten)
	}
	var facts = log.Fields{
		"cursor": hex.EncodeToString(rs.fromSeq), "synchpoint": hex.EncodeToString(rs.frontier),
		"watermarkAge": watermarkAge, "frontierIdle": rs.db.now().Sub(rs.frontierChanged), "watermarksState": "unknown",
	}
	rs.mu.Unlock()
	var frontier, err = rs.db.readFrontier(ctx)
	if err != nil {
		return facts, err
	}
	facts["synchpoint"] = hex.EncodeToString(frontier)
	if table := rs.tables[rs.db.WatermarksTable()]; table != nil {
		var state, err = rs.db.registrationState(ctx, table)
		if err != nil {
			return facts, err
		}
		facts["watermarksState"] = state
	}
	return facts, nil
}

func (rs *db2ReplicationStream) watermarkError(ctx context.Context) error {
	var facts, err = rs.progress(ctx)
	var failure = fmt.Errorf("ASN Capture (asncap) does not appear to be running or is not capturing %s: saved cursor=%s, global SYNCHPOINT=%s, watermark written %s ago, frontier last moved %s ago, watermarks registration STATE=%q",
		rs.db.config.Advanced.WatermarksTable, facts["cursor"], facts["synchpoint"], facts["watermarkAge"], facts["frontierIdle"], facts["watermarksState"])
	if err != nil {
		return fmt.Errorf("%w (reading replication diagnostics: %v)", failure, err)
	}
	return failure
}

func (db *db2Database) ReplicationDiagnostics(ctx context.Context) error {
	if db.replication == nil {
		return nil
	}
	var facts, err = db.replication.progress(ctx)
	log.WithFields(facts).WithError(err).Info("SQL Replication progress")
	return err
}
