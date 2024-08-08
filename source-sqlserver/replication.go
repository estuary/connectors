package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// TODO(wgd): Contemplate https://github.com/debezium/debezium/blob/main/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerStreamingChangeEventSource.java#L51
// and the schema upgrade process described there.

var replicationBufferSize = 4 * 1024 // Assuming change events average ~2kB then 4k * 2kB = 8MB

const (
	cdcPollingWorkers  = 4                      // Number of parallel worker threads to execute CDC polling operations
	cdcCleanupWorkers  = 16                     // Number of parallel worker threads to execute table cleanup operations
	cdcPollingInterval = 500 * time.Millisecond // How frequently to perform CDC polling
	cdcCleanupInterval = 15 * time.Second       // How frequently to perform CDC table cleanup
)

// LSN is just a type alias for []byte to make the code that works with LSN values a bit clearer.
type LSN = []byte

// sqlserverSourceInfo is source metadata for data capture events.
type sqlserverSourceInfo struct {
	sqlcapture.SourceCommon

	LSN        LSN    `json:"lsn" jsonschema:"description=The LSN at which a CDC event occurred. Only set for CDC events, not backfills."`
	SeqVal     []byte `json:"seqval" jsonschema:"description=Sequence value used to order changes to a row within a transaction. Only set for CDC events, not backfills."`
	UpdateMask any    `json:"updateMask,omitempty" jsonschema:"description=A bit mask with a bit corresponding to each captured column identified for the capture instance. Only set for CDC events, not backfills."`
}

func (si *sqlserverSourceInfo) Common() sqlcapture.SourceCommon {
	return si.SourceCommon
}

// ReplicationStream constructs a new ReplicationStream object, from which
// a neverending sequence of change events can be read.
func (db *sqlserverDatabase) ReplicationStream(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
	var stream = &sqlserverReplicationStream{db: db, conn: db.conn, cfg: db.config}
	if err := stream.open(ctx); err != nil {
		return nil, fmt.Errorf("error opening replication stream: %w", err)
	}

	if startCursor == "" {
		var maxLSN, err = cdcGetMaxLSN(ctx, db.conn)
		if err != nil {
			return nil, err
		}
		stream.fromLSN = maxLSN
	} else {
		var resumeLSN, err = base64.StdEncoding.DecodeString(startCursor)
		if err != nil {
			return nil, fmt.Errorf("error decoding resume cursor: %w", err)
		}
		stream.fromLSN = resumeLSN
	}

	return stream, nil
}

type sqlserverReplicationStream struct {
	db   *sqlserverDatabase
	conn *sql.DB
	cfg  *Config

	cancel context.CancelFunc            // Cancel function for the replication goroutine's context
	errCh  chan error                    // Error channel for the final exit status of the replication goroutine
	events chan sqlcapture.DatabaseEvent // Change event channel from the replication goroutine to the main thread

	fromLSN LSN // The LSN from which we will request changes on the next polling cycle

	tables struct {
		sync.RWMutex
		info map[string]*tableReplicationInfo
	}

	cleanup struct {
		sync.RWMutex
		ackLSN LSN // The latest LSN to be acknowledged by Flow
	}
}

type tableReplicationInfo struct {
	CaptureInstance string
	KeyColumns      []string
	ColumnTypes     map[string]any
}

func (rs *sqlserverReplicationStream) open(ctx context.Context) error {
	var (
		dbName     string
		cdcEnabled bool
	)
	const query = `SELECT name, is_cdc_enabled FROM sys.databases WHERE name = db_name()`
	if err := rs.conn.QueryRowContext(ctx, query).Scan(&dbName, &cdcEnabled); err != nil {
		return fmt.Errorf("error querying cdc state: %w", err)
	}
	if !cdcEnabled {
		return fmt.Errorf("CDC is not enabled on database %q", dbName)
	}

	rs.errCh = make(chan error)
	rs.events = make(chan sqlcapture.DatabaseEvent)
	rs.tables.info = make(map[string]*tableReplicationInfo)
	return nil
}

func (rs *sqlserverReplicationStream) ActivateTable(ctx context.Context, streamID string, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	log.WithField("table", streamID).Trace("activate table")

	// TODO(wgd): It's rather inefficient to redo the 'list instances' work every time a
	// table gets activated.
	var captureInstances, err = listCaptureInstances(ctx, rs.conn)
	if err != nil {
		return err
	}

	var instanceNames = captureInstances[streamID]
	var instanceName string
	if len(instanceNames) > 0 {
		// TODO(wgd): We should support using whichever capture instance is newer, here
		// to enable schema migrations in the recommended SQL Server way.
		instanceName = instanceNames[0]
	} else {
		return fmt.Errorf("no capture instance for table %q", streamID)
	}

	var columnTypes = make(map[string]any)
	for columnName, columnInfo := range discovery.Columns {
		columnTypes[columnName] = columnInfo.DataType
	}

	rs.tables.Lock()
	rs.tables.info[streamID] = &tableReplicationInfo{
		CaptureInstance: instanceName,
		KeyColumns:      keyColumns,
		ColumnTypes:     columnTypes,
	}
	rs.tables.Unlock()
	log.WithFields(log.Fields{"stream": streamID, "instance": instanceName}).Debug("activated table")
	return nil
}

func (rs *sqlserverReplicationStream) StartReplication(ctx context.Context) error {
	var streamCtx, streamCancel = context.WithCancel(ctx)
	rs.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	rs.errCh = make(chan error)
	rs.cancel = streamCancel

	go func() {
		var err = rs.run(streamCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		close(rs.events)
		rs.errCh <- err
	}()
	return nil
}

func (rs *sqlserverReplicationStream) Events() <-chan sqlcapture.DatabaseEvent {
	return rs.events
}

func (rs *sqlserverReplicationStream) Acknowledge(ctx context.Context, cursor string) error {
	log.WithField("cursor", cursor).Debug("acknowledged up to cursor")
	if cursor == "" {
		return nil
	}

	var cursorLSN, err = base64.StdEncoding.DecodeString(cursor)
	if err != nil {
		return fmt.Errorf("error decoding cursor %q: %w", cursor, err)
	}

	rs.cleanup.Lock()
	rs.cleanup.ackLSN = cursorLSN
	rs.cleanup.Unlock()
	return nil
}

func (rs *sqlserverReplicationStream) Close(ctx context.Context) error {
	log.Debug("replication stream close requested")
	rs.cancel()
	return <-rs.errCh
}

func (rs *sqlserverReplicationStream) run(ctx context.Context) error {
	var poll = time.NewTicker(cdcPollingInterval)
	var cleanup = time.NewTicker(cdcCleanupInterval)
	defer poll.Stop()
	defer cleanup.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			if err := rs.pollChanges(ctx); err != nil {
				log.WithField("err", err).Error("error polling change tables")
				return fmt.Errorf("error requesting CDC events: %w", err)
			}
		case <-cleanup.C:
			if err := rs.cleanupChangeTables(ctx); err != nil {
				log.WithField("err", err).Error("error cleaning up change tables")
				return fmt.Errorf("error cleaning up change tables: %w", err)
			}
		}
	}
}

func (rs *sqlserverReplicationStream) pollChanges(ctx context.Context) error {
	var toLSN, err = cdcGetMaxLSN(ctx, rs.conn)
	if err != nil {
		return err
	}

	if bytes.Equal(rs.fromLSN, toLSN) {
		log.Trace("server lsn hasn't advanced, not polling any tables")
		return nil
	}

	// Make a list of capture instances that we might want to poll, and then query the
	// database to determine the most recent change LSN for each capture instance.
	var captureInstances []string
	rs.tables.RLock()
	for _, info := range rs.tables.info {
		captureInstances = append(captureInstances, info.CaptureInstance)
	}
	captureInstanceMaxLSNs, err := cdcGetInstanceMaxLSNs(ctx, rs.conn, captureInstances)
	if err != nil {
		return fmt.Errorf("failed to query maximum LSNs for capture instances: %w", err)
	}

	// Put together a work queue of CDC polling operations to perform
	var queue []*tablePollInfo
	for streamID, info := range rs.tables.info {
		// Skip polling a particular capture instance if its maximum LSN is less than or
		// equal to our 'fromLSN' value for this polling operation, as this means that it
		// doesn't have any new changes for us.
		//
		// As documented on the `cdcGetInstanceMaxLSNs` function, a completely empty change
		// table will have a nil value in the max LSNs map, and the nil byte string compares
		// lexicographically before any other value, so the comparison here also covers the
		// case of a CDC instance which doesn't need to be polled because it has no changes
		// at all.
		var instanceMaxLSN, ok = captureInstanceMaxLSNs[info.CaptureInstance]
		if ok && bytes.Compare(instanceMaxLSN, rs.fromLSN) <= 0 {
			log.WithFields(log.Fields{
				"instance":       info.CaptureInstance,
				"instanceMaxLSN": instanceMaxLSN,
				"pollFromLSN":    rs.fromLSN,
			}).Trace("skipping poll for unchanged capture instance")
			continue
		}

		var schemaName, tableName = splitStreamID(streamID)
		queue = append(queue, &tablePollInfo{
			StreamID:     streamID,
			SchemaName:   schemaName,
			TableName:    tableName,
			InstanceName: info.CaptureInstance,
			KeyColumns:   info.KeyColumns,
			ColumnTypes:  info.ColumnTypes,
			FromLSN:      rs.fromLSN,
			ToLSN:        toLSN,
		})
	}
	rs.tables.RUnlock()

	// Execute all polling operations in the work queue
	var workers, workerContext = errgroup.WithContext(ctx)
	workers.SetLimit(cdcPollingWorkers)
	for _, item := range queue {
		var item = item // Copy to avoid loop+closure issues
		workers.Go(func() error {
			if err := rs.pollTable(workerContext, item); err != nil {
				return fmt.Errorf("error polling changes for table %q: %w", item.StreamID, err)
			}
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}

	var cursor = base64.StdEncoding.EncodeToString(toLSN)
	log.WithField("cursor", cursor).Debug("checkpoint at cursor")
	rs.events <- &sqlcapture.FlushEvent{
		Cursor: cursor,
	}
	rs.fromLSN = toLSN

	return nil
}

func (rs *sqlserverReplicationStream) cleanupChangeTables(ctx context.Context) error {
	// Query the start LSN of every capture instance
	var instanceMinLSNs, err = cdcGetInstanceMinLSNs(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error querying capture instance start LSNs: %w", err)
	}

	// Figure out what the latest acknowledged LSN is
	rs.cleanup.RLock()
	var ackedLSN = rs.cleanup.ackLSN
	rs.cleanup.RUnlock()

	// Make a list of capture instances which can be usefully cleaned up
	var cleanupInstances []string
	rs.tables.RLock()
	for _, info := range rs.tables.info {
		var startLSN, ok = instanceMinLSNs[info.CaptureInstance]
		if ok && bytes.Compare(startLSN, ackedLSN) < 0 {
			cleanupInstances = append(cleanupInstances, info.CaptureInstance)
		}
	}
	rs.tables.RUnlock()

	// Execute cleanup operations in parallel
	var workers, workerContext = errgroup.WithContext(ctx)
	workers.SetLimit(cdcCleanupWorkers)
	for _, instanceName := range cleanupInstances {
		var instanceName = instanceName // Copy to avoid loop+closure issues
		workers.Go(func() error {
			if err := cdcCleanupChangeTable(workerContext, rs.conn, instanceName, ackedLSN); err != nil {
				return fmt.Errorf("error cleaning change table %q: %w", instanceName, err)
			}
			return nil
		})
	}
	if err := workers.Wait(); err != nil {
		return err
	}
	return nil
}

type tablePollInfo struct {
	StreamID     string
	SchemaName   string
	TableName    string
	InstanceName string
	KeyColumns   []string
	ColumnTypes  map[string]any
	FromLSN      LSN
	ToLSN        LSN
}

func (rs *sqlserverReplicationStream) pollTable(ctx context.Context, info *tablePollInfo) error {
	log.WithFields(log.Fields{
		"stream":   info.StreamID,
		"instance": info.InstanceName,
		"fromLSN":  info.FromLSN,
		"toLSN":    info.ToLSN,
	}).Trace("polling stream")

	var query = fmt.Sprintf(`SELECT * FROM cdc.fn_cdc_get_all_changes_%s(@p1, @p2, N'all');`, info.InstanceName)
	rows, err := rs.conn.QueryContext(ctx, query, info.FromLSN, info.ToLSN)
	if err != nil {
		return fmt.Errorf("error requesting changes: %w", err)
	}
	defer rows.Close()

	cnames, err := rows.Columns()
	if err != nil {
		return err
	}

	var vals = make([]any, len(cnames))
	var vptrs = make([]any, len(vals))
	for idx := range vals {
		vptrs[idx] = &vals[idx]
	}

	for rows.Next() {
		if err := rows.Scan(vptrs...); err != nil {
			return fmt.Errorf("error scanning result row: %w", err)
		}
		var fields = make(map[string]interface{})
		for idx, name := range cnames {
			fields[name] = vals[idx]
		}

		log.WithFields(log.Fields{"stream": info.StreamID, "data": fields}).Trace("got change")

		// Extract the event's LSN and remove it from the record fields, since
		// it will be included in the source metadata already.
		var changeLSN, ok = fields["__$start_lsn"].(LSN)
		if !ok || changeLSN == nil {
			return fmt.Errorf("invalid '__$start_lsn' value (capture user may not have correct permissions): %v", changeLSN)
		}
		if bytes.Compare(changeLSN, info.FromLSN) <= 0 {
			log.WithFields(log.Fields{
				"stream":    info.StreamID,
				"changeLSN": changeLSN,
				"fromLSN":   info.FromLSN,
			}).Trace("filtered change <= fromLSN")
			continue
		}
		delete(fields, "__$start_lsn")

		// Extract the change type (Insert/Update/Delete) and remove it from the record data.
		var operation sqlcapture.ChangeOp
		opval, ok := fields["__$operation"]
		if !ok {
			return fmt.Errorf("internal error: cdc operation unspecified ('__$operation' unset)")
		}
		opcode, ok := opval.(int64)
		if !ok {
			return fmt.Errorf("internal error: cdc operation code %T(%#v) isn't an int64", opval, opval)
		}
		switch opcode {
		case 1:
			operation = sqlcapture.DeleteOp
		case 2:
			operation = sqlcapture.InsertOp
		case 4:
			operation = sqlcapture.UpdateOp
		default:
			return fmt.Errorf("invalid change operation: %d", opcode)
		}
		delete(fields, "__$operation")

		// Move the '__$seqval' and '__$update_mask' columns into metadata as well.
		seqval, ok := fields["__$seqval"].(LSN)
		if !ok || seqval == nil {
			return fmt.Errorf("invalid '__$seqval' value: %v", seqval)
		}
		delete(fields, "__$seqval")

		var updateMask = fields["__$update_mask"]
		delete(fields, "__$update_mask")

		var rowKey, err = sqlcapture.EncodeRowKey(info.KeyColumns, fields, info.ColumnTypes, encodeKeyFDB)
		if err != nil {
			return fmt.Errorf("error encoding stream %q row key: %w", info.StreamID, err)
		}
		if err := rs.db.translateRecordFields(info.ColumnTypes, fields); err != nil {
			return fmt.Errorf("error translating stream %q change event: %w", info.StreamID, err)
		}

		// For inserts and updates the change event records the state after the
		// change, and for deletions the change event (partially) records the state
		// before the change.
		var before, after map[string]any
		if operation == sqlcapture.InsertOp || operation == sqlcapture.UpdateOp {
			after = fields
		} else {
			before = fields
		}

		rs.events <- &sqlcapture.ChangeEvent{
			Operation: operation,
			RowKey:    rowKey,
			Source: &sqlserverSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Schema: info.SchemaName,
					Table:  info.TableName,
				},
				LSN:        changeLSN,
				SeqVal:     seqval,
				UpdateMask: updateMask,
			},
			Before: before,
			After:  after,
		}
	}

	return nil
}

func cdcGetMinLSN(ctx context.Context, conn *sql.DB, instanceName string) (LSN, error) {
	var minLSN LSN
	const query = `SELECT sys.fn_cdc_get_min_lsn(@p1);`
	if err := conn.QueryRowContext(ctx, query, instanceName).Scan(&minLSN); err != nil {
		return nil, fmt.Errorf("error querying minimum LSN for %q: %w", instanceName, err)
	}
	return minLSN, nil
}

func cdcGetMaxLSN(ctx context.Context, conn *sql.DB) (LSN, error) {
	var maxLSN LSN
	const query = `SELECT sys.fn_cdc_get_max_lsn();`
	if err := conn.QueryRowContext(ctx, query).Scan(&maxLSN); err != nil {
		return nil, fmt.Errorf("error querying database maximum LSN: %w", err)
	}
	if len(maxLSN) == 0 {
		return nil, fmt.Errorf("invalid result from 'sys.fn_cdc_get_max_lsn()', agent process likely not running")
	}
	return maxLSN, nil
}

// cdcGetInstanceMaxLSNs queries the maximum change event LSN for each listed capture instance.
//
// It does this by generating a hairy union query which simply selects `MAX(__$start_lsn)` for
// the underlying change table of each instance and combines the results. I have verified that
// this specific query executes as a cheap backwards index scan taking the first result for each
// change table, so this should be reasonably efficient.
//
// In the event that a capture instance contains no changes at all, the results map will contain
// a nil value for that instance LSN.
//
// The generated query has contains a distinct `SELECT` statement for every named capture instance,
// but I have verified in practice that this works for querying at least 1k capture instances at a
// time. If we ever hit a scaling limit here we can implement chunking, but so far that doesn't
// appear to be necessary.
func cdcGetInstanceMaxLSNs(ctx context.Context, conn *sql.DB, instanceNames []string) (map[string]LSN, error) {
	var subqueries []string
	for idx, instanceName := range instanceNames {
		// TODO(wgd): It would probably be a good idea to properly quote the CDC table name instead of just splatting
		// the instance name into the query string. But currently we don't do that for backfills or for the actual
		// replication polling query, so it would be premature to worry about it only here.
		var subquery = fmt.Sprintf("SELECT %d AS idx, MAX(__$start_lsn) AS lsn FROM [cdc].[%s_CT]", idx, instanceName)
		subqueries = append(subqueries, subquery)
	}
	var query = strings.Join(subqueries, " UNION ALL ")

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	var results = make(map[string]LSN)
	for rows.Next() {
		var index int
		var instanceMaxLSN LSN
		if err := rows.Scan(&index, &instanceMaxLSN); err != nil {
			return nil, fmt.Errorf("error scanning result: %w", err)
		}
		if index < 0 || index >= len(instanceNames) {
			return nil, fmt.Errorf("internal error: result index %d out of range for instance list %q", index, len(instanceNames))
		}
		var instanceName = instanceNames[index]
		log.WithFields(log.Fields{
			"instance": instanceName,
			"lsn":      instanceMaxLSN,
		}).Trace("queried max LSN for capture instance")
		results[instanceName] = instanceMaxLSN
	}
	return results, rows.Err()
}

// cdcGetInstanceMinLSNs queries the minimum change event LSN for all capture instances.
func cdcGetInstanceMinLSNs(ctx context.Context, conn *sql.DB) (map[string]LSN, error) {
	var query = `SELECT capture_instance, start_lsn FROM cdc.change_tables;`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}
	defer rows.Close()

	var results = make(map[string]LSN)
	for rows.Next() {
		var instanceName string
		var startLSN LSN
		if err := rows.Scan(&instanceName, &startLSN); err != nil {
			return nil, fmt.Errorf("error scanning result: %w", err)
		}
		log.WithFields(log.Fields{
			"instance": instanceName,
			"lsn":      startLSN,
		}).Trace("queried start LSN for capture instance")
		results[instanceName] = startLSN
	}
	return results, rows.Err()
}

func cdcCleanupChangeTable(ctx context.Context, conn *sql.DB, instanceName string, belowLSN LSN) error {
	log.WithFields(log.Fields{
		"instance": instanceName,
		"lsn":      fmt.Sprintf("%X", belowLSN),
	}).Debug("cleaning change table")

	var query = new(strings.Builder)
	fmt.Fprintf(query, "BEGIN DECLARE @retcode AS INT = 0; ")
	fmt.Fprintf(query, "EXEC @retcode = sys.sp_cdc_cleanup_change_table @capture_instance = @p1, @low_water_mark = 0x%X; ", belowLSN)
	fmt.Fprintf(query, "SELECT @retcode; END")

	var retcode int
	if err := conn.QueryRowContext(ctx, query.String(), instanceName).Scan(&retcode); err != nil {
		return fmt.Errorf("error executing CDC cleanup on instance %q below LSN %X: %w", instanceName, belowLSN, err)
	}
	if retcode != 0 {
		return fmt.Errorf("error executing CDC cleanup on instance %q below LSN %X: return code %d", instanceName, belowLSN, retcode)
	}
	return nil
}

func splitStreamID(streamID string) (string, string) {
	var bits = strings.SplitN(streamID, ".", 2)
	return bits[0], bits[1]
}

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
		if numResults == 0 {
			logEntry.Info("no results")
		}
	}

	query("SELECT * FROM sys.dm_server_services;")
	query("SELECT * FROM sys.dm_cdc_log_scan_sessions;")
	query("EXEC msdb.dbo.sp_help_job;")
	query("EXEC sys.sp_cdc_help_jobs;")
	query("SELECT * FROM msdb.dbo.cdc_jobs;")
	query("EXEC sys.sp_cdc_help_change_data_capture;")
	return nil
}
