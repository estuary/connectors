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
)

// TODO(wgd): Contemplate https://github.com/debezium/debezium/blob/main/debezium-connector-sqlserver/src/main/java/io/debezium/connector/sqlserver/SqlServerStreamingChangeEventSource.java#L51
// and the schema upgrade process described there.

var replicationBufferSize = 4 * 1024 // Assuming change events average ~2kB then 4k * 2kB = 8MB

// sqlserverSourceInfo is source metadata for data capture events.
type sqlserverSourceInfo struct {
	sqlcapture.SourceCommon

	LSN        []byte `json:"lsn" jsonschema:"description=The LSN at which a CDC event occurred. Only set for CDC events, not backfills."`
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

	fromLSN []byte // The LSN from which we will request changes on the next polling cycle

	tables struct {
		sync.RWMutex
		info map[string]*tableReplicationInfo
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
	return nil // Nothing to do here, SQL Server CDC retention is primarily time-based
}

func (rs *sqlserverReplicationStream) Close(ctx context.Context) error {
	log.Debug("replication stream close requested")
	rs.cancel()
	return <-rs.errCh
}

func (rs *sqlserverReplicationStream) run(ctx context.Context) error {
	// TODO(wgd): Make the polling interval part of advanced connector config.
	// A polling interval of 500ms is the default value that Debezium SQL Server uses.
	const pollInterval = 500 * time.Millisecond
	var poll = time.NewTicker(pollInterval)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			if err := rs.pollChanges(ctx); err != nil {
				log.WithField("err", err).Error("error polling change tables")
				return fmt.Errorf("error requesting CDC events: %w", err)
			}
		}
	}
}

func (rs *sqlserverReplicationStream) pollChanges(ctx context.Context) error {
	// TODO(wgd): Make the number of transactions per polling interval configurable.
	var pollTransactionsLimit = 1024
	var toLSN []byte
	if pollTransactionsLimit == 0 {
		var maxLSN, err = cdcGetMaxLSN(ctx, rs.conn)
		if err != nil {
			return err
		}
		toLSN = maxLSN
	} else {
		var nextLSN, err = cdcGetNextLSN(ctx, rs.conn, rs.fromLSN, pollTransactionsLimit)
		if err != nil {
			return err
		}
		toLSN = nextLSN
	}

	if bytes.Equal(rs.fromLSN, toLSN) {
		log.Trace("lsn hasn't advanced, not polling tables")
		return nil
	}

	// Make a list of tables we intend to poll. The copying is necessary
	// to avoid holding the table-info lock for too long.
	var queue []*tablePollInfo
	rs.tables.RLock()
	for streamID, info := range rs.tables.info {
		var schemaName, tableName = splitStreamID(streamID)
		queue = append(queue, &tablePollInfo{
			StreamID:     streamID,
			SchemaName:   schemaName,
			TableName:    tableName,
			InstanceName: info.CaptureInstance,
			KeyColumns:   info.KeyColumns,
			ColumnTypes:  info.ColumnTypes,
		})
	}
	rs.tables.RUnlock()

	for _, item := range queue {
		log.WithField("table", item.StreamID).Trace("polling table")
		if err := rs.pollTable(ctx, rs.fromLSN, toLSN, item); err != nil {
			return fmt.Errorf("table %q: %w", sqlcapture.JoinStreamID(item.SchemaName, item.TableName), err)
		}
	}

	log.WithField("lsn", toLSN).Trace("flushed up to LSN")
	rs.events <- &sqlcapture.FlushEvent{
		Cursor: base64.StdEncoding.EncodeToString(toLSN),
	}
	rs.fromLSN = toLSN

	return nil
}

type tablePollInfo struct {
	StreamID     string
	SchemaName   string
	TableName    string
	InstanceName string
	KeyColumns   []string
	ColumnTypes  map[string]any
}

func (rs *sqlserverReplicationStream) pollTable(ctx context.Context, fromLSN, toLSN []byte, info *tablePollInfo) error {
	log.WithFields(log.Fields{
		"stream":   info.StreamID,
		"instance": info.InstanceName,
		"fromLSN":  fromLSN,
		"toLSN":    toLSN,
	}).Trace("polling stream")

	var query = fmt.Sprintf(`SELECT * FROM cdc.fn_cdc_get_all_changes_%s(@p1, @p2, N'all');`, info.InstanceName)
	rows, err := rs.conn.QueryContext(ctx, query, fromLSN, toLSN)
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
		var changeLSN, ok = fields["__$start_lsn"].([]byte)
		if !ok || changeLSN == nil {
			return fmt.Errorf("invalid '__$start_lsn' value (capture user may not have correct permissions): %v", changeLSN)
		}
		if bytes.Compare(changeLSN, fromLSN) <= 0 {
			log.WithFields(log.Fields{
				"stream":    info.StreamID,
				"changeLSN": changeLSN,
				"fromLSN":   fromLSN,
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
		seqval, ok := fields["__$seqval"].([]byte)
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

func cdcGetMinLSN(ctx context.Context, conn *sql.DB, instanceName string) ([]byte, error) {
	var minLSN []byte
	const query = `SELECT sys.fn_cdc_get_min_lsn(@p1);`
	if err := conn.QueryRowContext(ctx, query, instanceName).Scan(&minLSN); err != nil {
		return nil, fmt.Errorf("error querying minimum LSN for %q: %w", instanceName, err)
	}
	return minLSN, nil
}

func cdcGetMaxLSN(ctx context.Context, conn *sql.DB) ([]byte, error) {
	var maxLSN []byte
	const query = `SELECT sys.fn_cdc_get_max_lsn();`
	if err := conn.QueryRowContext(ctx, query).Scan(&maxLSN); err != nil {
		return nil, fmt.Errorf("error querying database maximum LSN: %w", err)
	}
	if len(maxLSN) == 0 {
		return nil, fmt.Errorf("invalid result from 'sys.fn_cdc_get_max_lsn()', agent process likely not running")
	}
	return maxLSN, nil
}

func cdcGetNextLSN(ctx context.Context, conn *sql.DB, fromLSN []byte, pollTransactionsLimit int) ([]byte, error) {
	var nextLSN []byte
	const query = `SELECT MAX(start_lsn) FROM (
		             SELECT start_lsn FROM cdc.lsn_time_mapping
					   WHERE start_lsn >= @p2 AND tran_id <> 0
					   ORDER BY start_lsn
					   OFFSET 0 ROWS FETCH FIRST (@p1 + 1) ROWS ONLY
				   ) AS lsns;`
	if err := conn.QueryRowContext(ctx, query, pollTransactionsLimit, fromLSN).Scan(&nextLSN); err != nil {
		return nil, fmt.Errorf("error querying database for next poll-to LSN: %w", err)
	}
	if len(nextLSN) == 0 {
		// Sometimes the above query will return a null result. This is not a fatal error,
		// and will be corrected if/when more transactions get committed on the DB side.
		log.Trace("minor failure querying 'cdc.lsn_time_mapping' for the next target LSN")
		return fromLSN, nil
	}
	return nextLSN, nil
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
			log.WithFields(logFields).Info("got row")
		}
		if numResults == 0 {
			logEntry.Info("no results")
		}
	}

	query("EXEC msdb.dbo.sp_help_job;")
	return nil
}
