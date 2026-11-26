package main

import (
	"bytes"
	"container/heap"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// CD (change-data) tables contain committed changes copied by ASN Capture.
// COMMITSEQ identifies a transaction; INTENTSEQ orders its rows. The global
// SYNCHPOINT is the frontier through which every CD table has been published.
const replicationBatchSize = 1000

func (db *db2Database) ReplicationStream(ctx context.Context, startCursor json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var cursor string
	if len(startCursor) > 0 {
		if err := json.Unmarshal(startCursor, &cursor); err != nil {
			return nil, fmt.Errorf("invalid replication cursor: %w", err)
		}
	}
	var fromSeq []byte
	var err error
	if cursor == "" {
		fromSeq, err = db.readFrontier(ctx)
	} else {
		fromSeq, err = hex.DecodeString(cursor)
	}
	if err != nil {
		return nil, fmt.Errorf("error establishing replication cursor: %w", err)
	}
	db.activeTables = make(map[sqlcapture.StreamID]*tableReplicationInfo)
	db.replication = &db2ReplicationStream{
		db: db, fromSeq: fromSeq, tables: db.activeTables,
		batchSize: replicationBatchSize, watermarkTimeout: 5 * time.Minute,
		frontier: bytes.Clone(fromSeq), frontierChanged: db.now(),
	}
	return db.replication, nil
}

type db2ReplicationStream struct {
	db               *db2Database
	fromSeq          []byte
	tables           map[sqlcapture.StreamID]*tableReplicationInfo
	batchSize        int
	watermarkTimeout time.Duration
	frontier         []byte
	frontierChanged  time.Time
	watermarkWritten time.Time
	mu               sync.Mutex // Protect progress snapshots read by concurrent diagnostics.
}

var _ sqlcapture.ReplicationStream = (*db2ReplicationStream)(nil)

type tableReplicationInfo struct {
	StreamID       sqlcapture.StreamID
	Discovery      *sqlcapture.DiscoveryInfo
	Registration   *captureRegistration
	KeyColumns     []string
	CDColumns      []string
	CatalogColumns []string
	CDProjection   string
	OmittedColumns []string
	LOBColumns     []db2LOBColumn
}

func (rs *db2ReplicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, info *sqlcapture.DiscoveryInfo, metadata json.RawMessage) error {
	if info == nil {
		return fmt.Errorf("table %s is missing from discovery", streamID)
	}
	var table, err = rs.db.inspectTable(ctx, info, keyColumns)
	if err != nil {
		return err
	}
	if len(table.OmittedColumns) > 0 && rs.tables[streamID] == nil {
		log.WithFields(log.Fields{"table": streamID, "columns": table.OmittedColumns}).Warn("source columns missing from CD table or stored as LOB/XML change flags will be omitted from replicated changes")
	}

	rs.tables[streamID] = table
	return nil
}

func (rs *db2ReplicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	var streamID = rs.db.WatermarksTable()
	var info = discovery[streamID]
	if info == nil {
		return fmt.Errorf("watermarks table %s is missing from discovery", streamID)
	}
	return rs.ActivateTable(ctx, streamID, info.PrimaryKey, info, nil)
}

// StreamToFence polls synchronously so newly activated tables start at the
// last delivered cursor, after all rows through the preceding fence were emitted.
func (rs *db2ReplicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(sqlcapture.DatabaseEvent) error) (err error) {
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = ctx.Err()
		}
	}()
	var poll = time.NewTicker(rs.db.ReplicationPollingInterval())
	defer poll.Stop()

	if fenceAfter > 0 {
		var deadline = time.NewTimer(fenceAfter)
		defer deadline.Stop()
	timed:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				break timed
			case <-poll.C:
				if err := rs.pollChanges(ctx, callback); err != nil {
					return err
				}
			}
		}
	}

	var watermark = uuid.NewString()
	if err := rs.db.WriteWatermark(ctx, watermark); err != nil {
		return err
	}
	rs.mu.Lock()
	rs.watermarkWritten = rs.db.now()
	rs.mu.Unlock()
	var observed, committed bool
	var handleEvent = func(event sqlcapture.DatabaseEvent) error {
		if change, ok := event.(*sqlcapture.OldChangeEvent); ok &&
			change.StreamID() == rs.db.WatermarksTable() && change.Operation != sqlcapture.DeleteOp &&
			change.After["SLOT"] == rs.db.name && change.After["WATERMARK"] == watermark {
			observed = true
		}
		if err := callback(event); err != nil {
			return err
		}
		if _, ok := event.(sqlcapture.CommitEvent); ok && observed {
			committed = true
		}
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			var started = rs.db.now()
			var cursor = rs.fromSeq
			if err := rs.pollChanges(ctx, handleEvent); err != nil {
				return err
			}
			if committed {
				return nil
			}
			if rs.watermarkExpired(started, bytes.Equal(cursor, rs.fromSeq)) {
				return rs.watermarkError(ctx)
			}
		}
	}
}

// A poll begun after the publication deadline must see the watermark. Let an
// earlier poll finish draining its backlog; fail sooner only if it made no
// progress and the frontier has been idle for the entire timeout.
func (rs *db2ReplicationStream) watermarkExpired(pollStarted time.Time, idle bool) bool {
	var idleSince = rs.frontierChanged
	if idleSince.Before(rs.watermarkWritten) {
		idleSince = rs.watermarkWritten
	}
	return pollStarted.Sub(rs.watermarkWritten) >= rs.watermarkTimeout ||
		(idle && rs.db.now().Sub(idleSince) >= rs.watermarkTimeout)
}

// Acknowledgements do not prune CD tables; their retention is managed by ASN Capture.
func (rs *db2ReplicationStream) Acknowledge(ctx context.Context, cursor json.RawMessage) error {
	return nil
}

func (rs *db2ReplicationStream) Close(ctx context.Context) error { return nil }

func (db *db2Database) readFrontier(ctx context.Context) ([]byte, error) {
	var frontier []byte
	var register = quoteTableName(db.config.Advanced.CaptureSchema, "IBMSNAP_REGISTER")
	if err := db.conn.QueryRowContext(ctx, "SELECT SYNCHPOINT FROM "+register+" WHERE GLOBAL_RECORD = 'Y'").Scan(&frontier); err != nil {
		return nil, fmt.Errorf("error reading global SQL Replication SYNCHPOINT from %s: %w", register, err)
	}
	return frontier, nil
}

func (rs *db2ReplicationStream) emitCommit(seq []byte, callback func(sqlcapture.DatabaseEvent) error) error {
	if err := callback(&db2CommitEvent{CommitSeq: seq}); err != nil {
		return err
	}
	rs.mu.Lock()
	rs.fromSeq = bytes.Clone(seq)
	rs.mu.Unlock()
	return nil
}

func (rs *db2ReplicationStream) pollChanges(ctx context.Context, callback func(sqlcapture.DatabaseEvent) error) error {
	if err := rs.db.PeriodicChecks(ctx); err != nil {
		return err
	}
	var frontier, err = rs.db.readFrontier(ctx)
	if err != nil {
		return err
	}
	if !bytes.Equal(frontier, rs.frontier) {
		rs.mu.Lock()
		rs.frontier, rs.frontierChanged = bytes.Clone(frontier), rs.db.now()
		rs.mu.Unlock()
	}
	if bytes.Compare(frontier, rs.fromSeq) <= 0 {
		return nil
	}

	var commit []byte
	var pair = updatePairer{stream: rs}
	var emit = func(row *db2ChangeRow) error {
		var events, err = pair.pairUpdates(row)
		if err != nil {
			return err
		}
		for _, event := range events {
			if err := callback(event); err != nil {
				return err
			}
		}
		return nil
	}
	if err := rs.mergeChanges(ctx, frontier, func(row *db2ChangeRow) error {
		if commit != nil && !bytes.Equal(commit, row.CommitSeq) {
			if err := emit(nil); err != nil {
				return err
			}
			if err := rs.emitCommit(commit, callback); err != nil {
				return err
			}
		}
		commit = row.CommitSeq
		return emit(row)
	}); err != nil {
		return err
	}
	if err := emit(nil); err != nil {
		return err
	}
	if commit != nil {
		if err := rs.emitCommit(commit, callback); err != nil {
			return err
		}
	}
	if !bytes.Equal(commit, frontier) {
		return rs.emitCommit(frontier, callback)
	}
	return nil
}

func (rs *db2ReplicationStream) mergeChanges(ctx context.Context, frontier []byte, emit func(*db2ChangeRow) error) error {
	var tables []*tableReplicationInfo
	for _, table := range rs.tables {
		tables = append(tables, table)
	}
	slices.SortFunc(tables, func(a, b *tableReplicationInfo) int { return a.StreamID.Compare(b.StreamID) })
	var queue changeHeap
	for _, table := range tables {
		var cursor = &cdTableCursor{stream: rs, table: table, fromSeq: bytes.Clone(rs.fromSeq), toSeq: frontier}
		if err := cursor.advance(ctx); err != nil {
			return err
		}
		if cursor.row != nil {
			heap.Push(&queue, cursor)
		}
	}
	for queue.Len() > 0 {
		var cursor = heap.Pop(&queue).(*cdTableCursor)
		if err := emit(cursor.row); err != nil {
			return err
		}
		if err := cursor.advance(ctx); err != nil {
			return err
		}
		if cursor.row != nil {
			heap.Push(&queue, cursor)
		}
	}
	return nil
}

type cdTableCursor struct {
	stream         *db2ReplicationStream
	table          *tableReplicationInfo
	fromSeq, toSeq []byte
	commit, intent []byte
	batch          []*db2ChangeRow
	row            *db2ChangeRow
	done           bool
}

func (c *cdTableCursor) advance(ctx context.Context) error {
	if len(c.batch) == 0 && !c.done {
		if err := c.fetch(ctx); err != nil {
			return err
		}
	}
	c.row = nil
	if len(c.batch) > 0 {
		c.row, c.batch = c.batch[0], c.batch[1:]
	}
	return nil
}

func (c *cdTableCursor) fetch(ctx context.Context) error {
	var registration = c.table.Registration
	var query = "SELECT " + c.table.CDProjection + " FROM " + quoteTableName(registration.CDSchema, registration.CDTable)
	var args []any
	if c.commit != nil {
		query += " WHERE IBMSNAP_COMMITSEQ >= ? AND IBMSNAP_COMMITSEQ <= ? AND (IBMSNAP_COMMITSEQ > ? OR IBMSNAP_INTENTSEQ > ?)"
		args = []any{c.commit, c.toSeq, c.commit, c.intent}
	} else {
		query += " WHERE IBMSNAP_COMMITSEQ > ? AND IBMSNAP_COMMITSEQ <= ?"
		args = []any{c.fromSeq, c.toSeq}
	}
	query += fmt.Sprintf(" ORDER BY IBMSNAP_COMMITSEQ, IBMSNAP_INTENTSEQ FETCH FIRST %d ROWS ONLY", c.stream.batchSize)
	var rows, err = c.stream.db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("error polling CD table for %s: %w", c.table.StreamID, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	var expected = slices.Clone(c.table.CDColumns)
	for _, column := range c.table.LOBColumns {
		expected = append(expected, column.lengthAlias())
	}
	if !slices.Equal(columns, expected) {
		return fmt.Errorf("CD table columns changed for %s; restart the capture to rediscover its registration", c.table.StreamID)
	}
	var values = make([]any, len(c.table.CDColumns))
	var pointers = make([]any, len(values))
	for i := range values {
		pointers[i] = &values[i]
	}
	var lengths = make([]sql.NullInt64, len(c.table.LOBColumns))
	for i := range lengths {
		pointers = append(pointers, &lengths[i])
	}
	for rows.Next() {
		if err := rows.Scan(pointers...); err != nil {
			return err
		}
		if len(c.table.LOBColumns) > 0 {
			var key = []any{values[slices.Index(c.table.CDColumns, "IBMSNAP_COMMITSEQ")], values[slices.Index(c.table.CDColumns, "IBMSNAP_INTENTSEQ")]}
			if err := recoverLOBs(ctx, c.stream.db.conn, quoteTableName(registration.CDSchema, registration.CDTable)+" WHERE IBMSNAP_COMMITSEQ = ? AND IBMSNAP_INTENTSEQ = ?", key, c.table.LOBColumns, lengths, values, false); err != nil {
				return err
			}
		}
		var row = &db2ChangeRow{Table: c.table, Fields: make(map[string]any), Before: make(map[string]any)}
		for i, name := range c.table.CDColumns {
			var value = values[i]
			switch name {
			case "IBMSNAP_COMMITSEQ":
				row.CommitSeq, _ = value.([]byte)
			case "IBMSNAP_INTENTSEQ":
				row.IntentSeq, _ = value.([]byte)
			case "IBMSNAP_OPERATION":
				if bs, ok := value.([]byte); ok {
					value = string(bs)
				}
				row.Operation, _ = value.(string)
				row.Operation = strings.TrimSpace(row.Operation)
			case "IBMSNAP_LOGMARKER":
				row.LogMarker, _ = value.(time.Time)
			default:
				if _, ok := c.table.Discovery.Columns[name]; ok {
					row.Fields[name] = value
				}
				if prefix := registration.BeforeImagePrefix; prefix != "" && strings.HasPrefix(name, prefix) {
					var sourceName = strings.TrimPrefix(name, prefix)
					if _, ok := c.table.Discovery.Columns[sourceName]; ok {
						row.Before[sourceName] = value
					}
				}
			}
		}
		if len(row.CommitSeq) == 0 || len(row.IntentSeq) == 0 {
			return fmt.Errorf("missing binary commit or intent sequence in CD table for %s", c.table.StreamID)
		}
		c.batch = append(c.batch, row)
		c.commit, c.intent = row.CommitSeq, row.IntentSeq
	}
	if err := rows.Err(); err != nil {
		return err
	}
	c.done = len(c.batch) < c.stream.batchSize
	return nil
}

type changeHeap []*cdTableCursor

func (h changeHeap) Len() int { return len(h) }
func (h changeHeap) Less(i, j int) bool {
	var a, b = h[i].row, h[j].row
	if cmp := bytes.Compare(a.CommitSeq, b.CommitSeq); cmp != 0 {
		return cmp < 0
	}
	if cmp := bytes.Compare(a.IntentSeq, b.IntentSeq); cmp != 0 {
		return cmp < 0
	}
	return a.Table.StreamID.Compare(b.Table.StreamID) < 0
}
func (h changeHeap) Swap(i, j int)   { h[i], h[j] = h[j], h[i] }
func (h *changeHeap) Push(value any) { *h = append(*h, value.(*cdTableCursor)) }
func (h *changeHeap) Pop() any {
	var last = len(*h) - 1
	var value = (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return value
}
