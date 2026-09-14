package main

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgx/v5"
	"github.com/segmentio/encoding/json"
	"github.com/sirupsen/logrus"
)

// replicationStream consumes a CockroachDB sinkless ("core") changefeed — `CREATE CHANGEFEED
// FOR TABLE ... WITH ...` with no sink URI, which streams `(table, key, value)` rows back over
// the SQL connection — and presents it as a sqlcapture.ReplicationStream.
//
// `resolved` rows arrive on a fixed interval regardless of write activity, serving both as
// transaction-commit/fence points and as a liveness signal, so unlike the PostgreSQL connector
// there's no need to write watermarks to force progress on an idle database. The changefeed
// starts `WITH cursor='<anchor>', no_initial_scan` from the same HLC the backfill reads `AS OF
// SYSTEM TIME`, streams unfiltered, and relies on the collection key's reduction to collapse the
// snapshot/stream overlap and at-least-once duplicates.
//
// The anchor advances with every resolved timestamp (advanceSnapshotTimestamp) rather than
// staying fixed: otherwise a row in a not-yet-scanned chunk that changed after the anchor would
// have its change event emitted first and then its older snapshot state published by a later
// chunk, resurrecting stale or deleted rows in the reduced collection.
type replicationStream struct {
	db           *cockroachdbDatabase
	conn         *pgx.Conn          // Dedicated connection over which the changefeed streams.
	streamCtx    context.Context    // Long-lived context bounding the changefeed; cancelled on Close.
	streamCancel context.CancelFunc // Cancels streamCtx.

	resolvedInterval string // The changefeed `resolved` interval, e.g. "1s".
	cursor           string // Latest resolved HLC (or the snapshot timestamp before any resolved arrives).

	rows         pgx.Rows                       // The currently-running changefeed result set, or nil.
	activeByName map[string]sqlcapture.StreamID // Maps a changefeed's table name back to its StreamID.

	tables struct {
		sync.Mutex
		active map[sqlcapture.StreamID]*activeTableInfo // Tables which should be included in the changefeed.
		dirty  bool                                     // True when `active` has changed and the changefeed needs (re)starting.
	}
}

type activeTableInfo struct {
	schema, table string
	keyColumns    []string

	// fullPrimaryKey is the table's complete primary key including hidden columns, in key order.
	// Changefeed key arrays always hold the values of these columns (regardless of the collection
	// key), so delete-document reconstruction pairs against this rather than keyColumns.
	fullPrimaryKey []string
	// emitKeyColumns flags which fullPrimaryKey columns appear in captured documents. Hidden
	// virtual columns (hash-shard columns) are present in the key array but not in any document,
	// so their values are skipped when reconstructing a delete document.
	emitKeyColumns []bool
}

func (db *cockroachdbDatabase) ReplicationStream(ctx context.Context, startCursorJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var startCursor, err = unmarshalJSONString(startCursorJSON)
	if err != nil {
		return nil, fmt.Errorf("invalid start cursor JSON: %w", err)
	}

	// The snapshot timestamp anchors the whole capture: backfills read `AS OF SYSTEM TIME` it and
	// the changefeed starts from it. On a resumed capture we continue from the persisted cursor;
	// on a fresh capture we use the current cluster logical timestamp.
	var snapshotTimestamp = startCursor
	if snapshotTimestamp == "" {
		if err := db.conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&snapshotTimestamp); err != nil {
			return nil, fmt.Errorf("error querying cluster logical timestamp: %w", err)
		}
		logrus.WithField("snapshot", snapshotTimestamp).Info("starting fresh capture from current cluster timestamp")
	} else {
		logrus.WithField("cursor", snapshotTimestamp).Info("resuming capture from persisted cursor")
	}
	db.advanceSnapshotTimestamp(snapshotTimestamp)

	conn, err := pgx.Connect(ctx, db.config.ToURI())
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database for replication: %w", err)
	}
	// The changefeed is a long-running statement, so any statement timeout must be disabled.
	if _, err := conn.Exec(ctx, "SET statement_timeout = 0"); err != nil {
		logrus.WithField("err", err).Debug("failed to disable statement_timeout on replication connection")
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	var stream = &replicationStream{
		db:               db,
		conn:             conn,
		streamCtx:        streamCtx,
		streamCancel:     streamCancel,
		resolvedInterval: db.config.Advanced.ResolvedInterval,
		cursor:           snapshotTimestamp,
	}
	stream.tables.active = make(map[sqlcapture.StreamID]*activeTableInfo)
	return stream, nil
}

func (s *replicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, info *sqlcapture.DiscoveryInfo, metadata json.RawMessage) error {
	var fullPrimaryKey = keyColumns
	if details, ok := info.ExtraDetails.(*cockroachdbTableDiscoveryDetails); ok && len(details.fullPrimaryKey) > 0 {
		fullPrimaryKey = details.fullPrimaryKey
	}
	var emitKeyColumns = make([]bool, len(fullPrimaryKey))
	for i, col := range fullPrimaryKey {
		var _, discovered = info.Columns[col]
		emitKeyColumns[i] = discovered
	}

	s.tables.Lock()
	defer s.tables.Unlock()
	s.tables.active[streamID] = &activeTableInfo{
		schema:         info.Schema,
		table:          info.Name,
		keyColumns:     keyColumns,
		fullPrimaryKey: fullPrimaryKey,
		emitKeyColumns: emitKeyColumns,
	}
	s.tables.dirty = true
	return nil
}

func (s *replicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// The changefeed is started lazily by the first StreamToFence call which sees active tables.
	// On a fresh capture no tables are active until the generic backfill logic activates them, so
	// there's nothing to start here.
	return nil
}

// ensureChangefeed (re)starts the underlying changefeed if it isn't running or if the set of active
// tables has changed since it was last started.
func (s *replicationStream) ensureChangefeed(ctx context.Context) error {
	s.tables.Lock()
	defer s.tables.Unlock()

	if s.rows != nil && !s.tables.dirty {
		return nil
	}
	if len(s.tables.active) == 0 {
		return nil // Nothing to stream yet.
	}
	if s.rows != nil {
		s.rows.Close()
		s.rows = nil
	}

	// Build a stable, sorted table list for the changefeed statement and a name→StreamID map for
	// routing streamed rows back to their binding.
	var streamIDs = make([]sqlcapture.StreamID, 0, len(s.tables.active))
	for id := range s.tables.active {
		streamIDs = append(streamIDs, id)
	}
	sort.Slice(streamIDs, func(i, j int) bool { return streamIDs[i].String() < streamIDs[j].String() })

	// The changefeed is created `WITH full_table_name` so that streamed rows carry the fully
	// qualified `database.schema.table` name. Routing by bare table name would misdirect events
	// whenever two captured tables in different schemas share a name.
	var tableClauses = make([]string, 0, len(streamIDs))
	var activeByName = make(map[string]sqlcapture.StreamID, len(streamIDs))
	for _, id := range streamIDs {
		var info = s.tables.active[id]
		tableClauses = append(tableClauses, fmt.Sprintf("%s.%s", quoteIdentifier(info.schema), quoteIdentifier(info.table)))
		activeByName[fmt.Sprintf("%s.%s.%s", s.db.config.Database, info.schema, info.table)] = id
	}

	// The changefeed always (re)starts from the current anchor timestamp, so newly-activated
	// tables are covered from the same point their backfill reads (overlap collapsed by
	// reduction). `min_checkpoint_frequency` must be set alongside `resolved`, since the resolved
	// interval is otherwise clamped to its 30s default, capping how often a fence can be established.
	var anchor = s.db.snapshotTimestamp()
	var sql = fmt.Sprintf(
		"CREATE CHANGEFEED FOR TABLE %s WITH updated, resolved='%s', min_checkpoint_frequency='%s', cursor='%s', no_initial_scan, format=json, full_table_name",
		strings.Join(tableClauses, ", "), s.resolvedInterval, s.resolvedInterval, anchor,
	)
	logrus.WithFields(logrus.Fields{"tables": tableClauses, "cursor": anchor}).Info("starting changefeed")

	// The simple query protocol is used because the changefeed is a streaming statement which
	// doesn't fit the prepare/bind/execute model of the extended protocol.
	rows, err := s.conn.Query(s.streamCtx, sql, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return fmt.Errorf("error starting changefeed: %w", err)
	}
	s.rows = rows
	s.activeByName = activeByName
	s.tables.dirty = false
	return nil
}

func (s *replicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	if err := s.ensureChangefeed(ctx); err != nil {
		return err
	}

	s.tables.Lock()
	var haveActiveTables = len(s.tables.active) > 0
	s.tables.Unlock()
	if !haveActiveTables {
		// No tables are active yet (the catch-up phase of a fresh capture). Emit a commit at the
		// current cursor so the generic logic has a checkpoint, then return.
		return callback(&cockroachdbCommitEvent{Resolved: s.cursor})
	}

	// Establish a fence target: the current cluster logical timestamp. We stream until a resolved
	// timestamp at least this large is observed, which guarantees the fence is no earlier than the
	// point at which StreamToFence began. When fenceAfter is positive we additionally require that
	// much wall-clock time to elapse first, so that indefinite streaming doesn't busy-loop.
	var fenceTarget, err = s.queryHLC(ctx)
	if err != nil {
		return fmt.Errorf("error establishing fence position: %w", err)
	}
	var deadline = time.Now().Add(fenceAfter)
	logrus.WithFields(logrus.Fields{"target": fenceTarget, "fenceAfter": fenceAfter.String()}).Debug("streaming to fence")

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		event, resolved, err := s.readRow()
		if err != nil {
			return err
		}
		if event != nil {
			if err := callback(event); err != nil {
				return err
			}
			continue
		}
		// A resolved checkpoint is a transaction-commit/fence point. It also advances the anchor
		// timestamp at which subsequent backfill chunks read, which is what keeps unfiltered
		// streaming and chunked backfills convergent (see the replicationStream doc comment).
		if err := callback(&cockroachdbCommitEvent{Resolved: resolved}); err != nil {
			return err
		}
		s.cursor = resolved
		s.db.advanceSnapshotTimestamp(resolved)
		if time.Now().After(deadline) && compareHLC(resolved, fenceTarget) >= 0 {
			logrus.WithField("cursor", resolved).Debug("reached fence")
			return nil
		}
	}
}

// readRow reads the next changefeed row, blocking until one is available. It returns either a
// change event (resolved == "") or a resolved HLC timestamp (event == nil).
func (s *replicationStream) readRow() (sqlcapture.DatabaseEvent, string, error) {
	if !s.rows.Next() {
		if err := s.rows.Err(); err != nil {
			return nil, "", fmt.Errorf("changefeed stream error: %w", err)
		}
		return nil, "", fmt.Errorf("changefeed stream ended unexpectedly")
	}

	var tableName *string
	var key, value []byte
	if err := s.rows.Scan(&tableName, &key, &value); err != nil {
		return nil, "", fmt.Errorf("error scanning changefeed row: %w", err)
	}

	var msg changefeedMessage
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, "", fmt.Errorf("error parsing changefeed value %q: %w", value, err)
	}
	if msg.Resolved != "" {
		return nil, msg.Resolved, nil
	}

	if tableName == nil {
		return nil, "", fmt.Errorf("changefeed data row with no table name: %q", value)
	}
	var streamID, ok = s.activeByName[*tableName]
	if !ok {
		// A row for a table we're not capturing (shouldn't happen since we name the tables
		// explicitly). Treat as a keepalive so it doesn't trip the streaming watchdog.
		logrus.WithField("table", *tableName).Warn("received changefeed row for unrecognized table")
		return &sqlcapture.KeepaliveEvent{}, "", nil
	}
	event, err := s.makeChangeEvent(streamID, key, &msg)
	if err != nil {
		return nil, "", err
	}
	return event, "", nil
}

func (s *replicationStream) makeChangeEvent(streamID sqlcapture.StreamID, key []byte, msg *changefeedMessage) (sqlcapture.DatabaseEvent, error) {
	s.tables.Lock()
	var info = s.tables.active[streamID]
	s.tables.Unlock()
	if info == nil {
		return &sqlcapture.KeepaliveEvent{}, nil
	}

	var source = cockroachdbSource{
		SourceCommon: sqlcapture.SourceCommon{
			Millis: hlcToMillis(msg.Updated),
			Schema: info.schema,
			Table:  info.table,
		},
		Cursor: msg.Updated,
		Tag:    s.db.config.Advanced.SourceTag,
	}

	// A null `after` indicates a delete. We reconstruct a key-only document from the changefeed's
	// key array so that the document carries the primary key, and emit it with op='d'. The generated
	// collection schema reduces op='d' documents with `delete: true`.
	if isNullJSON(msg.After) {
		document, err := deleteDocument(info.fullPrimaryKey, info.emitKeyColumns, key)
		if err != nil {
			return nil, fmt.Errorf("error building delete document for %q: %w", streamID, err)
		}
		return &cockroachdbChangeEvent{
			stream:   streamID,
			op:       sqlcapture.DeleteOp,
			source:   source,
			document: document,
		}, nil
	}

	// An upsert. The changefeed doesn't distinguish inserts from updates, so we emit op='u'; the
	// distinction is immaterial to the merge reduction which produces the final row state.
	return &cockroachdbChangeEvent{
		stream:   streamID,
		op:       sqlcapture.UpdateOp,
		source:   source,
		document: msg.After,
	}, nil
}

// deleteDocument builds a `{"<key0>":<val0>,...}` JSON object from the changefeed key array. The
// key array holds the values of the table's full primary key in key order (hidden columns
// included, regardless of the collection key), so values are paired against the full primary key
// and only the columns which appear in captured documents are emitted.
func deleteDocument(fullPrimaryKey []string, emitKeyColumns []bool, key []byte) (json.RawMessage, error) {
	var keyValues []json.RawMessage
	if err := json.Unmarshal(key, &keyValues); err != nil {
		return nil, fmt.Errorf("error parsing changefeed key %q: %w", key, err)
	}
	if len(keyValues) != len(fullPrimaryKey) {
		return nil, fmt.Errorf("changefeed key %q has %d values but the primary key %q has %d columns", key, len(keyValues), fullPrimaryKey, len(fullPrimaryKey))
	}
	var buf = []byte{'{'}
	var emitted int
	for i, col := range fullPrimaryKey {
		if !emitKeyColumns[i] {
			continue
		}
		if emitted > 0 {
			buf = append(buf, ',')
		}
		colName, err := json.Marshal(col)
		if err != nil {
			return nil, err
		}
		buf = append(buf, colName...)
		buf = append(buf, ':')
		buf = append(buf, keyValues[i]...)
		emitted++
	}
	return append(buf, '}'), nil
}

func (s *replicationStream) Acknowledge(ctx context.Context, cursor json.RawMessage) error {
	// Sinkless changefeeds don't require client acknowledgement to advance: on restart we simply
	// resume from the persisted cursor. There's nothing to communicate back to the server.
	return nil
}

func (s *replicationStream) Close(ctx context.Context) error {
	logrus.Debug("replication stream close requested")
	s.streamCancel()
	if s.rows != nil {
		s.rows.Close()
	}
	return s.conn.Close(ctx)
}

// queryHLC returns the current cluster logical timestamp via the (non-changefeed) connection pool.
func (s *replicationStream) queryHLC(ctx context.Context) (string, error) {
	var hlc string
	if err := s.db.conn.QueryRow(ctx, "SELECT cluster_logical_timestamp()").Scan(&hlc); err != nil {
		return "", err
	}
	return hlc, nil
}

type changefeedMessage struct {
	After    json.RawMessage `json:"after"`
	Updated  string          `json:"updated"`
	Resolved string          `json:"resolved"`
}

func isNullJSON(bs json.RawMessage) bool {
	var trimmed = strings.TrimSpace(string(bs))
	return trimmed == "" || trimmed == "null"
}

// compareHLC compares two HLC timestamp strings of the form "<nanos>.<logical>". Both halves have a
// fixed width (the fractional part is always 10 digits), so stripping the decimal point yields
// integers whose magnitude comparison reproduces HLC ordering.
func compareHLC(a, b string) int {
	return hlcInt(a).Cmp(hlcInt(b))
}

func hlcInt(s string) *big.Int {
	var n = new(big.Int)
	if _, ok := n.SetString(strings.Replace(s, ".", "", 1), 10); !ok {
		return big.NewInt(0)
	}
	return n
}

// hlcToMillis extracts a Unix-milliseconds timestamp from the nanosecond integer part of an HLC.
func hlcToMillis(hlc string) int64 {
	var intPart = hlc
	if i := strings.IndexByte(hlc, '.'); i >= 0 {
		intPart = hlc[:i]
	}
	var nanos, err = strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return 0
	}
	return nanos / 1e6
}

func unmarshalJSONString(bs json.RawMessage) (string, error) {
	if bs == nil {
		return "", nil
	}
	var str string
	if err := json.Unmarshal(bs, &str); err != nil {
		return "", err
	}
	return str, nil
}
