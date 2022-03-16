package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/estuary/flow/go/protocols/airbyte"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// PersistentState represents the part of a connector's state which can be serialized
// and emitted in a state checkpoint, and resumed from after a restart.
type PersistentState struct {
	Cursor  string                `json:"cursor"`            // The replication cursor of the most recent 'Commit' event
	Streams map[string]TableState `json:"streams,omitempty"` // A mapping from table IDs (<namespace>.<table>) to table-specific state.
}

// Validate performs basic sanity-checking after a state has been parsed from JSON. More
// detailed checks are performed by UpdateState.
func (ps *PersistentState) Validate() error {
	return nil
}

// PendingStreams returns the IDs of all streams which still need to be backfilled,
// in sorted order for reproducibility.
func (ps *PersistentState) PendingStreams() []string {
	var pending []string
	for id, tableState := range ps.Streams {
		if tableState.Mode == TableModeBackfill {
			pending = append(pending, id)
		}
	}
	sort.Strings(pending)
	return pending
}

// TableState represents the serializable/resumable state of a particular table's capture.
// It is mostly concerned with the "backfill" scanning process and the transition from that
// to logical replication.
type TableState struct {
	// Mode is either "Backfill" during the backfill scanning process
	// or "Active" once the backfill is complete.
	Mode string `json:"mode"`
	// KeyColumns is the "primary key" used for ordering/chunking the backfill scan.
	KeyColumns []string `json:"key_columns"`
	// Scanned is a FoundationDB-serialized tuple representing the KeyColumns
	// values of the last row which has been backfilled. Replication events will
	// only be emitted for rows <= this value while backfilling is in progress.
	Scanned []byte `json:"scanned"`
	// dirty is set whenever the table state changes, and cleared whenever
	// a state update is emitted. It should never be serialized itself.
	dirty bool
}

// The table's mode can be one of:
//   Backfill: The table's rows are being backfilled and replication events will only be emitted for the already-backfilled portion.
//   Active: The table finished backfilling and replication events are emitted for the entire table.
//   Ignore: The table is being deliberately ignored.
const (
	TableModeIgnore   = "Ignore"
	TableModeBackfill = "Backfill"
	TableModeActive   = "Active"
)

// MessageOutput represents "the thing to which Capture writes records and state checkpoints".
// A json.Encoder satisfies this interface in normal usage, but during tests a custom MessageOutput
// is used which collects output in memory.
type MessageOutput interface {
	Encode(v interface{}) error
}

// Capture encapsulates the generic process of capturing data from a SQL database
// via replication, backfilling preexisting table contents, and emitting records/state
// updates. It uses the `Database` interface to interact with a specific database.
type Capture struct {
	Catalog  *airbyte.ConfiguredCatalog // The catalog read from `catalog.json`
	State    *PersistentState           // State read from `state.json` and emitted as updates
	Encoder  MessageOutput              // The encoder to which records and state updates are written
	Database Database                   // The database-specific interface which is operated by the generic Capture logic

	discovery map[string]TableInfo // Cached result of the most recent table discovery request
}

const (
	nonexistentWatermark   = "nonexistent-watermark" // The watermark which will be used for the final "tailing" stream call.
	streamIdleWarning      = 60 * time.Second        // After `streamIdleWarning` has elapsed since the last replication event, we log a warning.
	streamProgressInterval = 60 * time.Second        // After `streamProgressInterval` the replication streaming code may log a progress report.
)

// Run is the top level entry point of the capture process.
func (c *Capture) Run(ctx context.Context) error {
	var replStream, err = c.Database.StartReplication(ctx, c.State.Cursor)
	if err != nil {
		return fmt.Errorf("error starting replication: %w", err)
	}
	defer replStream.Close(ctx)

	// Perform discovery and cache the result. This is used at startup when
	// updating the state to reflect catalog changes, and then later it is
	// plumbed through so that value translation can take column types into
	// account.
	logrus.Info("discovering tables")
	c.discovery, err = c.Database.DiscoverTables(ctx)
	if err != nil {
		return fmt.Errorf("error discovering database tables: %w", err)
	}

	if err := c.updateState(ctx); err != nil {
		return fmt.Errorf("error updating capture state: %w", err)
	}

	// Backfill any tables which require it
	var results *resultSet
	for c.State.PendingStreams() != nil {
		var watermark = uuid.New().String()
		if err := c.Database.WriteWatermark(ctx, watermark); err != nil {
			return fmt.Errorf("error writing next watermark: %w", err)
		}
		if err := c.streamToWatermark(replStream, watermark, results); err != nil {
			return fmt.Errorf("error streaming until watermark: %w", err)
		} else if err := c.emitBuffered(results); err != nil {
			return fmt.Errorf("error emitting buffered results: %w", err)
		}
		results, err = c.backfillStreams(ctx, c.State.PendingStreams())
		if err != nil {
			return fmt.Errorf("error performing backfill: %w", err)
		}
	}
	logrus.Debug("finished backfilling tables")

	// Once there is no more backfilling to do, just stream changes forever and emit
	// state updates on every transaction commit.
	var targetWatermark = nonexistentWatermark
	if !c.Catalog.Tail {
		var watermark = uuid.New().String()
		if err = c.Database.WriteWatermark(ctx, watermark); err != nil {
			return fmt.Errorf("error writing poll watermark: %w", err)
		}
		targetWatermark = watermark
	}
	return c.streamToWatermark(replStream, targetWatermark, nil)
}

func (c *Capture) updateState(ctx context.Context) error {
	// Create the Streams map if nil
	if c.State.Streams == nil {
		c.State.Streams = make(map[string]TableState)
	}

	// Just to be a bit more forgiving, default unspecified namespaces to a
	// reasonable value. This whole bit of logic, as well as the `DefaultSchema`
	// method, could be removed but only if we're willing to let things break
	// when the user's catalog fails to specify the namespace.
	for idx := range c.Catalog.Streams {
		if c.Catalog.Streams[idx].Stream.Namespace == "" {
			var defaultSchema, err = c.Database.DefaultSchema(ctx)
			if err != nil {
				return fmt.Errorf("error querying default schema: %w", err)
			}
			c.Catalog.Streams[idx].Stream.Namespace = defaultSchema
		}
	}

	// Streams may be added to the catalog at various times. We need to
	// initialize new state entries for these streams, and while we're at
	// it this is a good time to sanity-check the primary key configuration.
	for _, catalogStream := range c.Catalog.Streams {
		var streamID = JoinStreamID(catalogStream.Stream.Namespace, catalogStream.Stream.Name)

		// In the catalog a primary key is an array of arrays of strings, but in the
		// case of Postgres each of those sub-arrays must be length-1 because we're
		// just naming a column and can't descend into individual fields.
		var catalogPrimaryKey []string
		for _, col := range catalogStream.PrimaryKey {
			if len(col) != 1 {
				return fmt.Errorf("stream %q: primary key element %q invalid", streamID, col)
			}
			catalogPrimaryKey = append(catalogPrimaryKey, col[0])
		}

		// If the `PrimaryKey` property is specified in the catalog then use that,
		// otherwise use the "native" primary key of this table in the database.
		// Print a warning if the two are not the same.
		var primaryKey = c.discovery[streamID].PrimaryKey
		if len(primaryKey) != 0 {
			logrus.WithFields(logrus.Fields{
				"table": streamID,
				"key":   primaryKey,
			}).Debug("queried primary key")
		}
		if len(catalogPrimaryKey) != 0 {
			if strings.Join(primaryKey, ",") != strings.Join(catalogPrimaryKey, ",") {
				logrus.WithFields(logrus.Fields{
					"stream":      streamID,
					"catalogKey":  catalogPrimaryKey,
					"databaseKey": primaryKey,
				}).Warn("primary key in catalog differs from database table")
			}
			primaryKey = catalogPrimaryKey
		}
		if len(primaryKey) == 0 {
			return fmt.Errorf("stream %q: primary key unspecified in the catalog and no primary key found in database", streamID)
		}

		// See if the stream is already initialized. If it's not, then create it.
		var streamState, ok = c.State.Streams[streamID]
		if !ok || streamState.Mode == TableModeIgnore {
			c.State.Streams[streamID] = TableState{Mode: TableModeBackfill, KeyColumns: primaryKey, dirty: true}
			continue
		}

		if strings.Join(streamState.KeyColumns, ",") != strings.Join(primaryKey, ",") {
			return fmt.Errorf("stream %q: primary key %q doesn't match initialized scan key %q", streamID, primaryKey, streamState.KeyColumns)
		}
	}

	// Likewise streams may be removed from the catalog, and we need to forget
	// the corresponding state information.
	for streamID := range c.State.Streams {
		// List membership checks are always a pain in Go, but that's all this loop is
		var streamExistsInCatalog = false
		for _, catalogStream := range c.Catalog.Streams {
			var catalogStreamID = JoinStreamID(catalogStream.Stream.Namespace, catalogStream.Stream.Name)
			if streamID == catalogStreamID {
				streamExistsInCatalog = true
			}
		}

		if !streamExistsInCatalog {
			logrus.WithField("stream", streamID).Info("stream removed from catalog")
			c.State.Streams[streamID] = TableState{Mode: TableModeIgnore, dirty: true}
		}
	}

	// Emit the new state to stdout. This isn't strictly necessary but it helps to make
	// the emitted sequence of state updates a lot more readable.
	return c.emitState()
}

func (c *Capture) streamToWatermark(replStream ReplicationStream, watermark string, results *resultSet) error {
	logrus.WithField("watermark", watermark).Info("streaming to watermark")
	var watermarksTable = c.Database.WatermarksTable()
	var watermarkReached = false

	// This part is all about logging enough information to give insight into
	// connector operation without being unusably spammy. These logs take two forms:
	//
	//   + A warning/info message when the stream has been sitting idle
	//     for a while since the last replication event. This is a warning
	//     when we expect to catch up to a watermark, and informational when
	//     that watermark is "nonexistent-watermark".
	//   + Periodic progress updates, just to show that streaming is ongoing.
	//
	// Neither of these can occur more frequently than their respective timeout
	// values, so the impact of the additional logging should be minimal.
	//
	// During backfills no individual `streamToWatermark` call should last long
	// enough for those timeouts to be hit (and if they are then that's very
	// useful information). Once we hit the steady state of pure replication
	// streaming:
	//
	//   + If change events are sufficiently infrequent or bursty, each change
	//     event (or burst of events) will elicit a "progress" log, followed by
	//     an "idle" log message some time after the event-burst has ended.
	//   + If a burst of events continues for longer than `streamProgressInterval`
	//     then additional progress reports will be logged.
	//   + A consistent high rate of events is just a burst that hasn't ended (so
	//     far), and will report progress every `streamProgressInterval`.
	var eventCount int
	var nextProgress = time.Now().Add(streamProgressInterval)
	var idleTimeout = time.AfterFunc(streamIdleWarning, func() {
		if watermark != nonexistentWatermark {
			logrus.WithField("timeout", streamIdleWarning.String()).Warn("replication stream idle")
		} else {
			logrus.WithField("timeout", streamIdleWarning.String()).Info("replication stream idle")
		}
	})
	defer idleTimeout.Stop()

	for event := range replStream.Events() {
		// Progress logging concerns
		eventCount++
		if time.Now().After(nextProgress) {
			logrus.WithField("count", eventCount).Info("replication stream progress")
			nextProgress = time.Now().Add(streamProgressInterval)
		}
		idleTimeout.Reset(streamIdleWarning)

		// Flush events update the checkpointed LSN and trigger a state update.
		// If this is the commit after the target watermark, it also ends the loop.
		if event.Operation == FlushOp {
			c.State.Cursor = event.Source.Cursor()
			if err := c.emitState(); err != nil {
				return fmt.Errorf("error emitting state update: %w", err)
			}
			if watermarkReached {
				return nil
			}
			continue
		}

		// Note when the expected watermark is finally observed. The subsequent Commit will exit the loop.
		var sourceCommon = event.Source.Common()
		var streamID = JoinStreamID(sourceCommon.Schema, sourceCommon.Table)
		if streamID == watermarksTable && event.Operation != DeleteOp {
			var actual = event.After["watermark"]
			logrus.WithFields(logrus.Fields{
				"expected": watermark,
				"actual":   actual,
			}).Debug("watermark change")
			if actual == watermark {
				watermarkReached = true
			}
		}

		// Handle the easy cases: Events on ignored or fully-active tables.
		var tableState = c.State.Streams[streamID]
		if tableState.Mode == "" || tableState.Mode == TableModeIgnore {
			logrus.WithFields(logrus.Fields{
				"stream": streamID,
				"op":     event.Operation,
			}).Debug("ignoring stream")
			continue
		}
		if tableState.Mode == TableModeActive {
			if err := c.handleChangeEvent(streamID, event); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
			continue
		}
		if tableState.Mode != TableModeBackfill {
			return fmt.Errorf("table %q in invalid mode %q", streamID, tableState.Mode)
		}

		// While a table is being backfilled, events occurring *before* the current scan point
		// will be emitted, while events *after* that point will be patched (or ignored) into
		// the buffered resultSet.
		var rowKey, err = encodeRowKey(tableState.KeyColumns, event.KeyFields(), c.Database)
		if err != nil {
			return fmt.Errorf("error encoding row key: %w", err)
		}
		if compareTuples(rowKey, tableState.Scanned) <= 0 {
			if err := c.handleChangeEvent(streamID, event); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
		} else if err := results.Patch(streamID, event, rowKey); err != nil {
			return fmt.Errorf("error patching resultset: %w", err)
		}
	}
	return nil
}

func (c *Capture) emitBuffered(results *resultSet) error {
	// Emit any buffered results and update table states accordingly.
	for _, streamID := range results.Streams() {
		var events = results.Changes(streamID)
		for _, event := range events {
			if err := c.handleChangeEvent(streamID, event); err != nil {
				return fmt.Errorf("error handling backfill change: %w", err)
			}
		}

		var state = c.State.Streams[streamID]
		if results.Complete(streamID) {
			state.Mode = TableModeActive
			state.Scanned = nil
		} else {
			state.Scanned = results.Scanned(streamID)
		}
		state.dirty = true
		c.State.Streams[streamID] = state
	}

	// Emit a new state update. The global `CurrentLSN` has been advanced by the
	// watermark commit event, and the individual stream `Scanned` tracking for
	// each stream has been advanced just above.
	return c.emitState()
}

func (c *Capture) backfillStreams(ctx context.Context, streams []string) (*resultSet, error) {
	logrus.WithField("streams", streams).Info("backfilling streams")
	var results = newResultSet()

	// TODO(wgd): Add a sanity-check assertion that the current watermark value
	// in the database matches the one we previously wrote? Maybe that's more effort
	// than it's worth until we have other evidence of correctness violations though.

	// TODO(wgd): We can dispatch these table reads concurrently with a WaitGroup
	// for synchronization.
	for _, streamID := range streams {
		var streamState = c.State.Streams[streamID]
		var schema, table = splitStreamID(streamID)

		// Fetch a chunk of entries from the specified stream
		var err error
		var resumeKey []interface{}
		if streamState.Scanned != nil {
			resumeKey, err = unpackTuple(streamState.Scanned, c.Database)
			if err != nil {
				return nil, fmt.Errorf("error unpacking resume key: %w", err)
			}
			if len(resumeKey) != len(streamState.KeyColumns) {
				return nil, fmt.Errorf("expected %d resume-key values but got %d", len(streamState.KeyColumns), len(resumeKey))
			}
		}

		events, err := c.Database.ScanTableChunk(ctx, schema, table, streamState.KeyColumns, resumeKey)
		if err != nil {
			return nil, fmt.Errorf("error scanning table: %w", err)
		}

		// Translate the resulting list of entries into a backfillChunk
		if err := results.Buffer(streamID, streamState.KeyColumns, events, c.Database); err != nil {
			return nil, fmt.Errorf("error buffering scan results: %w", err)
		}
	}
	return results, nil
}

func (c *Capture) handleChangeEvent(streamID string, event ChangeEvent) error {
	var out map[string]interface{}

	var meta = struct {
		Operation ChangeOp               `json:"op"`
		Source    SourceMetadata         `json:"source"`
		Before    map[string]interface{} `json:"before,omitempty"`
	}{
		Operation: event.Operation,
		Source:    event.Source,
		Before:    nil,
	}

	switch event.Operation {
	case InsertOp:
		if err := c.translateRecordFields(c.Database, streamID, event.After); err != nil {
			return fmt.Errorf("'after' of insert: %w", err)
		}
		out = event.After // Before is never used.
	case UpdateOp:
		if err := c.translateRecordFields(c.Database, streamID, event.Before); err != nil {
			return fmt.Errorf("'before' of update: %w", err)
		} else if err := c.translateRecordFields(c.Database, streamID, event.After); err != nil {
			return fmt.Errorf("'after' of update: %w", err)
		}
		meta.Before, out = event.Before, event.After
	case DeleteOp:
		if err := c.translateRecordFields(c.Database, streamID, event.Before); err != nil {
			return fmt.Errorf("'before' of delete: %w", err)
		}
		out = event.Before // After is never used.
	}
	out["_meta"] = &meta

	var rawData, err = json.Marshal(out)
	if err != nil {
		return fmt.Errorf("error encoding record data: %w", err)
	}
	var sourceCommon = event.Source.Common()
	return c.Encoder.Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: sourceCommon.Schema,
			Stream:    sourceCommon.Table,
			EmittedAt: time.Now().UnixNano() / int64(time.Millisecond),
			Data:      json.RawMessage(rawData),
		},
	})
}

func (c *Capture) translateRecordFields(db Database, streamID string, f map[string]interface{}) error {
	if f == nil {
		return nil
	}

	for id, val := range f {
		// Get information about this column, if the table and column name are known.
		// Otherwise columnInfo will be nil, but it's up to TranslateRecordField to
		// decide whether that is an error.
		var columnInfo *ColumnInfo
		if tableInfo, ok := c.discovery[streamID]; ok {
			if info, ok := tableInfo.Columns[id]; ok {
				columnInfo = &info
			}
		}

		var translated, err = db.TranslateRecordField(columnInfo, val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

func (c *Capture) emitState() error {
	// Put together an update which includes only those streams which have changed
	// since the last state output. At the same time, clear the dirty flags on all
	// those tables.
	var stateUpdate = PersistentState{
		Cursor:  c.State.Cursor,
		Streams: make(map[string]TableState),
	}
	for streamID, state := range c.State.Streams {
		if state.dirty {
			state.dirty = false
			c.State.Streams[streamID] = state
			stateUpdate.Streams[streamID] = state
		}
	}

	var rawState, err = json.Marshal(stateUpdate)
	if err != nil {
		return fmt.Errorf("error encoding state message: %w", err)
	}
	return c.Encoder.Encode(airbyte.Message{
		Type: airbyte.MessageTypeState,
		State: &airbyte.State{
			Data:  json.RawMessage(rawState),
			Merge: true,
		},
	})
}

// JoinStreamID combines a namespace and a stream name into a dotted name like "public.foo_table".
func JoinStreamID(namespace, stream string) string {
	return strings.ToLower(namespace + "." + stream)
}

// splitStreamID decomposes a dotted name like "public.foo_table" into separate schema and table components.
func splitStreamID(streamID string) (string, string) {
	var parts = strings.SplitN(streamID, ".", 2)
	return parts[0], parts[1]
}
