package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

const (
	defaultSchemaName = "public"
)

// PersistentState represents the part of a connector's state which can be serialized
// and emitted in a state checkpoint, and resumed from after a restart.
type PersistentState struct {
	// The LSN (sequence number) from which replication should resume.
	CurrentLSN pglogrepl.LSN `json:"current_lsn"`
	// A mapping from table IDs (<namespace>.<table>) to table-sepcific state.
	Streams map[string]*TableState `json:"streams"`
}

// Validate performs basic sanity-checking after a state has been parsed from JSON. More
// detailed checks are performed by UpdateState.
func (ps *PersistentState) Validate() error {
	return nil
}

// pendingStreams returns the IDs of all streams which still need to be backfilled,
// in sorted order for test stability.
func (ps *PersistentState) pendingStreams() []string {
	var pending []string
	for id, tableState := range ps.Streams {
		if tableState.Mode == tableModeBackfill {
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
	KeyColumns []string `json:"key_columns,omitempty"`
	// Scanned is a FoundationDB-serialized tuple representing the KeyColumns
	// values of the last row which has been backfilled. Replication events will
	// only be emitted for rows <= this value while backfilling is in progress.
	Scanned []byte `json:"scanned,omitempty"`
}

const (
	tableModeBackfill = "Backfill"
	tableModeActive   = "Active"
	tableModeIgnore   = "Ignore"
)

// capture encapsulates the entire process of capturing data from PostgreSQL with a particular
// configuration/catalog/state and emitting records and state updates to some messageOutput.
type capture struct {
	state   *PersistentState           // State read from `state.json` and emitted as updates
	config  *Config                    // The configuration read from `config.json`
	catalog *airbyte.ConfiguredCatalog // The catalog read from `catalog.json`
	encoder messageOutput              // The encoder to which records and state updates are written

	connScan   *pgx.Conn          // The DB connection used for table scanning
	replStream *replicationStream // The high-level replication stream abstraction
}

// messageOutput represents "the thing to which Capture writes records and state checkpoints".
// A json.Encoder satisfies this interface in normal usage, but during tests a custom messageOutput
// is used which collects output in memory.
type messageOutput interface {
	Encode(v interface{}) error
}

// RunCapture is the top level of the database capture process. It  is responsible for opening DB
// connections, scanning tables, and then streaming replication events until shutdown conditions
// (if any) are met.
func RunCapture(ctx context.Context, config *Config, catalog *airbyte.ConfiguredCatalog, state *PersistentState, dest messageOutput) error {
	logrus.WithFields(logrus.Fields{
		"uri":  config.ToURI(),
		"slot": config.SlotName,
	}).Info("starting capture")

	// Normal database connection used for table scanning
	var connScan, err = pgx.Connect(ctx, config.ToURI())
	if err != nil {
		return fmt.Errorf("unable to connect to database for table scan: %w", err)
	}
	defer connScan.Close(ctx)

	// Replication database connection used for event streaming
	replConnConfig, err := pgconn.ParseConfig(config.ToURI())
	if err != nil {
		return err
	}
	replConnConfig.RuntimeParams["replication"] = "database"
	connRepl, err := pgconn.ConnectConfig(ctx, replConnConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to database for replication: %w", err)
	}
	replStream, err := startReplication(ctx, connRepl, config.SlotName, config.PublicationName, state.CurrentLSN)
	if err != nil {
		return fmt.Errorf("unable to start replication stream: %w", err)
	}
	defer replStream.Close(ctx)

	var c = &capture{
		state:      state,
		config:     config,
		catalog:    catalog,
		encoder:    dest,
		connScan:   connScan,
		replStream: replStream,
	}

	if err := c.updateState(ctx); err != nil {
		return fmt.Errorf("error updating capture state: %w", err)
	}

	return c.streamChanges(ctx)
}

func (c *capture) updateState(ctx context.Context) error {
	var stateDirty = false

	// Create the Streams map if nil
	if c.state.Streams == nil {
		c.state.Streams = make(map[string]*TableState)
		stateDirty = true
	}

	// Streams may be added to the catalog at various times. We need to
	// initialize new state entries for these streams, and while we're at
	// it this is a good time to sanity-check the primary key configuration.
	var dbPrimaryKeys, err = getPrimaryKeys(ctx, c.connScan)
	if err != nil {
		return fmt.Errorf("error querying database about primary keys: %w", err)
	}

	for _, catalogStream := range c.catalog.Streams {
		var streamID = joinStreamID(catalogStream.Stream.Namespace, catalogStream.Stream.Name)

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
		var primaryKey = dbPrimaryKeys[streamID]
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
		var streamState, ok = c.state.Streams[streamID]
		if !ok {
			c.state.Streams[streamID] = &TableState{Mode: tableModeBackfill, KeyColumns: primaryKey}
			stateDirty = true
			continue
		}

		if strings.Join(streamState.KeyColumns, ",") != strings.Join(primaryKey, ",") {
			return fmt.Errorf("stream %q: primary key %q doesn't match initialized scan key %q", streamID, primaryKey, streamState.KeyColumns)
		}
	}

	// Likewise streams may be removed from the catalog, and we need to forget
	// the corresponding state information.
	for streamID := range c.state.Streams {
		// List membership checks are always a pain in Go, but that's all this loop is
		var streamExistsInCatalog = false
		for _, catalogStream := range c.catalog.Streams {
			var catalogStreamID = joinStreamID(catalogStream.Stream.Namespace, catalogStream.Stream.Name)
			if streamID == catalogStreamID {
				streamExistsInCatalog = true
			}
		}

		if !streamExistsInCatalog {
			logrus.WithField("stream", streamID).Info("stream removed from catalog")
			delete(c.state.Streams, streamID)
			stateDirty = true
		}
	}

	// If we've altered the state, emit it to stdout. This isn't strictly necessary
	// but it helps to make the emitted sequence of state updates a lot more readable.
	if stateDirty {
		c.emitState(c.state)
	}
	return nil
}

// This is the main loop of the capture process, which interleaves replication event
// streaming with backfill scan results as necessary.
func (c *capture) streamChanges(ctx context.Context) error {
	var results *resultSet
	for c.state.pendingStreams() != nil {
		watermark, err := writeWatermark(ctx, c.connScan, c.config.WatermarksTable, c.config.SlotName)
		if err != nil {
			return fmt.Errorf("error writing next watermark: %w", err)
		}
		if err := c.streamToWatermark(watermark, results); err != nil {
			return fmt.Errorf("error streaming until watermark: %w", err)
		} else if err := c.emitBuffered(results); err != nil {
			return fmt.Errorf("error emitting buffered results: %w", err)
		}
		results, err = c.backfillStreams(ctx, c.state.pendingStreams())
		if err != nil {
			return fmt.Errorf("error performing backfill: %w", err)
		}
	}

	// Once there is no more backfilling to do, just stream changes forever and emit
	// state updates on every transaction commit.
	var targetWatermark = "nonexistent-watermark"
	if !c.catalog.Tail {
		var watermark, err = writeWatermark(ctx, c.connScan, c.config.WatermarksTable, c.config.SlotName)
		if err != nil {
			return fmt.Errorf("error writing poll watermark: %w", err)
		}
		targetWatermark = watermark
	}
	logrus.WithFields(logrus.Fields{
		"tail":      c.catalog.Tail,
		"watermark": targetWatermark,
	}).Info("streaming until watermark")
	return c.streamToWatermark(targetWatermark, nil)
}

func (c *capture) streamToWatermark(watermark string, results *resultSet) error {
	var watermarkReached = false
	for event := range c.replStream.Events() {
		// Flush events update the checkpointed LSN and trigger a state update.
		// If this is the commit after the target watermark, it also ends the loop.
		if event.Operation == FlushOp {
			c.state.CurrentLSN = event.Source.Location[pgLocLastCommitEndLSN]
			if err := c.emitState(c.state); err != nil {
				return fmt.Errorf("error emitting state update: %w", err)
			}
			if watermarkReached {
				return nil
			}
			continue
		}

		// Note when the expected watermark is finally observed. The subsequent Commit will exit the loop.
		// TODO(wgd): Can we ensure/require that 'WatermarksTable' is always fully-qualified?
		var streamID = joinStreamID(event.Source.Schema, event.Source.Table)
		if streamID == c.config.WatermarksTable && event.Operation != DeleteOp {
			var actual = event.After["watermark"]
			logrus.WithFields(logrus.Fields{
				"expected": watermark,
				"actual":   actual,
			}).Debug("watermark write")

			if actual == watermark {
				watermarkReached = true
			}
		}

		// Handle the easy cases: Events on ignored or fully-active tables.
		var tableState = c.state.Streams[streamID]
		if tableState == nil || tableState.Mode == tableModeIgnore {
			logrus.WithFields(logrus.Fields{
				"stream": streamID,
				"op":     event.Operation,
			}).Debug("ignoring stream")
			continue
		}
		if tableState.Mode == tableModeActive {
			if err := c.handleChangeEvent(event); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
			continue
		}
		if tableState.Mode != tableModeBackfill {
			return fmt.Errorf("table %q in invalid mode %q", streamID, tableState.Mode)
		}

		// While a table is being backfilled, events occurring *before* the current scan point
		// will be emitted, while events *after* that point will be patched (or ignored) into
		// the buffered resultSet.
		var rowKey, err = encodeRowKey(tableState.KeyColumns, event.keyFields())
		if err != nil {
			return fmt.Errorf("error encoding row key: %w", err)
		}
		if compareTuples(rowKey, tableState.Scanned) <= 0 {
			if err := c.handleChangeEvent(event); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
		} else if err := results.Patch(streamID, event); err != nil {
			return fmt.Errorf("error patching resultset: %w", err)
		}
	}
	return nil
}

func (c *capture) emitBuffered(results *resultSet) error {
	// Emit any buffered results and update table states accordingly.
	for _, streamID := range results.Streams() {
		var events = results.Changes(streamID)
		for _, event := range events {
			if err := c.handleChangeEvent(event); err != nil {
				return fmt.Errorf("error handling backfill change: %w", err)
			}
		}

		if results.Complete(streamID) {
			c.state.Streams[streamID].Mode = tableModeActive
			c.state.Streams[streamID].Scanned = nil
		} else {
			c.state.Streams[streamID].Scanned = results.Scanned(streamID)
		}
	}

	// Emit a new state update. The global `CurrentLSN` has been advanced by the
	// watermark commit event, and the individual stream `Scanned` tracking for
	// each stream has been advanced just above.
	return c.emitState(c.state)
}

func (c *capture) backfillStreams(ctx context.Context, streams []string) (*resultSet, error) {
	var results = newResultSet()

	// TODO(wgd): Add a sanity-check assertion that the current watermark value
	// in the database matches the one we previously wrote? Maybe that's more effort
	// than it's worth until we have other evidence of correctness violations though.

	// TODO(wgd): We can dispatch these table reads concurrently with a WaitGroup
	// for synchronization.
	for _, streamID := range streams {
		var streamState = c.state.Streams[streamID]

		// Fetch a chunk of entries from the specified stream
		var events, err = scanTableChunk(ctx, c.connScan, streamID, streamState.KeyColumns, streamState.Scanned)
		if err != nil {
			return nil, fmt.Errorf("error scanning table: %w", err)
		}

		// Translate the resulting list of entries into a backfillChunk
		if err := results.Buffer(streamID, streamState.KeyColumns, events); err != nil {
			return nil, fmt.Errorf("error buffering scan results: %w", err)
		}
	}
	return results, nil
}

func (c *capture) handleChangeEvent(event *changeEvent) error {
	var out map[string]interface{}

	var meta = struct {
		Operation ChangeOp               `json:"op"`
		Source    *postgresSource        `json:"source"`
		Before    map[string]interface{} `json:"before,omitempty"`
	}{
		Operation: event.Operation,
		Source:    &event.Source,
		Before:    nil,
	}

	switch event.Operation {
	case InsertOp:
		if err := translateRecordFields(event.After); err != nil {
			return fmt.Errorf("'after' of insert: %w", err)
		}
		out = event.After // Before is never used.
	case UpdateOp:
		if err := translateRecordFields(event.Before); err != nil {
			return fmt.Errorf("'before' of update: %w", err)
		} else if err := translateRecordFields(event.After); err != nil {
			return fmt.Errorf("'after' of update: %w", err)
		}
		meta.Before, out = event.Before, event.After
	case DeleteOp:
		if err := translateRecordFields(event.Before); err != nil {
			return fmt.Errorf("'before' of delete: %w", err)
		}
		out = event.Before // After is never used.
	}
	out["_meta"] = &meta

	var rawData, err = json.Marshal(out)
	if err != nil {
		return fmt.Errorf("error encoding record data: %w", err)
	}
	return c.emit(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: event.Source.Schema,
			Stream:    event.Source.Table,
			EmittedAt: time.Now().UnixNano() / int64(time.Millisecond),
			Data:      json.RawMessage(rawData),
		},
	})
}

func translateRecordFields(f map[string]interface{}) error {
	if f == nil {
		return nil
	}

	for id, val := range f {
		var translated, err = translateRecordField(val)
		if err != nil {
			return fmt.Errorf("error translating field %q value %v: %w", id, val, err)
		}
		f[id] = translated
	}
	return nil
}

// translateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodeable output format. As a concrete example, the
// PostgreSQL `cidr` type becomes a `*net.IPNet`, but the default JSON
// marshalling of a `net.IPNet` isn't a great fit and we'd prefer to use
// the `String()` method to get the usual "192.168.100.0/24" notation.
func translateRecordField(val interface{}) (interface{}, error) {
	switch x := val.(type) {
	case *net.IPNet:
		return x.String(), nil
	case net.HardwareAddr:
		return x.String(), nil
	case [16]uint8: // UUIDs
		var s = new(strings.Builder)
		for i := range x {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				s.WriteString("-")
			}
			fmt.Fprintf(s, "%02x", x[i])
		}
		return s.String(), nil
	}
	if _, ok := val.(json.Marshaler); ok {
		return val, nil
	}
	if enc, ok := val.(pgtype.TextEncoder); ok {
		var bs, err = enc.EncodeText(nil, nil)
		return string(bs), err
	}
	return val, nil
}

func (c *capture) emitState(state interface{}) error {
	var rawState, err = json.Marshal(state)
	if err != nil {
		return fmt.Errorf("error encoding state message: %w", err)
	}
	return c.emit(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(rawState)},
	})
}

func (c *capture) emit(msg interface{}) error {
	return c.encoder.Encode(msg)
}

// joinStreamID combines a namespace and a stream name into a fully-qualified
// stream (or table) identifier. Because it's possible for the namespace to be
// unspecified, we default to "public" in that situation.
func joinStreamID(namespace, stream string) string {
	if namespace == "" {
		namespace = defaultSchemaName
	}
	return strings.ToLower(namespace + "." + stream)
}
