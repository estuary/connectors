package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/google/uuid"
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

// TableState represents the serializable/resumable state of a particular table's capture.
// It is mostly concerned with the "backfill" scanning process and the transition from that
// to logical replication.
type TableState struct {
	// Mode is either "Backfill" during the backfill scanning process
	// or "Active" once the backfill is complete.
	Mode string `json:"mode"`
	// ScanKey is the "primary key" used for ordering/chunking the backfill scan.
	ScanKey []string `json:"scan_key,omitempty"`
	// Scanned is a FoundationDB-serialized tuple representing the ScanKey column
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
	connRepl   *pgconn.PgConn     // The DB connection used for replication streaming
	replStream *replicationStream // The high-level replication stream abstraction
	watchdog   *time.Timer        // If non-nil, the Reset() method will be invoked whenever a record is emitted

	// We keep a count of change events since the last state checkpoint was
	// emitted. Currently this is only used to suppress "empty" commit messages
	// from triggering a state update (which improves test stability), but
	// this could also be used to "coalesce" smaller transactions in the
	// future.
	changesSinceLastCheckpoint int

	results   *backfillResults // The next chunk of buffered backfill scan results
	watermark string           // The watermark, which is known to occur after the point in time at which `results` was captured

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
	logrus.WithField("uri", config.ConnectionURI).WithField("slot", config.SlotName).Info("starting capture")

	if config.MaxLifespanSeconds != 0 {
		var duration = time.Duration(config.MaxLifespanSeconds * float64(time.Second))
		logrus.WithField("duration", duration).Info("limiting connector lifespan")
		var limitedCtx, cancel = context.WithTimeout(ctx, duration)
		defer cancel()
		ctx = limitedCtx
	}

	// Normal database connection used for table scanning
	var connScan, err = pgx.Connect(ctx, config.ConnectionURI)
	if err != nil {
		return fmt.Errorf("unable to connect to database for table scan: %w", err)
	}
	// TODO(wgd): Close this after merging NewCapture() and Execute()

	// Replication database connection used for event streaming
	replConnConfig, err := pgconn.ParseConfig(config.ConnectionURI)
	if err != nil {
		return err
	}
	replConnConfig.RuntimeParams["replication"] = "database"
	connRepl, err := pgconn.ConnectConfig(ctx, replConnConfig)
	if err != nil {
		return fmt.Errorf("unable to connect to database for replication: %w", err)
	}

	var c = &capture{
		state:    state,
		config:   config,
		catalog:  catalog,
		encoder:  dest,
		connScan: connScan,
		connRepl: connRepl,
	}

	if err := c.updateState(ctx); err != nil {
		return fmt.Errorf("error updating capture state: %w", err)
	}

	// In non-tailing mode (which should only occur during development) we need
	// to shut down after no further changes have been reported for a while. To
	// do this we create a cancellable context, and a watchdog timer which will
	// perform said cancellation if `PollTimeout` elapses between resets.
	if !c.catalog.Tail && c.config.PollTimeoutSeconds != 0 {
		var streamCtx, streamCancel = context.WithCancel(ctx)
		defer streamCancel()
		var wdtDuration = time.Duration(c.config.PollTimeoutSeconds * float64(time.Second))
		c.watchdog = time.AfterFunc(wdtDuration, streamCancel)
		ctx = streamCtx
	}

	replStream, err := startReplication(ctx, connRepl, config.SlotName, config.PublicationName, state.CurrentLSN)
	if err != nil {
		return fmt.Errorf("unable to start replication stream: %w", err)
	}
	defer replStream.Close(ctx)
	c.replStream = replStream

	err = c.streamChanges(ctx)
	if errors.Is(err, context.DeadlineExceeded) && c.config.MaxLifespanSeconds != 0 {
		logrus.WithField("err", err).WithField("maxLifespan", c.config.MaxLifespanSeconds).Info("maximum lifespan reached")
		return nil
	}
	if errors.Is(err, context.Canceled) && !c.catalog.Tail {
		return nil
	}
	return err
}

func (c *capture) updateState(ctx context.Context) error {
	var stateDirty = false

	// Create the Streams map if nil
	if c.state.Streams == nil {
		c.state.Streams = make(map[string]*TableState)
		stateDirty = true
	}

	// The first time the connector is run, we need to figure out the "Current LSN"
	// from which we will start replication. On subsequent runs this comes from the
	// resume state.
	if c.state.CurrentLSN == 0 {
		var sysident, err = pglogrepl.IdentifySystem(ctx, c.connRepl)
		if err != nil {
			return fmt.Errorf("unable to get current LSN from database: %w", err)
		}
		c.state.CurrentLSN = sysident.XLogPos
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
		// Table names coming from Postgres are always lowercase, so if we
		// normalize the stream name to lowercase on the catalog->state
		// transition then we can ignore the issue later on.
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
			logrus.WithField("table", streamID).WithField("key", primaryKey).Debug("queried primary key")
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
			c.state.Streams[streamID] = &TableState{Mode: tableModeBackfill, ScanKey: primaryKey}
			stateDirty = true
			continue
		}

		if strings.Join(streamState.ScanKey, ",") != strings.Join(primaryKey, ",") {
			return fmt.Errorf("stream %q: primary key %q doesn't match initialized scan key %q", streamID, primaryKey, streamState.ScanKey)
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

// joinStreamID combines a namespace and a stream name into a fully-qualified
// stream (or table) identifier. Because it's possible for the namespace to be
// unspecified, we default to "public" in that situation.
func joinStreamID(namespace, stream string) string {
	if namespace == "" {
		namespace = defaultSchemaName
	}
	return strings.ToLower(namespace + "." + stream)
}

// pendingStreams returns a list of all tables currently in `tableModeBackfill`,
// in sorted order so that output is deterministic.
func (c *capture) pendingStreams() []string {
	var pending []string
	for id, tableState := range c.state.Streams {
		if tableState.Mode == tableModeBackfill {
			pending = append(pending, id)
		}
	}
	sort.Strings(pending)
	return pending
}

// This is the main loop of the capture process, which interleaves replication event
// streaming with backfill scan results as necessary.
func (c *capture) streamChanges(ctx context.Context) error {
	if c.pendingStreams() != nil {
		var results *backfillResults
		if err := c.writeWatermark(ctx); err != nil {
			return fmt.Errorf("error writing dummy watermark: %w", err)
		}
		for c.pendingStreams() != nil {
			if err := c.streamToWatermark(); err != nil {
				return fmt.Errorf("error streaming until watermark: %w", err)
			} else if err := c.emitBuffered(results); err != nil {
				return fmt.Errorf("error emitting buffered results: %w", err)
			} else if err := c.backfillChunk(ctx); err != nil {
				return fmt.Errorf("error performing backfill: %w", err)
			} else if err := c.writeWatermark(ctx); err != nil {
				return fmt.Errorf("error writing next watermark: %w", err)
			}
		}
	}
	logrus.Debug("all streams fully active")

	// Once there is no more backfilling to do, just stream changes forever and emit
	// state updates on every transaction commit.
	for evt := range c.replStream.Events() {
		if evt.Type == "Commit" {
			if c.changesSinceLastCheckpoint > 0 {
				c.state.CurrentLSN = evt.LSN
				if err := c.emitState(c.state); err != nil {
					return fmt.Errorf("error emitting state update: %w", err)
				}
			}
			continue
		}

		var tableState = c.state.Streams[joinStreamID(evt.Namespace, evt.Table)]
		if tableState != nil && tableState.Mode == tableModeActive {
			if err := c.handleChangeEvent(evt); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
		}
	}
	return nil
}

func (c *capture) writeWatermark(ctx context.Context) error {
	// Generate a watermark UUID
	var wm = uuid.New().String()

	// TODO(wgd): Make this configurable via config.json?
	var table = "public.flow_watermarks"
	// TODO(wgd): Is it a good idea to reuse the replication slot name for this purpose?
	var slot = c.config.SlotName

	rows, err := c.connScan.Query(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot TEXT PRIMARY KEY, watermark TEXT);", table))
	if err != nil {
		return fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()

	var query = fmt.Sprintf(`INSERT INTO %s (slot, watermark) VALUES ($1,$2) ON CONFLICT (slot) DO UPDATE SET watermark = $2;`, table)
	rows, err = c.connScan.Query(ctx, query, slot, wm)
	if err != nil {
		return fmt.Errorf("error upserting new watermark for slot %q: %w", slot, err)
	}
	rows.Close()

	logrus.WithField("watermark", wm).Info("wrote watermark")
	c.watermark = wm
	return nil
}

type backfillResults struct {
	streams map[string]*backfillChunk
}

type backfillChunk struct {
	scanKey  []string
	scanned  []byte
	complete bool // When true, indicates that this chunk *completes* the table, and thus has no precise endpoint
	rows     map[string]*changeEvent
}

func (r *backfillResults) Complete(streamID string) bool {
	if r == nil {
		return false
	}
	return r.streams[streamID].complete
}

func (r *backfillResults) Scanned(streamID string) []byte {
	if r == nil {
		return nil
	}
	return r.streams[streamID].scanned
}

func (r *backfillResults) Patch(evt *changeEvent) error {
	if r == nil {
		return nil
	}
	var streamID = joinStreamID(evt.Namespace, evt.Table)
	var chunk, ok = r.streams[streamID]
	if !ok {
		return nil
	}

	var bs, err = encodeRowKey(chunk.scanKey, evt.Fields)
	if err != nil {
		return fmt.Errorf("error encoding patch key: %w", err)
	}
	var rowKey = string(bs)

	// Apply the new change event to the buffered result set. Note that it's
	// entirely possible to see a row inserted that already exists, or deleted
	// when it doesn't exist. In fact resolving such situations is entirely
	// the point of buffering and patching the results in the first place.
	switch evt.Type {
	case "Insert":
		chunk.rows[rowKey] = evt
	case "Update":
		chunk.rows[rowKey] = &changeEvent{
			Type:      "Insert",
			XID:       evt.XID,
			LSN:       evt.LSN,
			Namespace: evt.Namespace,
			Table:     evt.Table,
			Fields:    evt.Fields,
		}
	case "Delete":
		delete(chunk.rows, rowKey)
	default:
		return fmt.Errorf("patched invalid change type %q", evt.Type)
	}
	return nil
}

func (r *backfillResults) Changes(streamID string) []*changeEvent {
	if r == nil {
		return nil
	}

	// Sort row keys in order so that rows within a chunk will be
	// emitted deterministically. This only really matters for testing,
	// and it's in a relatively hot path here, so this might be low-
	// hanging fruit for optimization if profiling suggests it's needed.
	var keys []string
	for key := range r.streams[streamID].rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Make a list of events in the sorted order
	var evts []*changeEvent
	for _, key := range keys {
		evts = append(evts, r.streams[streamID].rows[key])
	}
	return evts
}

func (c *capture) streamToWatermark() error {
	var watermarkReached = false
	for evt := range c.replStream.Events() {
		// Stop processing replication events upon the watermark mutation's *Commit Event*
		if evt.Type == "Commit" && watermarkReached {
			c.state.CurrentLSN = evt.LSN
			return nil
		}
		// Ignore all other commits
		if evt.Type == "Commit" {
			continue
		}
		// Note when the expected watermark is finally observed
		var streamID = joinStreamID(evt.Namespace, evt.Table)
		// TODO(wgd): Make the watermarks table configurable
		if streamID == "public.flow_watermarks" {
			if evt.Fields["watermark"] == c.watermark {
				logrus.WithField("watermark", c.watermark).Debug("reached watermark")
				watermarkReached = true
			} else {
				logrus.WithFields(logrus.Fields{"current": evt.Fields["watermark"], "awaiting": c.watermark}).Warn("skipping unexpected watermark")
			}
		}

		// For each event we must decide whether to emit it, ignore it, or patch it
		// into a buffered result set. We begin by handling the easy cases of a table
		// which is ignored or fully active, and then follow that up with the range
		// comparisons that decide on a per-row basis during backfills.
		var tableState = c.state.Streams[streamID]
		if tableState == nil || tableState.Mode == tableModeIgnore {
			logrus.WithField("table", streamID).Debug("ignoring change from inactive table")
			c.emitLog(fmt.Sprintf("ignoring %q event: %v", evt.Type, evt.Fields))
			continue
		}
		if tableState.Mode == tableModeActive {
			logrus.WithField("table", streamID).Debug("emitting every change")
			if err := c.handleChangeEvent(evt); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
			continue
		}
		if tableState.Mode != tableModeBackfill {
			return fmt.Errorf("table %q in invalid mode %q", streamID, tableState.Mode)
		}

		var rowKey, err = encodeRowKey(tableState.ScanKey, evt.Fields)
		if err != nil {
			return fmt.Errorf("error encoding row key: %w", err)
		}

		if compareTuples(rowKey, tableState.Scanned) <= 0 {
			logrus.WithField("table", streamID).WithField("key", base64.StdEncoding.EncodeToString(rowKey)).Debug("emitting change")
			if err := c.handleChangeEvent(evt); err != nil {
				return fmt.Errorf("error handling replication event: %w", err)
			}
		} else if c.results.Complete(streamID) || compareTuples(rowKey, c.results.Scanned(streamID)) <= 0 {
			logrus.WithField("table", streamID).WithField("key", base64.StdEncoding.EncodeToString(rowKey)).Debug("patching change")
			if err := c.results.Patch(evt); err != nil {
				return fmt.Errorf("error patching buffered results: %w", err)
			}
			c.emitLog(fmt.Sprintf("patching %q event: %v", evt.Type, evt.Fields))
		} else {
			c.emitLog(fmt.Sprintf("filtered %q event (rowKey=%q, scanned=%q, nextScanned=%q): %v", evt.Type, rowKey, tableState.Scanned, c.results.Scanned(streamID), evt.Fields))
			logrus.WithField("table", streamID).WithField("key", base64.StdEncoding.EncodeToString(rowKey)).Debug("filtered change")
		}
	}
	return nil
}

func (c *capture) emitBuffered(results *backfillResults) error {
	// If there are buffered results (which is not the case during the initial
	// "dummy watermark" step which allows us to "catch up" to any new replication
	// events occurring while the connector was offline), emit them and also
	// update the table state accordingly.
	if c.results != nil {
		for _, streamID := range c.pendingStreams() {
			var evts = c.results.Changes(streamID)
			for _, evt := range evts {
				if err := c.handleChangeEvent(evt); err != nil {
					return fmt.Errorf("error handling backfill change: %w", err)
				}
			}

			if c.results.Complete(streamID) {
				c.state.Streams[streamID].Mode = tableModeActive
				c.state.Streams[streamID].Scanned = nil
			} else {
				c.state.Streams[streamID].Scanned = c.results.Scanned(streamID)
			}
		}
	}

	// Emit a new state update. The global `CurrentLSN` has been advanced by the
	// watermark commit event, and the individual stream `Scanned` tracking for
	// each stream has been advanced just above.
	return c.emitState(c.state)
}

func (c *capture) backfillChunk(ctx context.Context) error {
	var snapshot, err = snapshotDatabase(ctx, c.connScan)
	if err != nil {
		return fmt.Errorf("error creating database snapshot: %w", err)
	}
	defer snapshot.Close(ctx)

	// TODO(wgd): Add a sanity-check assertion that the current watermark value
	// in the database matches the one we expect in the connector. It's unlikely,
	// but if this is a thing that can actually happen in a given database we
	// need to know about it.

	var results = &backfillResults{streams: make(map[string]*backfillChunk)}
	for _, streamID := range c.pendingStreams() {
		var chunk = &backfillChunk{
			scanKey: c.state.Streams[streamID].ScanKey,
			scanned: c.state.Streams[streamID].Scanned,
			rows:    make(map[string]*changeEvent),
		}
		results.streams[streamID] = chunk

		// Go actually fetch something
		var table = snapshot.Table(streamID, chunk.scanKey)

		var evts, err = table.ScanChunk(ctx, chunk.scanned)
		if err != nil {
			return fmt.Errorf("error scanning table: %w", err)
		}
		if len(evts) == 0 {
			chunk.scanned = nil
			chunk.complete = true
		}
		for _, evt := range evts {
			c.emitLog(fmt.Sprintf("buffered %q event: %v", evt.Type, evt.Fields))

			var bs, err = encodeRowKey(chunk.scanKey, evt.Fields)
			if err != nil {
				return fmt.Errorf("error encoding row key: %w", err)
			}
			if chunk.scanned != nil && compareTuples(chunk.scanned, bs) >= 0 {
				// It's important for correctness that the ordering of serialized primary keys matches
				// the ordering that PostgreSQL uses. Since the results of `ScanChunk` are ordered by
				// PostgreSQL and we already have to serialize all the keys for backfill result patching,
				// we may as well opportunistically check that invariant here.
				return fmt.Errorf("primary key ordering failure: prev=%q, next=%q", chunk.scanned, bs)
			}
			chunk.rows[string(bs)] = evt
			chunk.scanned = bs
		}
		logrus.WithField("prev", c.state.Streams[streamID].Scanned).WithField("next", chunk.scanned).WithField("table", streamID).Info("backfill chunk")
	}

	c.results = results
	return nil
}

func (c *capture) handleChangeEvent(evt *changeEvent) error {
	// When in non-tailing mode, reset the shutdown watchdog whenever an event happens.
	if c.watchdog != nil {
		var wdtDuration = time.Duration(c.config.PollTimeoutSeconds * float64(time.Second))
		c.watchdog.Reset(wdtDuration)
	}

	evt.Fields["_change_type"] = evt.Type

	for id, val := range evt.Fields {
		var translated, err = translateRecordField(val)
		if err != nil {
			logrus.WithField("val", val).Error("value translation error")
			return fmt.Errorf("error translating field value: %w", err)
		}
		evt.Fields[id] = translated
	}
	return c.emitRecord(evt.Namespace, evt.Table, evt.Fields)
}

// TranslateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodable output format. As a concrete example, the
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

func (c *capture) emitRecord(ns, stream string, data interface{}) error {
	c.changesSinceLastCheckpoint++
	var rawData, err = json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error encoding record data: %w", err)
	}
	return c.encoder.Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: ns,
			Stream:    stream,
			EmittedAt: time.Now().UnixNano() / int64(time.Millisecond),
			Data:      json.RawMessage(rawData),
		},
	})
}

func (c *capture) emitState(state interface{}) error {
	c.changesSinceLastCheckpoint = 0
	var rawState, err = json.Marshal(state)
	if err != nil {
		return fmt.Errorf("error encoding state message: %w", err)
	}
	return c.encoder.Encode(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(rawState)},
	})
}

func (c *capture) emitLog(msg interface{}) error {
	var str string
	switch msg := msg.(type) {
	case string:
		str = msg
	default:
		var bs, err = json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshalling log message: %w", err)
		}
		str = string(bs)
	}

	return c.encoder.Encode(airbyte.Message{
		Type: airbyte.MessageTypeLog,
		Log: &airbyte.Log{
			Level:   airbyte.LogLevelDebug,
			Message: str,
		},
	})
}
