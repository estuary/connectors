package main

import (
	"context"
	"encoding/json"
	"errors"
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

// TableState represents the serializable/resumable state of a particular table's capture.
// It is mostly concerned with the "backfill" scanning process and the transition from that
// to logical replication.
type TableState struct {
	// Mode is either TableModeScanning during the backfill scanning process
	// or TableModeActive once replication resumes.
	Mode string `json:"mode"`
	// ScanKey is the "primary key" used for ordering/chunking the backfill scan.
	ScanKey []string `json:"scan_key"`
	// ScanRanges represents the portion of the table which has been scanned, and
	// at what point in time each chunk was scanned.
	ScanRanges []TableRange `json:"scan_ranges,omitempty"`
}

const (
	tableModeScanning = "Scanning"
	tableModeActive   = "Active"
)

// TableRange represents a specific chunk of some table's rows, which implicitly begins after the
// previous TableRange (see TableState) and continues up to EndKey (inclusive).
type TableRange struct {
	// ScannedLSN represents the point in time at which this chunk of the table was scanned,
	// and is used to make the scanning-to-replication transition seamless.
	ScannedLSN pglogrepl.LSN `json:"lsn"`

	// EndKey contains a FoundationDB-serialized tuple representing the ScanKey column values
	// of the final row in this Table Range. Each TableRange implicitly begins after the previous
	// one in the list (see TableState) and continues up through EndKey (inclusive).
	EndKey []byte `json:"key,omitempty"`
}

// capture encapsulates the entire process of capturing data from PostgreSQL with a particular
// configuration/catalog/state and emitting records and state updates to some messageOutput.
type capture struct {
	state   *PersistentState           // State read from `state.json` and emitted as updates
	config  *Config                    // The configuration read from `config.json`
	catalog *airbyte.ConfiguredCatalog // The catalog read from `catalog.json`
	encoder messageOutput              // The encoder to which records and state updates are written

	connScan *pgx.Conn      // The DB connection used for table scanning
	connRepl *pgconn.PgConn // The DB connection used for replication streaming
	watchdog *time.Timer    // If non-nil, the Reset() method will be invoked whenever a replication event is received

	// We keep a count of change events since the last state checkpoint was
	// emitted. Currently this is only used to suppress "empty" commit messages
	// from triggering a state update (which improves test stability), but
	// this could also be used to "coalesce" smaller transactions in the
	// future.
	changesSinceLastCheckpoint int
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
	if err := c.scanTables(ctx); err != nil {
		return fmt.Errorf("error scanning table contents: %w", err)
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
	// from which we will start replication after all table-scanning work is done.
	// Since change events which precede the table scan will be filtered out, this
	// value doesn't need to be exact so long as it precedes the table scan.
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
			c.state.Streams[streamID] = &TableState{Mode: tableModeScanning, ScanKey: primaryKey}
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

// scanTables uses SELECT queries to emit the initial contents of a table as
// INSERT events prior to replication streaming.
//
// Whenever a table is added to the catalog (including on first launch) we
// have to backfill the preexisting contents before resuming replication. This
// is done by issuing SELECT queries to read all of the data, and emitting each
// result row as an "Insert" event.
//
// Because database tables can grow pretty huge, this scanning is performed in
// chunks, with state checkpoints emitted periodically. The connector may be
// killed and restarted at one of these intermediate states, and needs to be
// able to pick back up where it left off.
func (c *capture) scanTables(ctx context.Context) error {
	// Skip unnecessary setup work if there's no scan work pending anyway.
	var pendingStreamIDs = c.pendingScans()
	if len(pendingStreamIDs) == 0 {
		return nil
	}

	// Take a snapshot of the database contents
	var snapshot, err = snapshotDatabase(ctx, c.connScan)
	if err != nil {
		return fmt.Errorf("error creating database snapshot: %w", err)
	}
	if snapshot.TransactionLSN() < c.state.CurrentLSN {
		return fmt.Errorf("table snapshot (LSN=%q) is older than the current replication LSN (%q)", snapshot.TransactionLSN(), c.state.CurrentLSN)
	}
	defer snapshot.Close(ctx)

	// Scan each pending table within this snapshot
	logrus.WithField("tables", c.pendingScans()).WithField("scanLSN", snapshot.TransactionLSN()).Info("scanning tables")
	for _, pendingStreamID := range pendingStreamIDs {
		if err := c.scanTable(ctx, snapshot, pendingStreamID); err != nil {
			return err
		}
	}
	return nil
}

// pendingScans returns a list of all tables which still need scanning,
// in sorted order. The sorting is important for test stability, because
// when there are multiple tables in the "Scanning" state we need to
// process them in a consistent order.
func (c *capture) pendingScans() []string {
	var pending []string
	for id, tableState := range c.state.Streams {
		if tableState.Mode == tableModeScanning {
			pending = append(pending, id)
		}
	}
	sort.Strings(pending)
	return pending
}

// scanTable scans a particular table to completion, emitting intermediate
// state updates along the way. If the connector is resumed from a partial
// state, scanning resumes from where it left off.
func (c *capture) scanTable(ctx context.Context, snapshot *databaseSnapshot, streamID string) error {
	var tableState = c.state.Streams[streamID]
	if tableState.Mode != tableModeScanning {
		return fmt.Errorf("stream %q in state %q (expected %q)", streamID, tableState.Mode, tableModeScanning)
	}

	var table = snapshot.Table(streamID, tableState.ScanKey)

	// The table scanning process is implemented as a loop which keeps doing
	// the next chunk of work until it's all done. This way we don't have to
	// worry too much about how resuming works across connector restarts,
	// because each iteration of this loop is basically independent except
	// for the stream state it reads and modifies.
	for tableState.Mode == tableModeScanning {
		// Each chunk we scan from the table extends some scan range. When there are
		// no ranges we create an empty placeholder. If the last range has a different
		// `ScannedLSN` from the current snapshot then there's been a restart and we
		// need to open a new range to use.
		var lastRange *TableRange
		if len(tableState.ScanRanges) == 0 {
			tableState.ScanRanges = append(tableState.ScanRanges, TableRange{
				ScannedLSN: snapshot.TransactionLSN(),
			})
		}
		lastRange = &tableState.ScanRanges[len(tableState.ScanRanges)-1]
		if lastRange.ScannedLSN != snapshot.TransactionLSN() {
			tableState.ScanRanges = append(tableState.ScanRanges, TableRange{
				EndKey:     lastRange.EndKey,
				ScannedLSN: snapshot.TransactionLSN(),
			})
			lastRange = &tableState.ScanRanges[len(tableState.ScanRanges)-1]
		}

		// Fetch the next chunk from the table after the end key of the previous one,
		// and extend the range as appropriate.
		var count, endKey, err = table.ScanChunk(ctx, lastRange.EndKey, c.handleChangeEvent)
		if err != nil {
			return fmt.Errorf("error processing snapshot events: %w", err)
		}
		if count == 0 {
			tableState.Mode = tableModeActive
		} else {
			lastRange.EndKey = endKey
		}
		if err := c.emitState(c.state); err != nil {
			return err
		}
	}
	return nil
}

// This is the "payoff" of all the setup work we did in `UpdateState` and `ScanTables`,
// in that once we reach this state we'll sit here indefinitely, awaiting change events
// from PostgreSQL via logical replication and relaying these changes downstream ASAP.
//
// Each time we receive a replication event from PostgreSQL it will be decoded and then
// passed off to `c.HandleReplicationEvent` for further processing.
func (c *capture) streamChanges(ctx context.Context) error {
	// TODO(wgd): Perhaps merge startReplication and process() call?
	var stream, err = startReplication(ctx, c.connRepl, c.config.SlotName, c.config.PublicationName, c.state.CurrentLSN)
	if err != nil {
		return fmt.Errorf("unable to start replication stream: %w", err)
	}
	defer stream.Close(ctx)
	if err := stream.process(ctx, c.handleReplicationEvent); err != nil {
		return fmt.Errorf("error processing replication events: %w", err)
	}
	logrus.Warnf("replication streaming terminated without error")
	return nil
}

func (c *capture) handleReplicationEvent(evt *changeEvent) error {
	// When in non-tailing mode, reset the shutdown watchdog whenever an event happens.
	if c.watchdog != nil {
		var wdtDuration = time.Duration(c.config.PollTimeoutSeconds * float64(time.Second))
		c.watchdog.Reset(wdtDuration)
	}

	// TODO(wgd): There will be a "Commit" event after every transaction, so if the DB
	// receives a bunch of single-row INSERTs we'll emit one state update per row added.
	// Is this too much?
	//
	// TODO(wgd): At some point during replication event processing we should be far
	// enough past the initial table scan that ideally we want to delete all the scan
	// ranges and drop into the fast-path of not doing any range comparison. Is there
	// ever a point where we can definitively say "Yes, we're fully caught up now"?
	if evt.Type == "Commit" {
		logrus.WithFields(logrus.Fields{"lsn": evt.LSN, "xid": evt.XID}).Debug("replication commit event")
		if c.changesSinceLastCheckpoint > 0 {
			c.state.CurrentLSN = evt.LSN
			return c.emitState(c.state)
		}
		return nil
	}

	logrus.WithFields(logrus.Fields{
		"type":      evt.Type,
		"xid":       evt.XID,
		"lsn":       evt.LSN,
		"namespace": evt.Namespace,
		"table":     evt.Table,
		"values":    evt.Fields,
	}).Debug("replication change event")

	var streamID = joinStreamID(evt.Namespace, evt.Table)
	var tableState = c.state.Streams[streamID]
	if tableState == nil {
		// If we're not tracking state by now, this table isn't in the catalog. Ignore it.
		return nil
	}
	if tableState.Mode != tableModeActive {
		return fmt.Errorf("invalid state %#v for table %q", tableState, evt.Table)
	}

	// Decide whether this event should be filtered out based on its LSN.
	var filter, err = c.filteredLSN(tableState, evt.Fields, evt.LSN)
	if err != nil {
		return fmt.Errorf("error in filter check: %w", err)
	}
	if filter {
		logrus.WithFields(logrus.Fields{
			"type":      evt.Type,
			"xid":       evt.XID,
			"lsn":       evt.LSN,
			"namespace": evt.Namespace,
			"table":     evt.Table,
			"values":    evt.Fields,
		}).Debug("filtered change")
		return nil
	}
	return c.handleChangeEvent(evt)
}

func (c *capture) filteredLSN(tableState *TableState, fields map[string]interface{}, lsn pglogrepl.LSN) (bool, error) {
	// If the table is "Active" but has no scan ranges then we won't filter any
	// events. Ideally, once we've gone well past the timeframe of the initial
	// table scan, we can discard all the scan range LSN tracking information
	// and fall into this fast-path.
	if len(tableState.ScanRanges) == 0 {
		return false, nil
	}

	// If there's only one scan range we can skip the entire encoding and
	// key comparison process and just use its `ScannedLSN` for all rows.
	if len(tableState.ScanRanges) > 1 {
		// Otherwise we have to actually implement generic primary key comparison
		var keyElems []interface{}
		for _, keyName := range tableState.ScanKey {
			keyElems = append(keyElems, fields[keyName])
		}
		var keyBytes, err = packTuple(keyElems)
		if err != nil {
			return false, fmt.Errorf("error encoding primary key for comparison: %w", err)
		}
		for _, scanRange := range tableState.ScanRanges {
			if compareTuples(keyBytes, scanRange.EndKey) < 0 {
				var isFiltered = lsn < scanRange.ScannedLSN
				return isFiltered, nil
			}
		}
	}

	// If the key did not fall into any known scan range during the previous
	// bit of code then conceptually it falls into the last range.
	var scannedLSN = tableState.ScanRanges[len(tableState.ScanRanges)-1].ScannedLSN
	var isFiltered = lsn < scannedLSN
	return isFiltered, nil
}

func (c *capture) handleChangeEvent(evt *changeEvent) error {
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
