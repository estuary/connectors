package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	// TODO(wgd): More principled handling of schemas throughout the code. Should
	// it be possible to interact with tables from schemas other than "public"?
	// If so, can we avoid making every stream be named "public.foo" in the
	// simple cases?
	DatabaseSchemaName = "public"
)

type PersistentState struct {
	CurrentLSN pglogrepl.LSN          `json:"current_lsn"`
	Streams    map[string]*TableState `json:"streams"`
}

func (ps *PersistentState) Validate() error {
	return nil
}

type TableState struct {
	Mode       string       `json:"mode"`
	ScanKey    []string     `json:"scan_key"`
	ScanRanges []TableRange `json:"scan_ranges,omitempty"`
}

const ScanStateScanning = "Scanning"
const ScanStateActive = "Active"

type TableRange struct {
	ScannedLSN pglogrepl.LSN `json:"lsn"`
	EndKey     []byte        `json:"key,omitempty"`
}

type Capture struct {
	State   *PersistentState           // State read from `state.json` and emitted as updates
	Config  *Config                    // The configuration read from `config.json`
	Catalog *airbyte.ConfiguredCatalog // The catalog read from `catalog.json`

	encoder  MessageOutput  // The encoder to which records and state updates are written
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

type MessageOutput interface {
	Encode(v interface{}) error
}

func NewCapture(ctx context.Context, config *Config, catalog *airbyte.ConfiguredCatalog, state *PersistentState, dest MessageOutput) (*Capture, error) {
	// Normal database connection used for table scanning
	connScan, err := pgx.Connect(ctx, config.ConnectionURI)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to database for table scan")
	}

	// Replication database connection used for event streaming
	replConnConfig, err := pgconn.ParseConfig(config.ConnectionURI)
	if err != nil {
		return nil, err
	}
	replConnConfig.RuntimeParams["replication"] = "database"
	connRepl, err := pgconn.ConnectConfig(ctx, replConnConfig)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to database for replication")
	}

	return &Capture{
		State:    state,
		Config:   config,
		Catalog:  catalog,
		encoder:  dest,
		connScan: connScan,
		connRepl: connRepl,
	}, nil
}

func (c *Capture) Execute(ctx context.Context) error {
	if c.Config.MaxLifespanSeconds != 0 {
		limitedCtx, cancel := context.WithTimeout(ctx, time.Duration(c.Config.MaxLifespanSeconds*float64(time.Second)))
		defer cancel()
		ctx = limitedCtx
	}

	if err := c.UpdateState(ctx); err != nil {
		return errors.Wrap(err, "error updating capture state")
	}
	if err := c.ScanTables(ctx); err != nil {
		return errors.Wrap(err, "error scanning table contents")
	}

	// In non-tailing mode (which should only occur during development) we need
	// to shut down after no further changes have been reported for a while. To
	// do this we create a cancellable context, and a watchdog timer which will
	// perform said cancellation if `PollTimeout` elapses between resets.
	if !c.Catalog.Tail && c.Config.PollTimeoutSeconds != 0 {
		streamCtx, streamCancel := context.WithCancel(ctx)
		defer streamCancel()
		wdtDuration := time.Duration(c.Config.PollTimeoutSeconds * float64(time.Second))
		c.watchdog = time.AfterFunc(wdtDuration, streamCancel)
		ctx = streamCtx
	}

	err := c.StreamChanges(ctx)
	if errors.Is(err, context.Canceled) && !c.Catalog.Tail {
		return nil
	}
	return err
}

func (c *Capture) UpdateState(ctx context.Context) error {
	stateDirty := false

	// Create the Streams map if nil
	if c.State.Streams == nil {
		c.State.Streams = make(map[string]*TableState)
		stateDirty = true
	}

	// The first time the connector is run, we need to figure out the "Current LSN"
	// from which we will start replication after all table-scanning work is done.
	// Since change events which precede the table scan will be filtered out, this
	// value doesn't need to be exact so long as it precedes the table scan.
	if c.State.CurrentLSN == 0 {
		sysident, err := pglogrepl.IdentifySystem(ctx, c.connRepl)
		if err != nil {
			return errors.Wrap(err, "unable to get current LSN from database")
		}
		c.State.CurrentLSN = sysident.XLogPos
		stateDirty = true
	}

	// Streams may be added to the catalog at various times. We need to
	// initialize new state entries for these streams, and while we're at
	// it this is a good time to sanity-check the primary key configuration.
	dbPrimaryKeys, err := GetPrimaryKeys(ctx, c.connScan)
	if err != nil {
		return errors.Wrap(err, "error querying database about primary keys")
	}

	for _, catalogStream := range c.Catalog.Streams {
		// Table names coming from Postgres are always lowercase, so if we
		// normalize the stream name to lowercase on the catalog->state
		// transition then we can ignore the issue later on.
		streamName := strings.ToLower(catalogStream.Stream.Name)

		// In the catalog a primary key is an array of arrays of strings, but it's
		// invalid for the individual key elements to not be unit-length (["foo"])
		var catalogPrimaryKey []string
		for _, col := range catalogStream.PrimaryKey {
			if len(col) != 1 {
				return errors.Errorf("stream %q: primary key element %q invalid", streamName, col)
			}
			catalogPrimaryKey = append(catalogPrimaryKey, col[0])
		}

		// If the `PrimaryKey` property is specified in the catalog then use that,
		// otherwise use the "native" primary key of this table in the database.
		// Print a warning if the two are not the same.
		dbTableID := strings.ToLower(DatabaseSchemaName + "." + streamName)
		primaryKey := dbPrimaryKeys[dbTableID]
		if len(primaryKey) != 0 {
			logrus.WithField("table", dbTableID).WithField("key", primaryKey).Debug("queried primary key")
		}
		if len(catalogPrimaryKey) > 0 {
			if strings.Join(primaryKey, ",") != strings.Join(catalogPrimaryKey, ",") {
				logrus.WithFields(logrus.Fields{
					"stream":      streamName,
					"catalogKey":  catalogPrimaryKey,
					"databaseKey": primaryKey,
				}).Warn("primary key in catalog differs from database table")
			}
			primaryKey = catalogPrimaryKey
		}
		if len(primaryKey) == 0 {
			return errors.Errorf("stream %q: primary key unspecified in the catalog and no primary key found in database", streamName)
		}

		// See if the stream is already initialized. If it's not, then create it.
		streamState, ok := c.State.Streams[streamName]
		if !ok {
			c.State.Streams[streamName] = &TableState{Mode: ScanStateScanning, ScanKey: primaryKey}
			stateDirty = true
			continue
		}

		if strings.Join(streamState.ScanKey, ",") != strings.Join(primaryKey, ",") {
			return errors.Errorf("stream %q: primary key %q doesn't match initialized scan key %q", streamName, primaryKey, streamState.ScanKey)
		}
	}

	// Likewise streams may be removed from the catalog, and we need to forget
	// the corresponding state information.
	for streamName := range c.State.Streams {
		// List membership checks are always a pain in Go, but that's all this loop is
		streamExistsInCatalog := false
		for _, catalogStream := range c.Catalog.Streams {
			if streamName == strings.ToLower(catalogStream.Stream.Name) {
				streamExistsInCatalog = true
			}
		}

		if !streamExistsInCatalog {
			logrus.WithField("stream", streamName).Info("stream removed from catalog")
			delete(c.State.Streams, streamName)
			stateDirty = true
		}
	}

	// If we've altered the state, emit it to stdout. This isn't strictly necessary
	// but it helps to make the emitted sequence of state updates a lot more readable.
	if stateDirty {
		c.EmitState(c.State)
	}
	return nil
}

// Whenever a stream is added to the catalog (including on first launch)
// we have to scan the full contents of the table in the database and emit each
// row as an "Insert" event. Because database tables can grow pretty huge, we do
// this in chunks, emitting state updates periodically and accepting that this
// connector might get killed and restarted from an intermediate state.
func (c *Capture) ScanTables(ctx context.Context) error {
	// Skip unnecessary setup work if there's no scan work pending anyway.
	if !c.PendingScans() {
		return nil
	}

	snapshot, err := SnapshotTable(ctx, c.connScan)
	if err != nil {
		return errors.Wrap(err, "error creating table snapshot stream")
	}
	if snapshot.TransactionLSN() < c.State.CurrentLSN {
		return errors.Errorf("table snapshot (LSN=%q) is older than the current replication LSN (%q)", snapshot.TransactionLSN(), c.State.CurrentLSN)
	}
	defer snapshot.Close(ctx)
	for c.PendingScans() {
		if err := c.ScanNext(ctx, snapshot); err != nil {
			return err
		}
	}
	return nil
}

func (c *Capture) PendingScans() bool {
	for _, tableState := range c.State.Streams {
		if tableState.Mode == ScanStateScanning {
			return true
		}
	}
	return false
}

func (c *Capture) ScanNext(ctx context.Context, snapshot *TableSnapshotStream) error {
	// Find the first stream which is still in the "Scanning" state.
	for streamName, tableState := range c.State.Streams {
		if tableState.Mode != "Scanning" {
			continue
		}

		// If this stream has no previously scanned range information, request the first
		// chunk, then initialize the scanned ranges list and emit a state update.
		if len(tableState.ScanRanges) == 0 {
			// TODO(wgd): Fix handling of multiple Schemas
			count, endKey, err := snapshot.ScanStart(ctx, DatabaseSchemaName, streamName, tableState.ScanKey, c.HandleChangeEvent)
			if err != nil {
				return errors.Wrap(err, "error processing snapshot events")
			}
			tableState.ScanRanges = []TableRange{{
				ScannedLSN: snapshot.TransactionLSN(),
				EndKey:     endKey,
			}}

			// If the initial scan returned zero rows then we proceed immediately
			// to 'Active' for this table. Note that in this case the 'EndKey' of
			// the range will be "", but that's okay because the `filteredLSN`
			// check isn't going to examine the key anyway since there's just one
			// scan range.
			if count == 0 {
				tableState.Mode = ScanStateActive
			}
			return c.EmitState(c.State)
		}

		// Otherwise if the last scanned range had a different LSN than the current snapshot,
		// there must have been a restart in between. Create a new scan range, initialized
		// to the zero-length range after the previous range.
		//
		// Whether or not this is the case, `lastRange` will end up referring to some range
		// with the same `ScannedLSN` as our current transaction, which will be extended by
		// reading further chunks from the table.
		lastRange := &tableState.ScanRanges[len(tableState.ScanRanges)-1]
		if lastRange.ScannedLSN != snapshot.TransactionLSN() {
			tableState.ScanRanges = append(tableState.ScanRanges, TableRange{
				ScannedLSN: snapshot.TransactionLSN(),
				EndKey:     lastRange.EndKey,
			})
			lastRange = &tableState.ScanRanges[len(tableState.ScanRanges)-1]
		}

		// This is the core operation where we spend most of our time when scanning
		// table contents: reading the next chunk from the table and extending the
		// endpoint of `lastRange`.
		// TODO(wgd): Fix handling of multiple Schemas
		count, nextKey, err := snapshot.ScanFrom(ctx, DatabaseSchemaName, streamName, tableState.ScanKey, lastRange.EndKey, c.HandleChangeEvent)
		if err != nil {
			return errors.Wrap(err, "error processing snapshot events")
		}
		if count == 0 {
			tableState.Mode = ScanStateActive
		} else {
			lastRange.EndKey = nextKey
		}
		return c.EmitState(c.State)
	}
	return errors.New("no scans pending")
}

// This is the "payoff" of all the setup work we did in `UpdateState` and `ScanTables`,
// in that once we reach this state we'll sit here indefinitely, awaiting change events
// from PostgreSQL via logical replication and relaying these changes downstream ASAP.
//
// Each time we receive a replication event from PostgreSQL it will be decoded and then
// passed off to `c.HandleReplicationEvent` for further processing.
func (c *Capture) StreamChanges(ctx context.Context) error {
	stream, err := StartReplication(ctx, c.connRepl, c.Config.SlotName, c.Config.PublicationName, c.State.CurrentLSN)
	if err != nil {
		return errors.Wrap(err, "unable to start replication stream")
	}
	defer stream.Close(ctx)
	if err := stream.Process(ctx, c.HandleReplicationEvent); err != nil {
		return errors.Wrap(err, "error processing replication events")
	}
	logrus.Warnf("replication streaming terminated without error")
	return nil
}

func (c *Capture) HandleReplicationEvent(evt *ChangeEvent) error {
	// When in non-tailing mode, reset the shutdown watchdog whenever an event happens.
	if c.watchdog != nil {
		wdtDuration := time.Duration(c.Config.PollTimeoutSeconds * float64(time.Second))
		c.watchdog.Reset(wdtDuration)
	}

	// TODO(wgd): The only thing the 'Commit' event is used for is to signal that
	// we could emit a state update. Can this be done better?
	//
	// TODO(wgd): There will be a "Commit" event after every transaction, so if there
	// are a bunch of independent single-row INSERTs we could end up emitting one state
	// output per row added, which is too much. Add some rate-limiting to EmitState.
	//
	// TODO(wgd): At some point during replication event processing we should be far
	// enough past the initial table scan that ideally we want to delete all the scan
	// ranges and drop into the fast-path of not doing any range comparison. Is there
	// ever a point where we can definitively say "Yes, we're fully caught up now"?
	if evt.Type == "Commit" {
		logrus.WithFields(logrus.Fields{"lsn": evt.LSN, "xid": evt.XID}).Debug("replication commit event")
		if c.changesSinceLastCheckpoint > 0 {
			c.State.CurrentLSN = evt.LSN
			return c.EmitState(c.State)
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

	tableState := c.State.Streams[evt.Table]
	if tableState == nil {
		// If we're not tracking state by now, this table isn't in the catalog. Ignore it.
		return nil
	}
	if tableState.Mode != ScanStateActive {
		return errors.Errorf("invalid state %#v for table %q", tableState, evt.Table)
	}

	// Decide whether this event should be filtered out based on its LSN.
	filter, err := c.filteredLSN(tableState, evt.Fields, evt.LSN)
	if err != nil {
		return errors.Wrap(err, "error in filter check")
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
	return c.HandleChangeEvent(evt)
}

func (c *Capture) filteredLSN(tableState *TableState, fields map[string]interface{}, lsn pglogrepl.LSN) (bool, error) {
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
		keyBytes, err := packTuple(keyElems)
		if err != nil {
			return false, errors.Wrap(err, "error encoding primary key for comparison")
		}
		for _, scanRange := range tableState.ScanRanges {
			if compareTuples(keyBytes, scanRange.EndKey) < 0 {
				isFiltered := lsn < scanRange.ScannedLSN
				return isFiltered, nil
			}
		}
	}

	// If the key did not fall into any known scan range during the previous
	// bit of code then conceptually it falls into the last range.
	scannedLSN := tableState.ScanRanges[len(tableState.ScanRanges)-1].ScannedLSN
	isFiltered := lsn < scannedLSN
	return isFiltered, nil
}

func (c *Capture) HandleChangeEvent(evt *ChangeEvent) error {
	// TODO(wgd): Should these field names and values aim for exact
	// compatibility with Airbyte Postgres CDC operation?
	evt.Fields["_change_type"] = evt.Type
	evt.Fields["_change_lsn"] = evt.LSN
	evt.Fields["_ingested_at"] = time.Now().Unix()
	return c.EmitRecord(evt.Namespace, evt.Table, evt.Fields)
}

func (c *Capture) EmitRecord(ns, stream string, data interface{}) error {
	c.changesSinceLastCheckpoint++
	rawData, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "error encoding record data")
	}
	return c.encoder.Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: ns,
			Stream:    stream,
			EmittedAt: time.Now().Unix(),
			Data:      json.RawMessage(rawData),
		},
	})
}

func (c *Capture) EmitState(state interface{}) error {
	c.changesSinceLastCheckpoint = 0
	rawState, err := json.Marshal(state)
	if err != nil {
		return errors.Wrap(err, "error encoding state message")
	}
	return c.encoder.Encode(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(rawState)},
	})
}
