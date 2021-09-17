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
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	DefaultSchemaName = "public"
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
		duration := time.Duration(c.Config.MaxLifespanSeconds * float64(time.Second))
		logrus.WithField("duration", duration).Info("limiting connector lifespan")
		limitedCtx, cancel := context.WithTimeout(ctx, duration)
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
	if errors.Is(err, context.DeadlineExceeded) && c.Config.MaxLifespanSeconds != 0 {
		logrus.WithField("err", err).WithField("maxLifespan", c.Config.MaxLifespanSeconds).Info("maximum lifespan reached")
		return nil
	}
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
		streamID := joinStreamID(catalogStream.Stream.Namespace, catalogStream.Stream.Name)

		// In the catalog a primary key is an array of arrays of strings, but it's
		// invalid for the individual key elements to not be unit-length (["foo"])
		var catalogPrimaryKey []string
		for _, col := range catalogStream.PrimaryKey {
			if len(col) != 1 {
				return errors.Errorf("stream %q: primary key element %q invalid", streamID, col)
			}
			catalogPrimaryKey = append(catalogPrimaryKey, col[0])
		}

		// If the `PrimaryKey` property is specified in the catalog then use that,
		// otherwise use the "native" primary key of this table in the database.
		// Print a warning if the two are not the same.
		primaryKey := dbPrimaryKeys[streamID]
		if len(primaryKey) != 0 {
			logrus.WithField("table", streamID).WithField("key", primaryKey).Debug("queried primary key")
		}
		if len(catalogPrimaryKey) > 0 {
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
			return errors.Errorf("stream %q: primary key unspecified in the catalog and no primary key found in database", streamID)
		}

		// See if the stream is already initialized. If it's not, then create it.
		streamState, ok := c.State.Streams[streamID]
		if !ok {
			c.State.Streams[streamID] = &TableState{Mode: ScanStateScanning, ScanKey: primaryKey}
			stateDirty = true
			continue
		}

		if strings.Join(streamState.ScanKey, ",") != strings.Join(primaryKey, ",") {
			return errors.Errorf("stream %q: primary key %q doesn't match initialized scan key %q", streamID, primaryKey, streamState.ScanKey)
		}
	}

	// Likewise streams may be removed from the catalog, and we need to forget
	// the corresponding state information.
	for streamID := range c.State.Streams {
		// List membership checks are always a pain in Go, but that's all this loop is
		streamExistsInCatalog := false
		for _, catalogStream := range c.Catalog.Streams {
			catalogStreamID := joinStreamID(catalogStream.Stream.Namespace, catalogStream.Stream.Name)
			if streamID == catalogStreamID {
				streamExistsInCatalog = true
			}
		}

		if !streamExistsInCatalog {
			logrus.WithField("stream", streamID).Info("stream removed from catalog")
			delete(c.State.Streams, streamID)
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

// joinStreamID combines a namespace and a stream name into a fully-qualified
// stream (or table) identifier. Because it's possible for the namespace to be
// unspecified, we default to "public" in that situation.
func joinStreamID(namespace, stream string) string {
	if namespace == "" {
		namespace = DefaultSchemaName
	}
	return strings.ToLower(namespace + "." + stream)
}

// Whenever a stream is added to the catalog (including on first launch)
// we have to scan the full contents of the table in the database and emit each
// row as an "Insert" event. Because database tables can grow pretty huge, we do
// this in chunks, emitting state updates periodically and accepting that this
// connector might get killed and restarted from an intermediate state.
func (c *Capture) ScanTables(ctx context.Context) error {
	// Skip unnecessary setup work if there's no scan work pending anyway.
	pendingStreamIDs := c.PendingScans()
	if len(pendingStreamIDs) == 0 {
		return nil
	}

	// Take a snapshot of the database contents
	snapshot, err := SnapshotDatabase(ctx, c.connScan)
	if err != nil {
		return errors.Wrap(err, "error creating database snapshot")
	}
	if snapshot.TransactionLSN() < c.State.CurrentLSN {
		return errors.Errorf("table snapshot (LSN=%q) is older than the current replication LSN (%q)", snapshot.TransactionLSN(), c.State.CurrentLSN)
	}
	defer snapshot.Close(ctx)

	// Scan each pending table within this snapshot
	logrus.WithField("tables", c.PendingScans()).WithField("scanLSN", snapshot.TransactionLSN()).Info("scanning tables")
	for _, pendingStreamID := range pendingStreamIDs {
		if err := c.ScanTable(ctx, snapshot, pendingStreamID); err != nil {
			return err
		}
	}
	return nil
}

// PendingScans returns a list of all tables which still need scanning,
// in sorted order. The sorting is important for test stability, because
// when there are multiple tables in the "Scanning" state we need to
// process them in a consistent order.
func (c *Capture) PendingScans() []string {
	var pending []string
	for id, tableState := range c.State.Streams {
		if tableState.Mode == ScanStateScanning {
			pending = append(pending, id)
		}
	}
	sort.Strings(pending)
	return pending
}

// ScanTable scans a particular table to completion, emitting intermediate
// state updates along the way. If the connector is resumed from a partial
// state, scanning resumes from where it left off.
func (c *Capture) ScanTable(ctx context.Context, snapshot *DatabaseSnapshot, streamID string) error {
	tableState := c.State.Streams[streamID]
	if tableState.Mode != ScanStateScanning {
		return errors.Errorf("stream %q in state %q (expected %q)", streamID, tableState.Mode, ScanStateScanning)
	}

	table := snapshot.Table(streamID, tableState.ScanKey)

	// The table scanning process is implemented as a loop which keeps doing
	// the next chunk of work until it's all done. This way we don't have to
	// worry too much about how resuming works across connector restarts,
	// because each iteration of this loop is basically independent except
	// for the stream state it reads and modifies.
	for tableState.Mode == ScanStateScanning {
		// If this stream has no previously scanned range information, request the first
		// chunk, then initialize the scanned ranges list and emit a state update.
		if len(tableState.ScanRanges) == 0 {
			count, endKey, err := table.ScanStart(ctx, c.HandleChangeEvent)
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
			if err := c.EmitState(c.State); err != nil {
				return err
			}
			continue
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
		count, nextKey, err := table.ScanFrom(ctx, lastRange.EndKey, c.HandleChangeEvent)
		if err != nil {
			return errors.Wrap(err, "error processing snapshot events")
		}
		if count == 0 {
			tableState.Mode = ScanStateActive
		} else {
			lastRange.EndKey = nextKey
		}
		if err := c.EmitState(c.State); err != nil {
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

	streamID := joinStreamID(evt.Namespace, evt.Table)
	tableState := c.State.Streams[streamID]
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
	evt.Fields["_change_type"] = evt.Type
	evt.Fields["_change_lsn"] = evt.LSN
	evt.Fields["_ingested_at"] = time.Now().Unix()

	for id, val := range evt.Fields {
		translated, err := TranslateRecordField(val)
		if err != nil {
			logrus.WithField("val", val).Error("value translation error")
			return errors.Wrap(err, "error translating field value")
		}
		evt.Fields[id] = translated
	}
	return c.EmitRecord(evt.Namespace, evt.Table, evt.Fields)
}

// TranslateRecordField "translates" a value from the PostgreSQL driver into
// an appropriate JSON-encodable output format. As a concrete example, the
// PostgreSQL `cidr` type becomes a `*net.IPNet`, but the default JSON
// marshalling of a `net.IPNet` isn't a great fit and we'd prefer to use
// the `String()` method to get the usual "192.168.100.0/24" notation.
func TranslateRecordField(val interface{}) (interface{}, error) {
	switch x := val.(type) {
	case *net.IPNet:
		return x.String(), nil
	case net.HardwareAddr:
		return x.String(), nil
	case [16]uint8: // UUIDs
		s := new(strings.Builder)
		for i := range x {
			if i == 4 || i == 6 || i == 8 || i == 10 {
				s.WriteString("-")
			}
			fmt.Fprintf(s, "%02x", x[i])
		}
		return s.String(), nil
	}
	if enc, ok := val.(pgtype.TextEncoder); ok {
		bs, err := enc.EncodeText(nil, nil)
		if err != nil {
			return nil, err
		}
		return string(bs), nil
	}
	return val, nil
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
