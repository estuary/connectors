package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/estuary/protocols/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

const (
	// Emit a log message to stderr for each event which is filtered out during
	// replication due to an LSN which indicates that it was already observed
	// during the initial table scan.
	LogFilteredEvents = true

	// Guarantee that the connector will not run longer than 48 hours without a
	// restart. Since advancing the PostgreSQL replication slot `restart_lsn` only
	// happens on restart (see comment in `replication.go` for explanation), this
	// connector requires occasional restarts to allow PostgreSQL to free older WAL
	// segments.
	PoisonPillDuration = 48 * time.Hour
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

type Config struct {
	ConnectionURI   string `json:"connectionURI"`
	SlotName        string `json:"slot_name"`
	PublicationName string `json:"publication_name"`
}

func (c *Config) Validate() error {
	if c.ConnectionURI == "" {
		return errors.New("Database Connection URI must be set")
	}
	if c.SlotName == "" {
		c.SlotName = "flow_slot"
	}
	if c.PublicationName == "" {
		c.PublicationName = "flow_publication"
	}
	return nil
}

const configSchema = `{
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title":   "Postgres Source Spec",
	"type":    "object",
	"properties": {
		"connectionURI": {
			"type":        "string",
			"title":       "Database Connection URI",
			"description": "Connection parameters, as a libpq-compatible connection string",
			"default":     "postgres://flow:flow@localhost:5432/flow"
		},
		"slot_name": {
			"type":        "string",
			"title":       "Replication Slot Name",
			"description": "The name of the PostgreSQL replication slot to replicate from",
			"default":     "flow_slot"
		},
		"publication_name": {
			"type":        "string",
			"title":       "Publication Name",
			"description": "The name of the PostgreSQL publication to replicate from",
			"default":     "flow_publication"
		}
	},
	"required": [ "connectionURI" ]
}`

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
	EndKey     []byte        `json:"key"`
}

func doRead(args airbyte.ReadCmd) error {
	ctx := context.Background()
	if PoisonPillDuration > 0 {
		limitedCtx, cancel := context.WithTimeout(ctx, PoisonPillDuration)
		defer cancel()
		ctx = limitedCtx
	}

	state := &PersistentState{Streams: make(map[string]*TableState)}
	if args.StateFile != "" {
		if err := args.StateFile.Parse(state); err != nil {
			return errors.Wrap(err, "unable to parse state file")
		}
	}

	config := new(Config)
	if err := args.ConfigFile.Parse(config); err != nil {
		return err
	}

	catalog := new(airbyte.ConfiguredCatalog)
	if err := args.CatalogFile.Parse(catalog); err != nil {
		return errors.Wrap(err, "unable to parse catalog")
	}

	capture, err := NewCapture(ctx, state, config, catalog)
	if err != nil {
		return err
	}
	if err := capture.UpdateState(ctx); err != nil {
		return errors.Wrap(err, "error updating capture state")
	}
	if err := capture.ScanTables(ctx); err != nil {
		return errors.Wrap(err, "error scanning table contents")
	}
	if err := capture.StreamChanges(ctx); err != nil {
		return errors.Wrap(err, "error streaming replication events")
	}
	return errors.New("read process should never terminate")
}

type Capture struct {
	state    *PersistentState           // State read from `state.json` and emitted as updates
	config   *Config                    // The configuration read from `config.json`
	catalog  *airbyte.ConfiguredCatalog // The catalog read from `catalog.json`
	output   *MessageOutput             // A JSON-encoding wrapper around stdout
	connScan *pgx.Conn                  // The DB connection used for table scanning
	connRepl *pgconn.PgConn             // The DB connection used for replication streaming
}

func NewCapture(ctx context.Context, state *PersistentState, config *Config, catalog *airbyte.ConfiguredCatalog) (*Capture, error) {
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
		state:    state,
		config:   config,
		catalog:  catalog,
		output:   NewMessageOutput(),
		connScan: connScan,
		connRepl: connRepl,
	}, nil
}

func (c *Capture) UpdateState(ctx context.Context) error {
	stateDirty := false

	// The first time the connector is run, we need to figure out the "Current LSN"
	// from which we will start replication after all table-scanning work is done.
	// Since change events which precede the table scan will be filtered out, this
	// value doesn't need to be exact so long as it precedes the table scan.
	if c.state.CurrentLSN == 0 {
		sysident, err := pglogrepl.IdentifySystem(ctx, c.connRepl)
		if err != nil {
			return errors.Wrap(err, "unable to get current LSN from database")
		}
		c.state.CurrentLSN = sysident.XLogPos
		stateDirty = true
	}

	// Streams may be added to the catalog at various times. We need to
	// initialize new state entries for these streams, and while we're at
	// it this is a good time to sanity-check the primary key configuration.
	for _, catalogStream := range c.catalog.Streams {
		streamName := catalogStream.Stream.Name
		streamState, stateOk := c.state.Streams[streamName]

		// Extract the list of column names that forms the primary key of the stream.
		// Note that this doesn't necessarily have to match the primary key of the
		// underlying database table (although a mismatch might make the initial
		// table scan less efficient).
		var primaryKey []string
		for _, col := range catalogStream.PrimaryKey {
			if len(col) != 1 {
				return errors.Errorf("stream %q: primary key path %q must contain exactly one string", streamName, col)
			}
			primaryKey = append(primaryKey, col[0])
		}
		if len(primaryKey) < 1 {
			return errors.Errorf("stream %q: no primary key specified", streamName)
		}

		// Only now that we've computed the `primaryKey` list can we initialize a
		// new stream state entry where necessary.
		if !stateOk {
			c.state.Streams[streamName] = &TableState{Mode: ScanStateScanning, ScanKey: primaryKey}
			stateDirty = true
			continue
		}

		if strings.Join(streamState.ScanKey, ";") != strings.Join(primaryKey, ";") {
			return errors.Errorf("stream %q: primary key changed (was %q, now %q)", streamName, streamState.ScanKey, primaryKey)
		}
	}

	// Likewise streams may be removed from the catalog, and we need to forget
	// the corresponding state information.
	for streamName := range c.state.Streams {
		// List membership checks are always a pain in Go, but that's all this loop is
		streamExistsInCatalog := false
		for _, catalogStream := range c.catalog.Streams {
			if streamName == catalogStream.Stream.Name {
				streamExistsInCatalog = true
			}
		}

		if !streamExistsInCatalog {
			log.Printf("Forgetting stream %q which is no longer in catalog", streamName)
			delete(c.state.Streams, streamName)
			stateDirty = true
		}
	}

	// If we've altered the state, emit it to stdout. This isn't strictly necessary
	// but it helps to make the emitted sequence of state updates a lot more readable.
	if stateDirty {
		c.output.EmitState(c.state)
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
	if snapshot.TransactionLSN() < c.state.CurrentLSN {
		return errors.Errorf("table snapshot (LSN=%q) is older than the current replication LSN (%q)", snapshot.TransactionLSN(), c.state.CurrentLSN)
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
	for _, tableState := range c.state.Streams {
		if tableState.Mode == ScanStateScanning {
			return true
		}
	}
	return false
}

func (c *Capture) ScanNext(ctx context.Context, snapshot *TableSnapshotStream) error {
	// Find the first stream which is still in the "Scanning" state.
	for streamName, tableState := range c.state.Streams {
		if tableState.Mode != "Scanning" {
			continue
		}

		// If this stream has no previously scanned range information, request the first
		// chunk, then initialize the scanned ranges list and emit a state update.
		if len(tableState.ScanRanges) == 0 {
			_, endKey, err := snapshot.ScanStart(ctx, "public", streamName, tableState.ScanKey, c.HandleChangeEvent)
			if err != nil {
				return errors.Wrap(err, "error processing snapshot events")
			}
			tableState.ScanRanges = []TableRange{{
				ScannedLSN: snapshot.TransactionLSN(),
				EndKey:     endKey,
			}}
			return c.output.EmitState(c.state)
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
		count, nextKey, err := snapshot.ScanFrom(ctx, "public", streamName, tableState.ScanKey, lastRange.EndKey, c.HandleChangeEvent)
		if err != nil {
			return errors.Wrap(err, "error processing snapshot events")
		}
		if count == 0 {
			tableState.Mode = ScanStateActive
		} else {
			lastRange.EndKey = nextKey
		}
		return c.output.EmitState(c.state)
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
	stream, err := StartReplication(ctx, c.connRepl, c.config.SlotName, c.config.PublicationName, c.state.CurrentLSN)
	if err != nil {
		return errors.Wrap(err, "unable to start replication stream")
	}
	defer stream.Close(ctx)
	if err := stream.Process(ctx, c.HandleReplicationEvent); err != nil {
		return errors.Wrap(err, "error processing replication events")
	}
	log.Printf("Stream processing terminated without error. That shouldn't be possible.")
	return nil
}

func (c *Capture) HandleReplicationEvent(evt *ChangeEvent) error {
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
		c.state.CurrentLSN = evt.LSN
		return c.output.EmitState(c.state)
	}

	tableState := c.state.Streams[evt.Table]
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
		if LogFilteredEvents {
			log.Printf("Filtered %s on %q at LSN=%q with values %v", evt.Type, evt.Table, evt.LSN, evt.Fields)
		}
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
	if evt.Type == "Insert" || evt.Type == "Update" {
		evt.Fields["_updated_at"] = time.Now()
	}
	if evt.Type == "Delete" {
		evt.Fields["_deleted_at"] = time.Now()
	}
	return c.output.EmitRecord(evt.Namespace, evt.Table, evt.Fields)
}

// A MessageOutput is a thread-safe JSON-encoding output stream with convenience
// methods for emitting Airbyte Records and State messages.
type MessageOutput struct {
	sync.Mutex
	encoder *json.Encoder
}

func NewMessageOutput() *MessageOutput {
	return &MessageOutput{encoder: json.NewEncoder(os.Stdout)}
}

func (m *MessageOutput) EmitRecord(ns, stream string, data interface{}) error {
	rawData, err := json.Marshal(data)
	if err != nil {
		return errors.Wrap(err, "error encoding record data")
	}
	m.Lock()
	defer m.Unlock()
	return m.encoder.Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: ns,
			Stream:    stream,
			EmittedAt: time.Now().Unix(),
			Data:      json.RawMessage(rawData),
		},
	})
}

func (m *MessageOutput) EmitState(state interface{}) error {
	rawState, err := json.Marshal(state)
	if err != nil {
		return errors.Wrap(err, "error encoding state message")
	}
	m.Lock()
	defer m.Unlock()
	return m.encoder.Encode(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(rawState)},
	})
}
