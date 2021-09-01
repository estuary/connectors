package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"os"
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

func doRead(args airbyte.ReadCmd) error {
	state := new(CaptureState)
	if state.Streams == nil {
		state.Streams = make(map[string]*TableState)
	}

	if args.StateFile != "" {
		if err := args.StateFile.Parse(state); err != nil {
			return errors.Wrap(err, "unable to parse state file")
		}
	}
	if err := args.ConfigFile.Parse(&state.config); err != nil {
		return err
	}
	if err := args.CatalogFile.Parse(&state.catalog); err != nil {
		return errors.Wrap(err, "unable to parse catalog")
	}
	state.output = NewMessageOutput()

	ctx := context.Background()
	if PoisonPillDuration > 0 {
		limitedCtx, cancel := context.WithTimeout(ctx, PoisonPillDuration)
		defer cancel()
		ctx = limitedCtx
	}

	return state.Process(ctx)
}

type CaptureState struct {
	CurrentLSN pglogrepl.LSN          `json:"current_lsn"`
	Streams    map[string]*TableState `json:"streams"`

	config  Config
	catalog airbyte.ConfiguredCatalog
	output  *MessageOutput
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

func (cs *CaptureState) Validate() error {
	return nil
}

func (cs *CaptureState) Process(ctx context.Context) error {
	// Normal database connection used for table scanning
	conn, err := pgx.Connect(ctx, cs.config.ConnectionURI)
	if err != nil {
		return errors.Wrap(err, "unable to connect to database")
	}

	// Replication database connection used for event streaming
	replicationConnConfig, err := pgconn.ParseConfig(cs.config.ConnectionURI)
	if err != nil {
		return err
	}
	// Always connect with replication=database
	replicationConnConfig.RuntimeParams["replication"] = "database"
	replicationConn, err := pgconn.ConnectConfig(ctx, replicationConnConfig)
	if err != nil {
		return errors.Wrap(err, "unable to establish replication connection")
	}

	// Database capture in a nutshell:
	//   1. If this is the first time we're starting, immediately go get the current
	//      server LSN, this is where we will begin replication in step 3. If we're
	//      resuming then we already have our start point and this is not necessary.
	//   2. Examine the configured catalog and resume state for mismatches, and update
	//      the state to reflect any new streams.
	//   3. Perform table scanning/snapshotting work until there's none left
	//      - For each stream, we keep track of its 'Mode' (Scanning/Active) and a set
	//        of (ScannedLSN, LastKey) ranges which define the "chunks" in which the
	//        initial table capture was performed.
	//      - Each newly created stream starts in the "Scanning" mode with no ranges.
	//      - For each stream in the "Scanning" mode, issue a SQL query to fetch the
	//        next chunk of rows.
	//        - If there are no more rows to fetch, change mode to "Active".
	//        - Otherwise update the appropriate range information.
	//        - Finally issue a state update either way.
	//   4. Begin replication streaming from `CurrentLSN`
	//      - Each time a `Commit` message is received, update `CurrentLSN` and emit a state update.
	//      - Whenever a change event (Insert/Update/Delete) occurs, look up the appropriate
	//        range based on the primary key, and if `EventLSN > ScannedLSN` then emit a
	//        change message.

	// Step One: Get the current server LSN if our start point isn't already known
	if cs.CurrentLSN == 0 {
		sysident, err := pglogrepl.IdentifySystem(ctx, replicationConn)
		if err != nil {
			conn.Close(ctx)
			return errors.Wrap(err, "unable to get current LSN from database")
		}
		cs.CurrentLSN = sysident.XLogPos
	}

	// Synchronize stream states with the configured catalog. If a new stream has
	// been added then it's initialized in the "Pending" state, and if a previously
	// configured stream has been removed then the corresponding state entry should
	// be removed.
	catalogStateDirty := false
	for _, catalogStream := range cs.catalog.Streams {
		// Create state entries for catalog streams that don't already have one
		streamName := catalogStream.Stream.Name
		streamState, ok := cs.Streams[streamName]

		if len(catalogStream.PrimaryKey) < 1 {
			return errors.Errorf("stream %q: no primary key specified", streamName)
		}
		var pkey []string
		for _, col := range catalogStream.PrimaryKey {
			if len(col) != 1 {
				return errors.Errorf("stream %q: primary key path %q must contain exactly one string", streamName, col)
			}
			pkey = append(pkey, col[0])
		}

		if !ok {
			log.Printf("Initializing new stream %q as %q", streamName, ScanStateScanning)
			cs.Streams[streamName] = &TableState{Mode: ScanStateScanning, ScanKey: pkey}
			catalogStateDirty = true
			continue
		}
		if !stringsEqual(streamState.ScanKey, pkey) {
			return errors.Errorf("stream %q: primary key changed (was %q, now %q)", streamName, streamState.ScanKey, catalogStream.PrimaryKey)
		}
	}
	for streamName := range cs.Streams {
		// Delete state entries for streams that are no longer in the catalog
		streamExistsInCatalog := false
		for _, catalogStream := range cs.catalog.Streams {
			if streamName == catalogStream.Stream.Name {
				streamExistsInCatalog = true
			}
		}
		if !streamExistsInCatalog {
			log.Printf("Forgetting stream %q which is no longer in catalog", streamName)
			delete(cs.Streams, streamName)
			catalogStateDirty = true
		}
	}
	// This isn't strictly necessary since we should get the exact same result
	// if re-run with the previous input state and the same catalog, but it's
	// cheap and makes it easier to read and understand the sequence of state
	// outputs.
	if catalogStateDirty {
		cs.output.EmitState(cs)
	}

	// Step Two: Table Scanning. If no scans are pending, we skip the whole
	// process of creating a snapshot.
	if cs.HasPendingScans() {
		snapshot, err := SnapshotTable(ctx, conn)
		if err != nil {
			return errors.Wrap(err, "error creating table snapshot stream")
		}
		if snapshot.TransactionLSN() < cs.CurrentLSN {
			return errors.Errorf("table snapshot (LSN=%q) is older than the current replication LSN (%q)", snapshot.TransactionLSN(), cs.CurrentLSN)
		}
		defer snapshot.Close(ctx)
		for cs.HasPendingScans() {
			if err := cs.PerformNextScan(ctx, snapshot); err != nil {
				return err
			}
		}
	}

	// Step Three: Stream replication events
	stream, err := StartReplication(ctx, replicationConn, cs.config.SlotName, cs.config.PublicationName, cs.CurrentLSN)
	if err != nil {
		return errors.Wrap(err, "unable to start replication stream")
	}
	defer stream.Close(ctx)
	if err := stream.Process(ctx, cs.HandleReplicationEvent); err != nil {
		return errors.Wrap(err, "error processing replication events")
	}
	log.Printf("Stream processing terminated without error. That shouldn't be possible.")
	return nil
}

func (cs *CaptureState) HasPendingScans() bool {
	for _, tableState := range cs.Streams {
		if tableState.Mode == ScanStateScanning {
			return true
		}
	}
	return false
}

func (cs *CaptureState) PerformNextScan(ctx context.Context, snapshot *TableSnapshotStream) error {
	for streamName, tableState := range cs.Streams {
		if tableState.Mode != "Scanning" {
			continue
		}

		// If there are no scanned ranges, start a new scan and initialize the scanned ranges list.
		// TODO(wgd): What happens if the table is entirely empty? I'm pretty sure our end key is
		// nil, then.
		if len(tableState.ScanRanges) == 0 {
			_, endKey, err := snapshot.ScanStart(ctx, "public", streamName, tableState.ScanKey, cs.HandleSnapshotEvent)
			if err != nil {
				return errors.Wrap(err, "error processing snapshot events")
			}
			tableState.ScanRanges = []TableRange{{
				ScannedLSN: snapshot.TransactionLSN(),
				EndKey:     endKey,
			}}
			return cs.output.EmitState(cs)
		}

		// If the most recently scanned range had a different LSN than the current snapshot,
		// there must have been a restart in between. Create a new scan range, initialized
		// to the zero-length range after the previous range.
		lastRange := &tableState.ScanRanges[len(tableState.ScanRanges)-1]
		if lastRange.ScannedLSN != snapshot.TransactionLSN() {
			tableState.ScanRanges = append(tableState.ScanRanges, TableRange{
				ScannedLSN: snapshot.TransactionLSN(),
				EndKey:     lastRange.EndKey,
			})
			lastRange = &tableState.ScanRanges[len(tableState.ScanRanges)-1]
		}

		// At this point `lastRange` must refer to a range with the same `ScannedLSN`
		// as our current table scanning transaction, so we reach the core loop of our
		// table scanning logic: scan another chunk from the database and update the
		// scan state.
		count, nextKey, err := snapshot.ScanFrom(ctx, "public", streamName, tableState.ScanKey, lastRange.EndKey, cs.HandleSnapshotEvent)
		if err != nil {
			return errors.Wrap(err, "error processing snapshot events")
		}
		if count == 0 {
			tableState.Mode = ScanStateActive
		} else {
			lastRange.EndKey = nextKey
		}
		return cs.output.EmitState(cs)
	}
	return errors.New("no scans pending")
}

func (cs *CaptureState) HandleSnapshotEvent(evt *ChangeEvent) error {
	// Snapshot "events" are just a bunch of fake "Insert"s and should be emitted unconditionally
	return cs.HandleChangeEvent(evt)
}

func (cs *CaptureState) HandleReplicationEvent(evt *ChangeEvent) error {
	// TODO(wgd): The only thing the 'Commit' event is used for is to signal that
	// we could emit a state update. Can this be done better?
	if evt.Type == "Commit" {
		// TODO(wgd): There will be a "Commit" event after every transaction, so if there
		// are a bunch of independent single-row INSERTs we could end up emitting one state
		// output per row added, which is too much. Add some rate-limiting to EmitState.
		cs.CurrentLSN = evt.LSN
		return cs.output.EmitState(cs)
	}

	tableState := cs.Streams[evt.Table]
	if tableState == nil {
		// Tables for which we're not tracking state by the time replication event
		// processing has begun are tables that aren't configured in the catalog,
		// so don't do any further processing on their events.
		return nil
	}
	if tableState.Mode != ScanStateActive {
		// This should never happen since the table scanning state machine ought to
		// progress every table into the Active state before moving on to replication,
		// but better safe than sorry.
		return errors.Errorf("invalid state %#v for table %q", tableState, evt.Table)
	}

	// For each event, we need to figure out what "scan range" it falls into
	// based on the primary key, and compare the event LSN to the LSN at which
	// that range was scanned.
	//
	// TODO(wgd): Double-check that this will always be correct, given my
	// current understanding of PostgreSQL logical replication internals.
	isFiltered, err := cs.checkRangeLSN(tableState, evt.Fields, evt.LSN)
	if err != nil {
		return errors.Wrap(err, "could not determine whether to filter event")
	}
	if isFiltered {
		if LogFilteredEvents {
			log.Printf("Filtered %s on %q at LSN=%q with values %v", evt.Type, evt.Table, evt.LSN, evt.Fields)
		}
		return nil
	}
	return cs.HandleChangeEvent(evt)
}

func (cs *CaptureState) checkRangeLSN(tableState *TableState, fields map[string]interface{}, lsn pglogrepl.LSN) (bool, error) {
	// If the table was empty during the initial scan there will be no scan range
	// entries in the state (because we have no 'Last Key' to track them with),
	// so in this special case we know that no events on the table should be
	// filtered.
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
			if bytes.Compare(keyBytes, scanRange.EndKey) < 0 {
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

func (cs *CaptureState) HandleChangeEvent(evt *ChangeEvent) error {
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
	return cs.output.EmitRecord(evt.Namespace, evt.Table, evt.Fields)
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

func stringsEqual(xs, ys []string) bool {
	if len(xs) != len(ys) {
		return false
	}
	for i := range xs {
		if xs[i] != ys[i] {
			return false
		}
	}
	return true
}
