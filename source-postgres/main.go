package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
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

// Guarantee that the connector will not run longer than 48 hours without a
// restart. Since advancing the PostgreSQL replication slot `restart_lsn` only
// happens on restart (see comment in `replication.go` for explanation), this
// connector requires occasional restarts to allow PostgreSQL to free older WAL
// segments.
const poisonPillDuration = 48 * time.Hour

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

	ctx, cancel := context.WithTimeout(context.Background(), poisonPillDuration)
	defer cancel()

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
	Mode        string        `json:"mode"`
	SnapshotLSN pglogrepl.LSN `json:"scanned_lsn,omitempty"`
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
	//   2. For all tables which are in the ConfiguredCatalog but not in the current CaptureState:
	//      - Perform a full scan of the table, turning every result row into an `Insert` event
	//      - Obtain an LSN corresponding to this table scan contents, such that events older
	//        than this LSN are already taken into account and newer events represent changes
	//        from that baseline.
	//      - Add an entry to CaptureState indicating that this table is in the `Snapshot(<LSN>)`
	//        state, and emit the resulting CaptureState.
	//   3. Begin replication from `CurrentLSN`
	//      - Each time a "consistent point" is reached where there are no open transactions,
	//        update the `CurrentLSN` to the new point and emit a state update.
	//      - Whenever a change event (Insert/Update/Delete) occurs, look up the table state
	//        - If it's `Active` then emit the event.
	//        - If it's `Snapshot(<LSN>)` and `eventLSN` > `snapshotLSN` then enter `Active` state and emit the event.
	//        - If it's `Snapshot(<LSN>)` and `eventLSN` <= `snapshotLSN` then discard the event.

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
	//
	// TODO(wgd): While I'm thinking about the catalog, what exactly am I supposed
	// to do with the various other fields of `airbyte.ConfiguredStream`?
	catalogStateDirty := false
	for _, catalogStream := range cs.catalog.Streams {
		// Create state entries for catalog streams that don't already have one
		streamName := catalogStream.Stream.Name
		if _, ok := cs.Streams[streamName]; !ok {
			log.Printf("Initializing new stream %q as Pending", streamName)
			cs.Streams[streamName] = &TableState{Mode: "Pending"}
			catalogStateDirty = true
		}
	}
	for streamName := range cs.Streams {
		// Delete state entries for streams that are no longer in the catalog
		streamExistsInCatalog := false
		for _, catalogStream := range cs.catalog.Streams {
			if streamName == catalogStream.Stream.Name {
				streamExistsInCatalog = true
				break
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
			cs.PerformNextScan(ctx, snapshot)
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
		if tableState.Mode == "Pending" {
			return true
		}
	}
	return false
}

func (cs *CaptureState) PerformNextScan(ctx context.Context, snapshot *TableSnapshotStream) error {
	for streamName, tableState := range cs.Streams {
		if tableState.Mode != "Pending" {
			continue
		}

		log.Printf("Scanning table %q at LSN=%q", streamName, snapshot.TransactionLSN())
		// The way scanning works here is that each call to `ProcessFrom` yields a
		// single finite chunk of results plus some information that can be used to
		// fetch the next chunk next time, and then we repeat this process iteratively
		// until there are no more results.
		//
		// Thus after each chunk we can emit a new state update in the near future.
		// But currently the code doesn't quite do that bit, that's just the ultimate
		// goal towards which the current structure of the code is aimed.
		//
		// TODO(wgd): Restructure things so that:
		//   3. The transition "Pending->Scanning" invokes ProcessStart and stores a "PrevTID" field in TableState
		//   4. Probably some other stuff, but we'll need to resolve some of the other issues around
		//      resuming and identifying ranges when streaming first.
		// TODO(wgd): Add support for namespaces other than "public"
		// TODO(wgd): Emit state updates and add resume capability in between chunks
		count, prevTID, err := snapshot.ScanStart(ctx, "public", streamName, cs.HandleSnapshotEvent)
		if err != nil {
			return errors.Wrap(err, "error processing snapshot events")
		}
		for count > 0 {
			count, prevTID, err = snapshot.ScanFrom(ctx, "public", streamName, prevTID, cs.HandleSnapshotEvent)
			if err != nil {
				return errors.Wrap(err, "error processing snapshot events")
			}
		}

		log.Printf("Done scanning table %q at LSN=%q", streamName, snapshot.TransactionLSN())
		cs.Streams[streamName].Mode = "Snapshot"
		cs.Streams[streamName].SnapshotLSN = snapshot.TransactionLSN()
		return cs.output.EmitState(cs)
	}
	return errors.New("no scans pending")
}

func (cs *CaptureState) HandleSnapshotEvent(evt *ChangeEvent) error {
	// Snapshot "events" are just a bunch of fake "Insert"s and should be emitted unconditionally
	return cs.HandleChangeEvent(evt)
}

func (cs *CaptureState) HandleReplicationEvent(evt *ChangeEvent) error {
	// TODO(wgd): The only thing the 'Begin' and 'Commit' event is used for is
	// to signal that we could emit a state update. Can this be done better?
	if evt.Type == "Commit" {
		// TODO(wgd): There will be a "Commit" event after every transaction, so if there
		// are a bunch of independent single-row INSERTs we could end up emitting one state
		// output per row added, which is too much. Add some rate-limiting to EmitState.
		cs.CurrentLSN = evt.LSN
		return cs.output.EmitState(cs)
	}

	tableState := cs.Streams[evt.Table]
	// Tables for which we're not tracking state by the time replication event
	// processing has begun are tables that aren't configured in the catalog,
	// so don't emit any change messages for them.
	if tableState == nil {
		return nil
	}
	// Tables in the "Snapshot" state shouldn't emit change messages until we
	// reach the appropriate LSN
	if tableState.Mode == "Snapshot" && evt.LSN < tableState.SnapshotLSN {
		log.Printf("Filtered %s on %q at LSN=%q", evt.Type, evt.Table, evt.LSN)
		return nil
	}
	// Once we've reached the LSN at which the table was scanned, it becomes active
	if tableState.Mode == "Snapshot" && evt.LSN >= tableState.SnapshotLSN {
		log.Printf("Stream %q marked active at LSN=%q", evt.Table, evt.LSN)
		tableState.Mode = "Active"
		tableState.SnapshotLSN = 0
	}
	// And once a table is active we emit its events
	if tableState.Mode == "Active" {
		return cs.HandleChangeEvent(evt)
	}
	// This should never happen
	return errors.Errorf("invalid state %#v for table %q", tableState, evt.Table)
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
