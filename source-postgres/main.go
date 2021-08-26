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
	ConnectionURI string `json:"connectionURI"`
}

func (c *Config) Validate() error { return nil }

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

	state.output.debugOutputDelay = 1 * time.Second // DEBUG CHANGE, DO NOT COMMIT

	ctx, cancel := context.WithTimeout(context.Background(), poisonPillDuration)
	defer cancel()

	return state.Process(ctx)
}

type CaptureState struct {
	CurrentLSN pglogrepl.LSN          `json:"current_lsn"`
	Streams    map[string]*TableState `json:"streams"`

	openTransactions int

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

const SLOT_NAME = "estuary_flow_slot"
const PUBLICATION_NAME = "estuary_flow_publication"

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

	// Step Two: Table Scanning
	// TODO(wgd): Remove stream state entries if the catalog entry is deleted
	for _, catalogStream := range cs.catalog.Streams {
		streamName := catalogStream.Stream.Name
		if _, ok := cs.Streams[streamName]; ok {
			continue
		}
		if err := cs.PerformTableSnapshot(ctx, conn, streamName); err != nil {
			return err
		}
	}

	// Step Three: Stream replication events
	// TODO(wgd): Get slot name and publication name from the config or the catalog or whatever
	stream, err := StartReplication(ctx, replicationConn, SLOT_NAME, PUBLICATION_NAME, cs.CurrentLSN)
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

func (cs *CaptureState) PerformTableSnapshot(ctx context.Context, conn *pgx.Conn, streamName string) error {
	snapshot, err := SnapshotTable(ctx, conn, "public", streamName)
	if err != nil {
		return errors.Wrap(err, "error creating table snapshot stream")
	}
	defer snapshot.Close(ctx)
	log.Printf("Scanning table %q at LSN=%q", streamName, snapshot.TransactionLSN())
	if err := snapshot.Process(ctx, cs.HandleSnapshotEvent); err != nil {
		return errors.Wrap(err, "error processing snapshot events")
	}
	log.Printf("Done scanning table %q at LSN=%q", streamName, snapshot.TransactionLSN())
	cs.Streams[streamName] = &TableState{
		Mode:        "Snapshot",
		SnapshotLSN: snapshot.TransactionLSN(),
	}
	return cs.output.EmitState(cs)
}

func (cs *CaptureState) HandleSnapshotEvent(event string, lsn pglogrepl.LSN, ns, table string, fields map[string]interface{}) error {
	// Snapshot "events" are just a bunch of fake "Insert"s and should be emitted unconditionally
	return cs.HandleChangeEvent(event, lsn, ns, table, fields)
}

func (cs *CaptureState) HandleReplicationEvent(event string, lsn pglogrepl.LSN, ns, table string, fields map[string]interface{}) error {
	if event == "Begin" {
		cs.openTransactions++
		return nil
	}
	if event == "Commit" {
		// TODO(wgd): What exactly are the semantics of transaction committing in the
		// logical replication WAL? Should we be buffering Insert/Update/Delete operations
		// and only emitting them as part of the Commit?
		cs.openTransactions--
		if cs.openTransactions == 0 {
			log.Printf("Consistent Point at LSN=%q", lsn)
			cs.CurrentLSN = lsn
			return cs.output.EmitState(cs)
		}
		return nil
	}

	tableState := cs.Streams[table]
	// Tables for which we're not tracking state by the time replication event
	// processing has begun are tables that aren't configured in the catalog,
	// so don't emit any change messages for them.
	if tableState == nil {
		return nil
	}
	// Tables in the "Snapshot" state shouldn't emit change messages until we
	// reach the appropriate LSN
	if tableState.Mode == "Snapshot" && lsn < tableState.SnapshotLSN {
		log.Printf("Filtered %s on %q at LSN=%q", event, table, lsn)
		return nil
	}
	// Once we've reached the LSN at which the table was scanned, it becomes active
	if tableState.Mode == "Snapshot" && lsn >= tableState.SnapshotLSN {
		log.Printf("Stream %q marked active at LSN=%q", table, lsn)
		tableState.Mode = "Active"
		tableState.SnapshotLSN = 0
	}
	// And once a table is active we emit its events
	if tableState.Mode == "Active" {
		return cs.HandleChangeEvent(event, lsn, ns, table, fields)
	}
	// This should never happen
	return errors.Errorf("invalid state %#v for table %q", tableState, table)
}

func (cs *CaptureState) HandleChangeEvent(event string, lsn pglogrepl.LSN, ns, table string, fields map[string]interface{}) error {
	//log.Printf("%s(LSN=%q, Table=%s.%s)", event, lsn, ns, table)

	// TODO(wgd): Should these field names and values aim for exact
	// compatibility with Airbyte Postgres CDC operation?
	fields["_change_type"] = event
	fields["_change_lsn"] = lsn
	if event == "Insert" || event == "Update" {
		fields["_updated_at"] = time.Now()
	}
	if event == "Delete" {
		fields["_deleted_at"] = time.Now()
	}
	return cs.output.EmitRecord(ns, table, fields)
}

// A MessageOutput is a thread-safe JSON-encoding output stream with convenience
// methods for emitting Airbyte Records and State messages.
type MessageOutput struct {
	sync.Mutex
	encoder *json.Encoder

	debugOutputDelay time.Duration // DEBUG CHANGE, DO NOT COMMIT
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
	time.Sleep(m.debugOutputDelay) // DEBUG CHANGE, DO NOT COMMIT
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
	time.Sleep(m.debugOutputDelay) // DEBUG CHANGE, DO NOT COMMIT
	return m.encoder.Encode(airbyte.Message{
		Type:  airbyte.MessageTypeState,
		State: &airbyte.State{Data: json.RawMessage(rawState)},
	})
}
