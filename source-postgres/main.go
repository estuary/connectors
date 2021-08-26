package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
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

const SLOT_NAME = "estuary_flow_slot"
const PUBLICATION_NAME = "estuary_flow_publication"
const OUTPUT_PLUGIN = "pgoutput"

type ResumeState struct {
	SlotName  string
	ResumeLSN pglogrepl.LSN
}

func (rs *ResumeState) Validate() error {
	return nil
}

func doRead(args airbyte.ReadCmd) error {
	var config Config
	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	}
	var catalog airbyte.ConfiguredCatalog
	if err := args.CatalogFile.Parse(&catalog); err != nil {
		return errors.Wrap(err, "unable to parse catalog")
	}
	var state ResumeState
	if args.StateFile != "" {
		if err := args.StateFile.Parse(&state); err != nil {
			return errors.Wrap(err, "unable to parse state file")
		}
	}

	ctx := context.Background()
	connConfig, err := pgx.ParseConfig(config.ConnectionURI)
	if err != nil {
		return err
	}

	// Connect normally
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return errors.Wrap(err, "unable to connect to database")
	}
	defer conn.Close(ctx)

	// Connect with replication=database
	replConfig, err := pgconn.ParseConfig(config.ConnectionURI)
	if err != nil {
		return err
	}
	replConfig.RuntimeParams["replication"] = "database"
	replConn, err := pgconn.ConnectConfig(ctx, replConfig)
	if err != nil {
		return errors.Wrap(err, "unable to establish replication connection")
	}

	snapshot, err := SnapshotTable(ctx, conn, "public", "babynames")
	if err != nil {
		return errors.Wrap(err, "error creating table snapshot stream")
	}
	log.Printf("Created table snapshot stream at LSN=%q", snapshot.TransactionLSN())
	if err := snapshot.Process(ctx, emitChangeRecord); err != nil {
		return errors.Wrap(err, "error processing snapshot events")
	}
	log.Printf("Table snapshot processing complete")

	// TODO: Emit a state update as soon as the snapshot is complete, don't
	// wait for the first COMMIT message to occur. Or modify the logic in
	// `streamReplicationEvents` to emit eagerly before doing anything else.
	sysident, err := pglogrepl.IdentifySystem(ctx, replConn)
	if err != nil {
		return errors.Wrap(err, "unable to get current LSN from database")
	}
	stream, err := StartReplication(ctx, replConn, SLOT_NAME, PUBLICATION_NAME, sysident.XLogPos)
	if err != nil {
		return errors.Wrap(err, "unable to start replication stream")
	}

	log.Printf("Beginning stream processing")
	if err := stream.Process(ctx, emitChangeRecord); err != nil {
		return errors.Wrap(err, "error processing replication events")
	}
	log.Printf("Stream processing completed (this shouldn't happen)")

	// What's the data type for the state encoding which can represent
	// things like "The connector was previously running on tables 'foo'
	// and 'bar' through to <LSN>, and now it's been relaunched and there
	// is a new table 'baz' which needs to be snapshot-scanned" and then
	// a bit later "The table 'baz' has been snapshot-scanned and we are
	// now doing a catchup stream of the replication events until we reach
	// the same point in the logs"?
	//
	// Or rather, our state encoding largely just needs to represent the
	// checkpoints in that process. Since there's no way to *resume* the
	// snapshot scanning part, that should be treated as atomic. So we
	// need to keep track of each table separately, and there are two
	// states that a table can be in:
	//   + Catchup(<LSN>) - Until we catch up with <LSN> in the replication
	//     stream we will discard all events for this table.
	//   + Active() - Just emit records for this table.
	//
	// What happens if the connector is shut down, a table is removed from
	// the catalog, it's re-run for a while, and then the table is re-added?
	// There's no hope of doing something better than re-scanning, so that
	// implies that if a table vanishes from the catalog while being mentioned
	// in the resume state we should erase it from future resume state outputs.

	// There are two viable approaches here: either suspend log processing
	// entirely using context cancellation, or else cause it to block on
	// a semaphore *inside* of the handler until we're ready to resume.

	// Okay, so the idea is that we're going to start up by opening a
	// transaction for the full table scan and figure out what tables
	// will need said full scan. These can be immediately scanned and
	// the corresponding records emitted to stdout.
	//
	// Then we begin streaming processing from our resume cursor, and
	// what we need to do is filter out any change events from our new
	// tables until such a time as the change LSNs are greater than
	// the transaction LSN in which we read the table initially.
	//
	// Now, what are the semantics if the connector crashes or gets
	// killed prior to finishing the table scan and/or catch-up
	// streaming? As I understand it, Flow/Gazette will bundle
	// together data records and state updates into a "Checkpoint"
	// of some sort, so what are the consistency guarantees there?
	//
	// I'm especially worried about the following scenario:
	//   1. Connector launches, reads all table contents, begins catch-up
	//      streaming (that is, event processing where the current LSN is
	//      still less than the "start emitting events for the new table"
	//      threshold), and emits at least one state update.
	//   2. Connector gets killed
	//   3. Some rows get deleted from that table
	//   4. Connector is re-launched
	//
	// If we're not careful this could result in some deletions that will
	// never be noticed. But if the checkpointing behavior relies on there
	// being state updates then we might be able to provide better guarantees
	// by suppressing any state updates until the catch-up streaming ends.
	//
	// Of course there's the possibility of a reinforcing loop there, if the
	// catch-up streaming process consistently takes longer than the "kill and
	// restart connector" threshold then the amount of data to be caught up on
	// will just keep growing larger and larger each time it runs.
	//
	// Which brings us back to the "chunked" table scan option, which would
	// require us to interleave table scans with replication event handling,
	// but would in exchange allow us to say "when doing stream processing,
	// ignore events on <table> for keys between <key> and <key> until you
	// reach sequence number <LSN>".

	// But that's more complex than currently required. To start with, let's
	// simply do the "initial table scan" case. But specifically let's have
	// an LSN per table

	return nil
}

func emitChangeRecord(event string, lsn pglogrepl.LSN, ns, table string, fields map[string]interface{}) error {
	log.Printf("%s(LSN=%q, Table=%s.%s)", event, lsn, ns, table)

	if event == "Commit" {
		// TODO(wgd): What exactly are the semantics of transaction committing in the
		// logical replication WAL? Should we be buffering Insert/Update/Delete operations
		// and only emitting them as part of the Commit?
		rawState, err := json.Marshal(ResumeState{
			ResumeLSN: lsn,
		})
		if err != nil {
			return errors.Wrap(err, "error encoding state message")
		}
		if err := json.NewEncoder(os.Stdout).Encode(airbyte.Message{
			Type:  airbyte.MessageTypeState,
			State: &airbyte.State{Data: json.RawMessage(rawState)},
		}); err != nil {
			return errors.Wrap(err, "error writing state message")
		}
		return nil
	}

	if event == "Begin" {
		log.Printf("Ignoring BEGIN at LSN=%q", pglogrepl.LSN(lsn))
		return nil
	}

	fields["_type"] = event
	if event == "Delete" {
		fields["_deleted"] = true
	}

	rawData, err := json.Marshal(fields)
	if err != nil {
		return errors.Wrap(err, "error encoding message data")
	}
	if err := json.NewEncoder(os.Stdout).Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Namespace: ns,
			Stream:    table,
			EmittedAt: time.Now().Unix(),
			Data:      json.RawMessage(rawData),
		},
	}); err != nil {
		return errors.Wrap(err, "error writing output message")
	}
	return nil
}
