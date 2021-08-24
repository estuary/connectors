package main

import (
	"context"
	"encoding/json"
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

	if err := streamSnapshotEvents(ctx, conn, "public", "babynames", emitChangeRecord); err != nil {
		return errors.Wrap(err, "error streaming snapshot events")
	}

	// TODO: Emit a state update as soon as the snapshot is complete, don't
	// wait for the first COMMIT message to occur. Or modify the logic in
	// `streamReplicationEvents` to emit eagerly before doing anything else.

	if err := streamReplicationEvents(ctx, replConn, SLOT_NAME, PUBLICATION_NAME, 0, emitChangeRecord); err != nil {
		return errors.Wrap(err, "error streaming replication events")
	}

	return nil
}

func emitChangeRecord(namespace, table string, eventType string, fields map[string]interface{}) error {
	fields["_type"] = eventType
	if eventType == "Delete" {
		fields["_deleted"] = true
	}

	rawData, err := json.Marshal(fields)
	if err != nil {
		return errors.Wrap(err, "error encoding message data")
	}
	if err := json.NewEncoder(os.Stdout).Encode(airbyte.Message{
		Type: airbyte.MessageTypeRecord,
		Record: &airbyte.Record{
			Stream:    table,
			Namespace: namespace,
			EmittedAt: time.Now().Unix(),
			Data:      json.RawMessage(rawData),
		},
	}); err != nil {
		return errors.Wrap(err, "error writing output message")
	}
	return nil
}
