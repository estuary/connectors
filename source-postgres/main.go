package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

var spec = airbyte.Spec{
	SupportsIncremental:           true,                            // TODO(wgd): Verify that this is true once implemented
	SupportedDestinationSyncModes: airbyte.AllDestinationSyncModes, // TODO(wgd): Verify that this is true once implemented
	ConnectionSpecification:       json.RawMessage(configSchema),
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

func doCheck(args airbyte.CheckCmd) error {
	result := &airbyte.ConnectionStatus{Status: airbyte.StatusSucceeded}
	if err := checkDatabaseConnection(args.ConfigFile); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func checkDatabaseConnection(configFile airbyte.ConfigFile) error {
	var config Config
	if err := configFile.Parse(&config); err != nil {
		return err
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, config.ConnectionURI)
	if err != nil {
		return errors.Wrap(err, "unable to connect to database")
	}
	defer conn.Close(ctx)

	// I'm torn on whether actually performing a query is a necessary part
	// of verifying the database connection, but I lean towards "yes". Any
	// PostgreSQL database should have `pg_catalog.pg_tables` available, and
	// it will probably be required for the discovery process anyway, so this
	// seems okay.
	//
	// Actually, perhaps I ought to turn 'checkDatabaseConnection' into
	// 'discoverTables' and have the 'check' operation just verify that this
	// discovery process doesn't error out?
	var schema, table string
	const TABLES_QUERY = "SELECT schemaname, tablename FROM pg_catalog.pg_tables;"
	_, err = conn.QueryFunc(ctx, TABLES_QUERY, nil, []interface{}{&schema, &table}, func (r pgx.QueryFuncRow) error {
		log.Printf("Got Row: (%v, %v)", schema, table)
		return nil
	})
	return err
}

func doDiscover(args airbyte.DiscoverCmd) error {
	// TODO: Implement
	return nil
}

func doRead(args airbyte.ReadCmd) error {
	// TODO: Implement
	return nil
}