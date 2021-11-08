package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/estuary/protocols/airbyte"
)

func main() {
	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

// Config tells the connector how to connect to the source database and can
// optionally be used to customize some other parameters such as polling timeout.
type Config struct {
	ConnectionURI      string  `json:"connectionURI"`
	SlotName           string  `json:"slot_name"`
	PublicationName    string  `json:"publication_name"`
	WatermarksTable    string  `json:"watermarks_table"`
	PollTimeoutSeconds float64 `json:"poll_timeout_seconds"`
	MaxLifespanSeconds float64 `json:"max_lifespan_seconds"`
}

// Validate checks that the configuration passes some basic sanity checks, and
// fills in default values when optional parameters are unset.
func (c *Config) Validate() error {
	if c.ConnectionURI == "" {
		return fmt.Errorf("Database Connection URI must be set")
	}
	if c.SlotName == "" {
		c.SlotName = "flow_slot"
	}
	if c.PublicationName == "" {
		c.PublicationName = "flow_publication"
	}
	if c.WatermarksTable == "" {
		c.WatermarksTable = "public.flow_watermarks"
	}
	if c.PollTimeoutSeconds == 0 {
		c.PollTimeoutSeconds = 10
	}
	return nil
}

var spec = airbyte.Spec{
	SupportsIncremental:     true,
	ConnectionSpecification: json.RawMessage(configSchema),
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
		},
		"watermarks_table": {
			"type":        "string",
			"title":       "Watermarks Table",
			"description": "The name of the table used for 'watermark' writes during backfilling",
			"default":     "public.flow_watermarks"
		},
		"poll_timeout_seconds": {
			"type":        "number",
			"title":       "Poll Timeout (seconds)",
			"description": "When tail=false, controls how long to sit idle before shutting down",
			"default":     10
		},
		"max_lifespan_seconds": {
			"type":        "number",
			"title":       "Maximum Connector Lifespan (seconds)",
			"description": "When nonzero, imposes a maximum runtime after which to unconditionally shut down",
			"default":     0
		}
	},
	"required": [ "connectionURI" ]
}`

func doCheck(args airbyte.CheckCmd) error {
	var config Config
	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	}
	var result = &airbyte.ConnectionStatus{Status: airbyte.StatusSucceeded}
	if _, err := DiscoverCatalog(context.Background(), config); err != nil {
		result.Status = airbyte.StatusFailed
		result.Message = err.Error()
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:             airbyte.MessageTypeConnectionStatus,
		ConnectionStatus: result,
	})
}

func doDiscover(args airbyte.DiscoverCmd) error {
	var config Config
	if err := args.ConfigFile.Parse(&config); err != nil {
		return err
	}
	var catalog, err = DiscoverCatalog(context.Background(), config)
	if err != nil {
		return err
	}
	return airbyte.NewStdoutEncoder().Encode(airbyte.Message{
		Type:    airbyte.MessageTypeCatalog,
		Catalog: catalog,
	})
}

func doRead(args airbyte.ReadCmd) error {
	var ctx = context.Background()

	var state = &PersistentState{Streams: make(map[string]*TableState)}
	if args.StateFile != "" {
		if err := args.StateFile.Parse(state); err != nil {
			return fmt.Errorf("unable to parse state file: %w", err)
		}
	}

	var config = new(Config)
	if err := args.ConfigFile.Parse(config); err != nil {
		return err
	}

	var catalog = new(airbyte.ConfiguredCatalog)
	if err := args.CatalogFile.Parse(catalog); err != nil {
		return fmt.Errorf("unable to parse catalog: %w", err)
	}

	return RunCapture(ctx, config, catalog, state, json.NewEncoder(os.Stdout))
}
