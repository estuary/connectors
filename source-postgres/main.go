package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/protocols/airbyte"
)

// Config tells the connector how to connect to the source database and can
// optionally be used to customize some other parameters such as polling timeout.
type Config struct {
	Database        string `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from."`
	Host            string `json:"host" jsonschema:"description=Host name of the database to connect to."`
	Password        string `json:"password" jsonschema:"description=User password configured within the database."`
	Port            uint16 `json:"port" jsonschema:"default=5432"`
	PublicationName string `json:"publication_name,omitempty" jsonschema:"default=flow_publication,description=The name of the PostgreSQL publication to replicate from."`
	SlotName        string `json:"slot_name,omitempty" jsonschema:"default=flow_slot,description=The name of the PostgreSQL replication slot to replicate from."`
	User            string `json:"user" jsonschema:"default=postgres,description=Database user to use."`
	WatermarksTable string `json:"watermarks_table,omitempty" jsonschema:"default=public.flow_watermarks,description=The name of the table used for watermark writes during backfills."`
}

func main() {
	var schema = jsonschema.Reflect(&Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}

	var spec = airbyte.Spec{
		SupportsIncremental:     true,
		ConnectionSpecification: json.RawMessage(configSchema),
	}

	airbyte.RunMain(spec, doCheck, doDiscover, doRead)
}

// Validate checks that the configuration passes some basic sanity checks, and
// fills in default values when optional parameters are unset.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.SlotName == "" {
		c.SlotName = "flow_slot"
	}
	if c.PublicationName == "" {
		c.PublicationName = "flow_publication"
	}
	if c.WatermarksTable == "" {
		c.WatermarksTable = "public.flow_watermarks"
	}

	return nil
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	var host = c.Host
	if c.Port != 0 {
		host = fmt.Sprintf("%s:%d", host, c.Port)
	}
	var uri = url.URL{
		Scheme: "postgres",
		Host:   host,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	return uri.String()
}

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
