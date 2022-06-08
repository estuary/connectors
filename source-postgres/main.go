package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/airbyte"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

func main() {
	var schema = schemagen.GenerateSchema("PostgreSQL Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	var spec = airbyte.Spec{
		SupportsIncremental:     true,
		ConnectionSpecification: json.RawMessage(configSchema),
		DocumentationURL:        "https://go.estuary.dev/source-postgresql",
	}

	sqlcapture.AirbyteMain(spec, func(configFile airbyte.ConfigFile) (sqlcapture.Database, error) {
		var config Config
		if err := configFile.Parse(&config); err != nil {
			return nil, fmt.Errorf("error parsing config file: %w", err)
		}
		config.SetDefaults()
		return &postgresDatabase{config: &config}, nil
	})
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,default=127.0.0.1:5432,description=The host or host:port at which the database can be reached."`
	Database string         `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from."`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as."`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	PublicationName string `json:"publicationName,omitempty" jsonschema:"default=flow_publication,description=The name of the PostgreSQL publication to replicate from."`
	SlotName        string `json:"slotName,omitempty" jsonschema:"default=flow_slot,description=The name of the PostgreSQL replication slot to replicate from."`
	WatermarksTable string `json:"watermarksTable,omitempty" jsonschema:"default=public.flow_watermarks,description=The name of the table used for watermark writes during backfills. Must be fully-qualified in '<schema>.<table>' form."`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Advanced.WatermarksTable != "" && !strings.Contains(c.Advanced.WatermarksTable, ".") {
		return fmt.Errorf("config parameter 'watermarksTable' must be fully-qualified as '<schema>.<table>': %q", c.Advanced.WatermarksTable)
	}

	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.Advanced.SlotName == "" {
		c.Advanced.SlotName = "flow_slot"
	}
	if c.Advanced.PublicationName == "" {
		c.Advanced.PublicationName = "flow_publication"
	}
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = "public.flow_watermarks"
	}

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the PostgreSQL
	// default 5432.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":5432"
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	var uri = url.URL{
		Scheme: "postgres",
		Host:   c.Address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	return uri.String()
}

type postgresDatabase struct {
	config *Config
	conn   *pgx.Conn
}

func (db *postgresDatabase) Connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"address":  db.config.Address,
		"user":     db.config.User,
		"database": db.config.Database,
		"slot":     db.config.Advanced.SlotName,
	}).Info("initializing connector")

	// Normal database connection used for table scanning
	var conn, err = pgx.Connect(ctx, db.config.ToURI())
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	db.conn = conn
	return nil
}

func (db *postgresDatabase) Close(ctx context.Context) error {
	if err := db.conn.Close(ctx); err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}
	return nil
}

func (db *postgresDatabase) DefaultSchema(ctx context.Context) (string, error) {
	return "public", nil
}

func (db *postgresDatabase) EmptySourceMetadata() sqlcapture.SourceMetadata {
	return &postgresSource{}
}

func (db *postgresDatabase) EncodeKeyFDB(key interface{}) (tuple.TupleElement, error) {
	switch key := key.(type) {
	case pgtype.Numeric:
		return encodePgNumericKeyFDB(key)
	default:
		return key, nil
	}
}

func (db *postgresDatabase) DecodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	switch t := t.(type) {
	case tuple.Tuple:
		if d := maybeDecodePgNumericTuple(t); d != nil {
			return d, nil
		}

		return nil, errors.New("failed in decoding the fdb tuple")
	default:
		return t, nil
	}
}
