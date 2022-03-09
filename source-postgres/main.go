package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/alecthomas/jsonschema"
	np "github.com/estuary/connectors/network-proxy-service"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/airbyte"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

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
	Database        string                 `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from."`
	Host            string                 `json:"host" jsonschema:"description=Host name of the database to connect to."`
	NetworkProxy    *np.NetworkProxyConfig `json:"networkProxy,omitempty" jsonschema:"description=Configurations to enable network proxies."`
	Password        string                 `json:"password" jsonschema:"description=User password configured within the database."`
	Port            uint16                 `json:"port" jsonschema:"default=5432" jsonschema:"description=Port to the DB connection. If SshForwardingConfig is enabled, a dynamic port is allocated if Port is unspecified."`
	PublicationName string                 `json:"publicationName,omitempty" jsonschema:"default=flow_publication,description=The name of the PostgreSQL publication to replicate from."`
	SlotName        string                 `json:"slotName,omitempty" jsonschema:"default=flow_slot,description=The name of the PostgreSQL replication slot to replicate from."`
	User            string                 `json:"user" jsonschema:"default=postgres,description=Database user to use."`
	WatermarksTable string                 `json:"watermarksTable,omitempty" jsonschema:"default=public.flow_watermarks,description=The name of the table used for watermark writes during backfills. Must be fully-qualified in '<schema>.<table>' form."`
}

// Validate checks that the configuration possesses all required properties.
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

	if c.WatermarksTable != "" && !strings.Contains(c.WatermarksTable, ".") {
		return fmt.Errorf("config parameter 'watermarksTable' must be fully-qualified as '<schema>.<table>': %q", c.WatermarksTable)
	}

	if err := c.NetworkProxy.Validate(); err != nil {
		return fmt.Errorf("Network proxy config err: %w", err)
	}

	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
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

type postgresDatabase struct {
	config *Config
	conn   *pgx.Conn
}

func (db *postgresDatabase) Connect(ctx context.Context) error {
	if err := db.config.NetworkProxy.Start(); err != nil {
		return fmt.Errorf("unable to start network proxy %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"host":     db.config.Host,
		"port":     db.config.Port,
		"user":     db.config.User,
		"database": db.config.Database,
		"slot":     db.config.SlotName,
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
