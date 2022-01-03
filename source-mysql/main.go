package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/alecthomas/jsonschema"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/protocols/airbyte"
	"github.com/go-mysql-org/go-mysql/client"
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
		return &mysqlDatabase{config: &config}, nil
	})
}

// Config tells the connector how to connect to the source database and
// capture changes from it.
type Config struct {
	Address  string `json:"address" jsonschema:"default=127.0.0.1:3306,description=Database host:port to connect to."`
	User     string `json:"user" jsonschema:"default=flow_capture,description=Database user to connect as."`
	Password string `json:"password" jsonschema:"description=Password for the specified database user."`
	DBName   string `json:"dbname" jsonschema:"description=Name of the database to connect to."`
	ServerID int    `json:"server_id" jsonschema:"description=Server ID for replication."`

	WatermarksTable string `json:"watermarks_table,omitempty" jsonschema:"default=flow.watermarks,description=The name of the table used for watermark writes during backfills."`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
		{"dbname", c.DBName},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if c.ServerID == 0 {
		return fmt.Errorf("missing 'server_id'")
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.WatermarksTable == "" {
		c.WatermarksTable = "flow.watermarks"
	}
}

type mysqlDatabase struct {
	config        *Config
	conn          *client.Conn
	defaultSchema string
}

func (db *mysqlDatabase) Connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"addr":     db.config.Address,
		"user":     db.config.User,
		"dbName":   db.config.DBName,
		"serverID": db.config.ServerID,
	}).Info("initializing connector")

	// Normal database connection used for table scanning
	var conn, err = client.Connect(db.config.Address, db.config.User, db.config.Password, db.config.DBName)
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	db.conn = conn
	return nil
}

func (db *mysqlDatabase) Close(ctx context.Context) error {
	if err := db.conn.Close(); err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}
	return nil
}

func (db *mysqlDatabase) DefaultSchema(ctx context.Context) (string, error) {
	if db.defaultSchema == "" {
		var results, err = db.conn.Execute("SELECT database();")
		if err != nil {
			return "", fmt.Errorf("error querying default schema: %w", err)
		}
		if len(results.Values) == 0 {
			return "", fmt.Errorf("error querying default schema: no result rows")
		}
		db.defaultSchema = string(results.Values[0][0].AsString())
		logrus.WithField("schema", db.defaultSchema).Debug("queried default schema")
	}

	return db.defaultSchema, nil
}

func (db *mysqlDatabase) EmptySourceMetadata() sqlcapture.SourceMetadata {
	return &mysqlSourceInfo{}
}

// mysqlSourceInfo is source metadata for data capture events.
type mysqlSourceInfo struct {
	sqlcapture.SourceCommon
	FlushCursor string `json:"cursor,omitempty" jsonschema:"description=Cursor value representing the current position in the binlog."`
}

func (s *mysqlSourceInfo) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

func (s *mysqlSourceInfo) Cursor() string {
	return s.FlushCursor
}
