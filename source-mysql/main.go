package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/protocols/airbyte"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/sirupsen/logrus"
)

func main() {
	sqlcapture.AirbyteMain(spec, func(configFile airbyte.ConfigFile) (sqlcapture.Database, error) {
		var config Config
		if err := configFile.Parse(&config); err != nil {
			return nil, fmt.Errorf("error parsing config file: %w", err)
		}
		return &mysqlDatabase{config: &config}, nil
	})
}

// Config tells the connector how to connect to the source database and
// capture changes from it.
type Config struct {
	Address  string `json:"address"`
	User     string `json:"user"`
	Pass     string `json:"pass"`
	DBName   string `json:"dbname"`
	ServerID int    `json:"server_id"`

	//SlotName        string `json:"slot_name"`
	//PublicationName string `json:"publication_name"`
	WatermarksTable string `json:"watermarks_table"`
}

// Validate checks that the configuration passes some basic sanity checks, and
// fills in default values when optional parameters are unset.
func (c *Config) Validate() error {
	if c.WatermarksTable == "" {
		c.WatermarksTable = "flow.watermarks"
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
		"server_id": {
			"type": "integer",
			"title": "Server ID",
			"description": "The unique Server ID in the cluster"
		},
		"watermarks_table": {
			"type":        "string",
			"title":       "Watermarks Table",
			"description": "The name of the table used for watermark writes during backfills",
			"default":     "flow.watermarks"
		}
	},
	"required": [ "server_id" ]
}`

type mysqlDatabase struct {
	config        *Config
	conn          *client.Conn
	defaultSchema string
}

func (db *mysqlDatabase) Connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"addr":   db.config.Address,
		"user":   db.config.User,
		"pass":   db.config.Pass,
		"dbName": db.config.DBName,
	}).Info("initializing connector")

	// Normal database connection used for table scanning
	var conn, err = client.Connect(db.config.Address, db.config.User, db.config.Pass, db.config.DBName)
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
