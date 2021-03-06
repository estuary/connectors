package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/airbyte"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"

	mysqlLog "github.com/siddontang/go-log/log"
)

const minimumExpiryTime = 7 * 24 * time.Hour

func main() {
	fixMysqlLogging()
	var schema = schemagen.GenerateSchema("MySQL Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	var spec = airbyte.Spec{
		SupportsIncremental:     true,
		ConnectionSpecification: json.RawMessage(configSchema),
		DocumentationURL:        "https://go.estuary.dev/source-mysql",
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

// fixMysqlLogging works around some unfortunate defaults in the go-log package, which is used by
// go-mysql. This configures their logger to write to stderr instead of stdout (who does that?) and
// sets the level filter to match the level used by logrus. Unfortunately, there's no way to configure
// go-log to log in JSON format, so we'll still end up with interleaved JSON and plain text. But
// Flow handles that fine, so it's primarily just a visual inconvenience.
func fixMysqlLogging() {
	var handler, err = mysqlLog.NewStreamHandler(os.Stderr)
	// Based on a look at the source code, NewStreamHandler never actually returns an error, so this
	// is just a bit of future proofing.
	if err != nil {
		panic(fmt.Sprintf("failed to intialize mysql logging: %v", err))
	}

	mysqlLog.SetDefaultLogger(mysqlLog.NewDefault(handler))
	// Looking at the source code, it seems that the level names pretty muc" match those used by logrus.
	// In the event that anything doesn't match, it'll fall back to info level.
	// Source: https://github.com/siddontang/go-log/blob/1e957dd83bed/log/logger.go#L116
	mysqlLog.SetLevelByName(logrus.GetLevel().String())
}

// Config tells the connector how to connect to the source database and
// capture changes from it.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached."`
	User     string         `json:"user" jsonschema:"title=Login Username,default=flow_capture,description=The database user to authenticate as."`
	Password string         `json:"password" jsonschema:"title=Login Password,description=Password for the specified database user." jsonschema_extras:"secret=true"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	WatermarksTable          string `json:"watermarks_table,omitempty" jsonschema:"title=Watermarks Table Name,default=flow.watermarks,description=The name of the table used for watermark writes. Must be fully-qualified in '<schema>.<table>' form."`
	DBName                   string `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SkipBinlogRetentionCheck bool   `json:"skip_binlog_retention_check,omitempty" jsonschema:"title=Skip Binlog Retention Sanity Check,default=false,description=Bypasses the 'dangerously short binlog retention' sanity check at startup. Only do this if you understand the danger and have a specific need."`
	NodeID                   uint32 `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value."`
	SkipBackfills            string `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
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
		return fmt.Errorf("invalid 'watermarksTable' configuration: table name %q must be fully-qualified as \"<schema>.<table>\"", c.Advanced.WatermarksTable)
	}
	if c.Advanced.SkipBackfills != "" {
		for _, skipStreamID := range strings.Split(c.Advanced.SkipBackfills, ",") {
			if !strings.Contains(skipStreamID, ".") {
				return fmt.Errorf("invalid 'skipBackfills' configuration: table name %q must be fully-qualified as \"<schema>.<table>\"", skipStreamID)
			}
		}
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = "flow.watermarks"
	}
	if c.Advanced.DBName == "" {
		c.Advanced.DBName = "mysql"
	}
	if c.Advanced.NodeID == 0 {
		c.Advanced.NodeID = 0x476C6F77 // "Flow"
	}

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the MySQL
	// default 3306.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":3306"
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
		"dbName":   db.config.Advanced.DBName,
		"user":     db.config.User,
		"serverID": db.config.Advanced.NodeID,
	}).Info("initializing connector")

	// Normal database connection used for table scanning
	var conn, err = client.Connect(db.config.Address, db.config.User, db.config.Password, db.config.Advanced.DBName, func(c *client.Conn) {
		// TODO(wgd): Consider adding an optional 'serverName' config parameter which
		// if set makes this false and sets 'ServerName' so it will be verified properly.
		c.SetTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		})
	})
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	db.conn = conn

	// Sanity-check binlog retention and error out if it's insufficiently long.
	// By doing this during the Connect operation it will occur both during
	// actual captures and when performing discovery/config validation, which
	// is likely what we want.
	if !db.config.Advanced.SkipBinlogRetentionCheck {
		expiryTime, err := db.getBinlogExpiry()
		if err != nil {
			return fmt.Errorf("error querying binlog expiry time: %w", err)
		}
		if expiryTime < minimumExpiryTime {
			return fmt.Errorf("binlog retention period is too short (go.estuary.dev/PoMlNf): server reports %s but at least %s is required (and 30 days is preferred wherever possible)", expiryTime.String(), minimumExpiryTime.String())
		}
	}

	return nil
}

func (db *mysqlDatabase) getBinlogExpiry() (time.Duration, error) {
	// When running on Amazon RDS MySQL there's an RDS-specific configuration
	// for binlog retention, so that takes precedence if it exists.
	rdsRetentionHours, err := db.queryIntegerVariable(`SELECT name, value FROM mysql.rds_configuration WHERE name = 'binlog retention hours';`)
	if err == nil {
		return time.Duration(rdsRetentionHours) * time.Hour, nil
	}

	// The new 'binlog_expire_logs_seconds' variable takes priority
	expireLogsSeconds, err := db.queryIntegerVariable(`SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';`)
	if err != nil {
		return 0, err
	}
	if expireLogsSeconds > 0 {
		return time.Duration(expireLogsSeconds) * time.Second, nil
	}

	// However 'expire_logs_days' will be used instead if 'seconds' was zero
	expireLogsDays, err := db.queryIntegerVariable(`SHOW VARIABLES LIKE 'expire_logs_days';`)
	if err != nil {
		return 0, err
	}
	if expireLogsDays > 0 {
		return time.Duration(expireLogsDays) * 24 * time.Hour, nil
	}

	// If both 'binlog_expire_logs_seconds' and 'expire_logs_days' are set to zero
	// MySQL will not automatically purge binlog segments. For simplicity we just
	// represent that as a 'one year' expiry time, since all we need the value for
	// is to make sure it's not too short.
	return 365 * 24 * time.Hour, nil
}

func (db *mysqlDatabase) queryIntegerVariable(query string) (int64, error) {
	var results, err = db.conn.Execute(query)
	if err != nil {
		return 0, fmt.Errorf("error executing query %q: %w", query, err)
	}
	if len(results.Values) == 0 {
		return 0, fmt.Errorf("no results from query %q", query)
	}

	// Return the second column of the first row. It has to be the second
	// column because that's how the `SHOW VARIABLES LIKE` query does it.
	var value = &results.Values[0][1]
	switch value.Type {
	case mysql.FieldValueTypeNull:
		return 0, nil
	case mysql.FieldValueTypeString:
		var n, err = strconv.ParseInt(string(value.AsString()), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("couldn't parse string value as decimal number: %w", err)
		}
		return n, nil
	case mysql.FieldValueTypeFloat:
		return int64(value.AsFloat64()), nil
	case mysql.FieldValueTypeUnsigned:
		return int64(value.AsUint64()), nil
	}
	return value.AsInt64(), nil
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

func (db *mysqlDatabase) EncodeKeyFDB(key interface{}) (tuple.TupleElement, error) {
	return key, nil
}

func (db *mysqlDatabase) DecodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	return t, nil
}

func (db *mysqlDatabase) ShouldBackfill(streamID string) bool {
	if db.config.Advanced.SkipBackfills != "" {
		// This repeated splitting is a little inefficient, but this check is done at
		// most once per table during connector startup and isn't really worth caching.
		for _, skipStreamID := range strings.Split(db.config.Advanced.SkipBackfills, ",") {
			if strings.EqualFold(streamID, skipStreamID) {
				return false
			}
		}
	}
	return true
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
