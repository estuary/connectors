package main

import (
	"context"
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
	Address  string         `json:"address" jsonschema:"title=Server Address and Port,default=127.0.0.1:3306,description=The host:port at which the database can be reached."`
	Login    loginConfig    `json:"login" jsonschema:"title=Login Configuration"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type loginConfig struct {
	User     string `json:"user" jsonschema:"title=Login Username,default=flow_capture,description=The database user to authenticate as."`
	Password string `json:"password" jsonschema:"title=Login Password,description=Password for the specified database user." jsonschema_extras:"secret=true"`
}

type advancedConfig struct {
	WatermarksTable          string `json:"watermarks_table,omitempty" jsonschema:"title=Watermarks Table Name,default=flow.watermarks,description=The name of the table used for watermark writes. Must be fully-qualified in '<schema>.<table>' form."`
	DBName                   string `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SkipBinlogRetentionCheck bool   `json:"skip_binlog_retention_check,omitempty" jsonschema:"title=Skip Binlog Retention Sanity Check,default=false,description=Bypasses the 'dangerously short binlog retention' sanity check at startup. Only do this if you understand the danger and have a specific need."`
	NodeID                   uint32 `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value."`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.Login.User},
		{"password", c.Login.Password},
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
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = "flow.watermarks"
	}
	if c.Advanced.DBName == "" {
		c.Advanced.DBName = "mysql"
	}
	if c.Advanced.NodeID == 0 {
		c.Advanced.NodeID = 0x476C6F77 // "Flow"
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
		"user":     db.config.Login.User,
		"serverID": db.config.Advanced.NodeID,
	}).Info("initializing connector")

	// Normal database connection used for table scanning
	var conn, err = client.Connect(db.config.Address, db.config.Login.User, db.config.Login.Password, db.config.Advanced.DBName)
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
			return fmt.Errorf("binlog retention period is too short (go.estuary.dev/PoMlNf): server reports %s but at least %s is required (and 30+ days is preferred)", expiryTime.String(), minimumExpiryTime.String())
		}
	}

	return nil
}

func (db *mysqlDatabase) getBinlogExpiry() (time.Duration, error) {
	// TODO(wgd): Test whether this works on Amazon RDS, and add additional logic
	// to use the appropriate `mysql.rds_show_configuration` incantation if needed.

	// The new 'binlog_expire_logs_seconds' variable takes priority
	expireLogsSeconds, err := db.getIntegerSystemVariable("binlog_expire_logs_seconds")
	if err != nil {
		return 0, err
	}
	if expireLogsSeconds > 0 {
		return time.Duration(expireLogsSeconds) * time.Second, nil
	}

	// However 'expire_logs_days' will be used instead if 'seconds' was zero
	expireLogsDays, err := db.getIntegerSystemVariable("expire_logs_days")
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

func (db *mysqlDatabase) getIntegerSystemVariable(name string) (int64, error) {
	var results, err = db.conn.Execute(fmt.Sprintf("SHOW VARIABLES LIKE '%s';", name))
	if err != nil {
		return 0, fmt.Errorf("error querying system variables: %w", err)
	}
	for _, row := range results.Values {
		var rowName, rowValue = string(row[0].AsString()), &row[1]
		logrus.WithFields(logrus.Fields{"name": rowName, "value": rowValue.Value()}).Debug("got system variable")
		if rowName == name {
			switch rowValue.Type {
			case mysql.FieldValueTypeString:
				var n, err = strconv.ParseInt(string(rowValue.AsString()), 10, 64)
				if err != nil {
					return 0, fmt.Errorf("couldn't parse string value as decimal number: %w", err)
				}
				return n, nil
			case mysql.FieldValueTypeFloat:
				return int64(rowValue.AsFloat64()), nil
			case mysql.FieldValueTypeUnsigned:
				return int64(rowValue.AsUint64()), nil
			default:
				return rowValue.AsInt64(), nil
			}
		}
	}
	return 0, fmt.Errorf("no value found for '%s'", name)
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
