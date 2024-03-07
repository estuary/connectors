package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	perrors "github.com/pingcap/errors"
	"github.com/sirupsen/logrus"

	mysqlLog "github.com/siddontang/go-log/log"

	_ "time/tzdata"
)

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

const minimumExpiryTime = 7 * 24 * time.Hour

var mysqlDriver = &sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-mysql",
	Connect:          connectMySQL,
}

func main() {
	fixMysqlLogging()
	boilerplate.RunMain(mysqlDriver)
}

func connectMySQL(ctx context.Context, name string, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults(name)

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
	if config.NetworkTunnel != nil && config.NetworkTunnel.SSHForwarding != nil && config.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		host, port, err := net.SplitHostPort(config.Address)
		if err != nil {
			return nil, fmt.Errorf("splitting address to host and port: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: config.NetworkTunnel.SSHForwarding.SSHEndpoint,
			PrivateKey:  []byte(config.NetworkTunnel.SSHForwarding.PrivateKey),
			ForwardHost: host,
			ForwardPort: port,
			LocalPort:   "3306",
		}
		var tunnel = sshConfig.CreateTunnel()

		// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
		// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
		if err := tunnel.Start(); err != nil {
			return nil, fmt.Errorf("error starting network tunnel: %w", err)
		}
	}

	var db = &mysqlDatabase{config: &config}
	if err := db.connect(ctx); err != nil {
		return nil, err
	}
	return db, nil
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
	// Looking at the source code, it seems that the level names pretty much match those used by logrus.
	// In the event that anything doesn't match, it'll fall back to info level.
	// Source: https://github.com/siddontang/go-log/blob/1e957dd83bed/log/logger.go#L116
	mysqlLog.SetLevelByName(logrus.GetLevel().String())
}

// Config tells the connector how to connect to the source database and
// capture changes from it.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"title=Login Username,default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"title=Login Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Timezone string         `json:"timezone,omitempty" jsonschema:"title=Timezone,description=Timezone to use when capturing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read and columns with type datetime are being captured. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the 'time_zone' system variable if both are set (go.estuary.dev/80J6rX)." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	WatermarksTable          string `json:"watermarks_table,omitempty" jsonschema:"title=Watermarks Table Name,default=flow.watermarks,description=The name of the table used for watermark writes. Must be fully-qualified in '<schema>.<table>' form."`
	DBName                   string `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SkipBinlogRetentionCheck bool   `json:"skip_binlog_retention_check,omitempty" jsonschema:"title=Skip Binlog Retention Sanity Check,default=false,description=Bypasses the 'dangerously short binlog retention' sanity check at startup. Only do this if you understand the danger and have a specific need."`
	NodeID                   uint32 `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value."`
	SkipBackfills            string `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize        int    `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,description=The number of rows which should be fetched from the database in a single backfill query."`
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
	if len(c.Password) > 32 {
		return fmt.Errorf("passwords used as part of replication cannot exceed 32 characters in length due to an internal limitation in MySQL: password length of %d characters is too long, please use a shorter password", len(c.Password))
	}
	if c.Timezone != "" {
		if _, err := sqlcapture.ParseTimezone(c.Timezone); err != nil {
			return err
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
func (c *Config) SetDefaults(name string) {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = "flow.watermarks"
	}
	if c.Advanced.DBName == "" {
		c.Advanced.DBName = "mysql"
	}
	if c.Advanced.NodeID == 0 {
		// The only constraint on the node/server ID is that it needs to be unique
		// within a particular replication topology. We would also like it to be
		// consistent for a given capture for observability reasons, so here we
		// derive a default value by hashing the task name.
		var nameHash = sha256.Sum256([]byte(name))
		c.Advanced.NodeID = binary.BigEndian.Uint32(nameHash[:])
		c.Advanced.NodeID &= 0x7FFFFFFF // Clear MSB because watermark writes use the node ID as an integer key
	}
	if c.Advanced.BackfillChunkSize <= 0 {
		c.Advanced.BackfillChunkSize = 50000
	}

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the MySQL
	// default 3306.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":3306"
	}
}

func configSchema() json.RawMessage {
	var schema = schemagen.GenerateSchema("MySQL Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return json.RawMessage(configSchema)
}

type mysqlDatabase struct {
	config           *Config
	conn             *client.Conn
	explained        map[string]struct{} // Tracks tables which have had an `EXPLAIN` run on them during this connector invocation.
	datetimeLocation *time.Location      // The location in which to interpret DATETIME column values as timestamps.
	includeTxIDs     map[string]bool     // Tracks which tables should have XID properties in their replication metadata.
}

func (db *mysqlDatabase) connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"addr":     db.config.Address,
		"dbName":   db.config.Advanced.DBName,
		"user":     db.config.User,
		"serverID": db.config.Advanced.NodeID,
	}).Info("initializing connector")

	var address = db.config.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if db.config.NetworkTunnel != nil && db.config.NetworkTunnel.SSHForwarding != nil && db.config.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:3306"
	}

	// Normal database connection used for table scanning
	var conn *client.Conn
	var err error
	var withTLS = func(c *client.Conn) {
		// TODO(wgd): Consider adding an optional 'serverName' config parameter which
		// if set makes this false and sets 'ServerName' so it will be verified properly.
		c.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	}
	if conn, err = client.Connect(address, db.config.User, db.config.Password, db.config.Advanced.DBName, withTLS); err == nil {
		logrus.WithField("addr", address).Debug("connected with TLS")
		db.conn = conn
	} else if conn, err = client.Connect(address, db.config.User, db.config.Password, db.config.Advanced.DBName); err == nil {
		logrus.WithField("addr", address).Warn("connected without TLS")
		db.conn = conn
	} else {
		if err, ok := perrors.Cause(err).(*mysql.MyError); ok {
			if err.Code == mysql.ER_ACCESS_DENIED_ERROR {
				return cerrors.NewUserError(err, "incorrect username or password")
			}
		}

		return fmt.Errorf("unable to connect to database: %w", err)
	}

	if db.config.Timezone != "" {
		// The user-entered timezone value is verified to parse without error in (*Config).Validate,
		// so this parsing is not expected to fail.
		loc, err := sqlcapture.ParseTimezone(db.config.Timezone)
		if err != nil {
			return fmt.Errorf("invalid config timezone: %w", err)
		}
		logrus.WithFields(logrus.Fields{
			"tzName": db.config.Timezone,
			"loc":    loc.String(),
		}).Debug("using datetime location from config")
		db.datetimeLocation = loc
	} else {
		// Infer the location in which captured DATETIME values will be interpreted from the system
		// variable 'time_zone' if it is set on the database.
		var tzName string
		if tzName, err = queryTimeZone(conn); err == nil {
			var loc *time.Location
			if loc, err = sqlcapture.ParseTimezone(tzName); err == nil {
				logrus.WithFields(logrus.Fields{
					"tzName": tzName,
					"loc":    loc.String(),
				}).Debug("using datetime location queried from database")
				db.datetimeLocation = loc
			}
		}

		if db.datetimeLocation == nil {
			logrus.WithField("err", err).Warn("unable to determine database timezone and no timezone in capture configuration")
			logrus.Warn("capturing DATETIME values will not be permitted")
		}
	}

	// Set our desired timezone (specifically for the backfill connection, this has
	// no effect on the database as a whole) to UTC. This is required for backfills of
	// TIMESTAMP columns to behave consistently, and has no effect on DATETIME columns.
	if _, err := db.conn.Execute("SET SESSION time_zone = '+00:00';"); err != nil {
		return fmt.Errorf("error setting session time_zone: %w", err)
	}

	return nil
}

func queryTimeZone(conn *client.Conn) (string, error) {
	var tzName, err = queryStringVariable(conn, `SELECT @@GLOBAL.time_zone;`)
	if err != nil {
		return "", fmt.Errorf("error querying 'time_zone' system variable: %w", err)
	}
	logrus.WithField("time_zone", tzName).Debug("queried time_zone system variable")
	if tzName == "SYSTEM" {
		return "", errDatabaseTimezoneUnknown
	}

	return tzName, nil
}

func queryStringVariable(conn *client.Conn, query string) (string, error) {
	var results, err = conn.Execute(query)
	if err != nil {
		return "", fmt.Errorf("error executing query %q: %w", query, err)
	} else if len(results.Values) == 0 {
		return "", fmt.Errorf("no results from query %q", query)
	}

	var value = &results.Values[0][0]
	if value.Type == mysql.FieldValueTypeString {
		return string(value.AsString()), nil
	}
	return fmt.Sprintf("%s", value.Value()), nil
}

func (db *mysqlDatabase) Close(ctx context.Context) error {
	if err := db.conn.Close(); err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}
	return nil
}

func (db *mysqlDatabase) EmptySourceMetadata() sqlcapture.SourceMetadata {
	return &mysqlSourceInfo{}
}

func (db *mysqlDatabase) FallbackCollectionKey() []string {
	return []string{"/_meta/source/cursor"}
}

func encodeKeyFDB(key, ktype interface{}) (tuple.TupleElement, error) {
	if columnType, ok := ktype.(*mysqlColumnType); ok {
		return columnType.encodeKeyFDB(key)
	} else if typeName, ok := ktype.(string); ok {
		switch typeName {
		case "decimal":
			if val, ok := key.([]byte); ok {
				// TODO(wgd): This should probably be done in a more principled way, but
				// this is a viable placeholder solution.
				return strconv.ParseFloat(string(val), 64)
			}
		}
	}
	return key, nil
}

func decodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	switch v := t.(type) {
	case []byte:
		return string(v), nil
	}
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

func (db *mysqlDatabase) RequestTxIDs(schema, table string) {
	if db.includeTxIDs == nil {
		db.includeTxIDs = make(map[string]bool)
	}
	db.includeTxIDs[sqlcapture.JoinStreamID(schema, table)] = true
}

// mysqlSourceInfo is source metadata for data capture events.
type mysqlSourceInfo struct {
	sqlcapture.SourceCommon
	EventCursor string `json:"cursor" jsonschema:"description=Cursor value representing the current position in the binlog."`
	TxID        string `json:"txid,omitempty" jsonschema:"description=The global transaction identifier associated with a change by MySQL. Only set if GTIDs are enabled."`
}

func (s *mysqlSourceInfo) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}
