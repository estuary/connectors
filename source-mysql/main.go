package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	networkTunnel "github.com/estuary/connectors/go-network-tunnel"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"

	mysqlLog "github.com/siddontang/go-log/log"
)

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
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

func connectMySQL(ctx context.Context, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults()

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
	if config.NetworkTunnel != nil && config.NetworkTunnel.SshForwarding != nil && config.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		host, port, err := net.SplitHostPort(config.Address)
		if err != nil {
			return nil, fmt.Errorf("splitting address to host and port: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: config.NetworkTunnel.SshForwarding.SshEndpoint,
			PrivateKey:  []byte(config.NetworkTunnel.SshForwarding.PrivateKey),
			ForwardHost: host,
			ForwardPort: port,
			LocalPort:   "3306",
		}
		var tunnel = sshConfig.CreateTunnel()

		// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
		// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
		err = tunnel.Start()

		if err != nil {
			logrus.WithField("error", err).Error("network tunnel error")
		}
	}

	return &mysqlDatabase{config: &config}, nil
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
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"title=Login Username,default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"title=Login Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	WatermarksTable          string `json:"watermarks_table,omitempty" jsonschema:"title=Watermarks Table Name,default=flow.watermarks,description=The name of the table used for watermark writes. Must be fully-qualified in '<schema>.<table>' form."`
	DBName                   string `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SkipBinlogRetentionCheck bool   `json:"skip_binlog_retention_check,omitempty" jsonschema:"title=Skip Binlog Retention Sanity Check,default=false,description=Bypasses the 'dangerously short binlog retention' sanity check at startup. Only do this if you understand the danger and have a specific need."`
	NodeID                   uint32 `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value."`
	SkipBackfills            string `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize        int    `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=131072,description=The number of rows which should be fetched from the database in a single backfill query."`
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
	if c.Advanced.BackfillChunkSize <= 0 {
		c.Advanced.BackfillChunkSize = 128 * 1024
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
	config    *Config
	conn      *client.Conn
	explained map[string]struct{} // Tracks tables which have had an `EXPLAIN` run on them during this connector invocation
}

func (db *mysqlDatabase) Connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"addr":     db.config.Address,
		"dbName":   db.config.Advanced.DBName,
		"user":     db.config.User,
		"serverID": db.config.Advanced.NodeID,
	}).Info("initializing connector")

	var address = db.config.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if db.config.NetworkTunnel != nil && db.config.NetworkTunnel.SshForwarding != nil && db.config.NetworkTunnel.SshForwarding.SshEndpoint != "" {
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
		return fmt.Errorf("unable to connect to database: %w", err)
	}

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
	rdsRetentionHours, err := db.queryNumericVariable(`SELECT name, value FROM mysql.rds_configuration WHERE name = 'binlog retention hours';`)
	if err == nil {
		return time.Duration(rdsRetentionHours) * time.Hour, nil
	}

	// The newer 'binlog_expire_logs_seconds' variable takes priority if it exists and is nonzero.
	expireLogsSeconds, err := db.queryNumericVariable(`SHOW VARIABLES LIKE 'binlog_expire_logs_seconds';`)
	if err == nil && expireLogsSeconds > 0 {
		return time.Duration(expireLogsSeconds * float64(time.Second)), nil
	}

	// And as the final resort we'll check 'expire_logs_days' if 'seconds' was zero or nonexistent.
	expireLogsDays, err := db.queryNumericVariable(`SHOW VARIABLES LIKE 'expire_logs_days';`)
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

func (db *mysqlDatabase) queryNumericVariable(query string) (float64, error) {
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
		var n, err = strconv.ParseFloat(string(value.AsString()), 64)
		if err != nil {
			return 0, fmt.Errorf("couldn't parse string value as number: %w", err)
		}
		return n, nil
	case mysql.FieldValueTypeUnsigned:
		return float64(value.AsUint64()), nil
	case mysql.FieldValueTypeSigned:
		return float64(value.AsInt64()), nil
	}
	return value.AsFloat64(), nil
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
