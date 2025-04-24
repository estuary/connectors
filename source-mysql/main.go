package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"

	_ "time/tzdata"
)

var featureFlagDefaults = map[string]bool{
	// When true, date columns will be discovered as `type: string, format: date`
	// instead of simply `type: string`
	"date_schema_format": true,

	// When true, columns of type TINYINT(1) will be treated as booleans.
	"tinyint1_as_bool": false,

	// When set, discovered collection schemas will request that schema inference be
	// used _in addition to_ the full column/types discovery we already do.
	"use_schema_inference": false,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": false,
}

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
			return nil, err
		}
	}

	var featureFlags = common.ParseFeatureFlags(config.Advanced.FeatureFlags, featureFlagDefaults)
	if config.Advanced.FeatureFlags != "" {
		logrus.WithField("flags", featureFlags).Info("parsed feature flags")
	}

	// This is a bit of an ugly hack to allow us to specify a single _value_ as part of a feature flag.
	// The alternative would be that we'd have to add a visible advanced option for what is really just
	// an internal mechanism for us to use.
	var initialBackfillCursor string
	var forceResetCursor string
	for flag, value := range featureFlags {
		if strings.HasPrefix(flag, "initial_backfill_cursor=") && value {
			initialBackfillCursor = strings.TrimPrefix(flag, "initial_backfill_cursor=")
		}
		if strings.HasPrefix(flag, "force_reset_cursor=") && value {
			forceResetCursor = strings.TrimPrefix(flag, "force_reset_cursor=")
		}
	}

	var db = &mysqlDatabase{
		config:                &config,
		featureFlags:          featureFlags,
		initialBackfillCursor: initialBackfillCursor,
		forceResetCursor:      forceResetCursor,
	}
	if err := db.connect(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

// Config tells the connector how to connect to the source database and
// capture changes from it.
type Config struct {
	Address     string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User        string         `json:"user" jsonschema:"title=Login Username,default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password    string         `json:"password" jsonschema:"title=Login Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Timezone    string         `json:"timezone,omitempty" jsonschema:"title=Timezone,description=Timezone to use when capturing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read and columns with type datetime are being captured. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the 'time_zone' system variable if both are set (go.estuary.dev/80J6rX)." jsonschema_extras:"order=3"`
	HistoryMode bool           `json:"historyMode" jsonschema:"default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=4"`
	Advanced    advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	DBName                   string   `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SkipBinlogRetentionCheck bool     `json:"skip_binlog_retention_check,omitempty" jsonschema:"title=Skip Binlog Retention Sanity Check,default=false,description=Bypasses the 'dangerously short binlog retention' sanity check at startup. Only do this if you understand the danger and have a specific need."`
	NodeID                   uint32   `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value."`
	SkipBackfills            string   `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize        int      `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,description=The number of rows which should be fetched from the database in a single backfill query."`
	DiscoverSchemas          []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	FeatureFlags             string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`

	// Deprecated config options which no longer do much of anything.
	WatermarksTable   string `json:"watermarks_table,omitempty" jsonschema:"title=Watermarks Table Name,default=flow.watermarks,description=This property is deprecated and will be removed in the near future. Previously named the table to be used for watermark writes. Currently the only effect of this setting is to exclude the watermarks table from discovery if present."`
	HeartbeatInterval string `json:"heartbeat_interval,omitempty" jsonschema:"title=Heartbeat Interval,default=60s,description=This property is deprecated and will be removed in the near future. Has no effect." jsonschema_extras:"pattern=^[-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+$"`
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
		if _, err := schedule.ParseTimezone(c.Timezone); err != nil {
			return err
		}
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
		c.Advanced.NodeID &= 0x7FFFFFFF // Clear MSB for legacy reasons. Probably not necessary any longer but changing it would change the node ID.
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

func (db *mysqlDatabase) connect(_ context.Context) error {
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

	// The MySQL client library plumbing around net.Conn timeouts is hackish and terrible
	// and must not be trusted. However we still want to protect against hangs during the
	// initial connection process. Since all we're going to do if the connection hangs is
	// error out anyway, we can do this by cutting out the middleman and just setting up
	// a directly fatal-if-not-cancelled timer.
	var connectionTimer = time.AfterFunc(60*time.Second, func() {
		logrus.WithField("addr", address).Fatal("failed to connect before deadline")
	})
	defer connectionTimer.Stop()

	const mysqlErrorCodeSecureTransportRequired = 3159 // From https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
	var mysqlErr *mysql.MyError
	var withTLS = func(c *client.Conn) error {
		c.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
		return nil
	}

	// The following if-else chain looks somewhat complicated but it's really very simple.
	// * We'd prefer to use TLS, so we first try to connect with TLS, and then if that fails
	//   we try again without.
	// * If either error is an incorrect username/password then we just report that.
	// * Otherwise we report both errors because it's better to be clear what failed and how.
	// * Except if the non-TLS connection specifically failed because TLS is required then
	//   we don't need to mention that and just return the with-TLS error.
	if connWithTLS, errWithTLS := client.Connect(address, db.config.User, db.config.Password, db.config.Advanced.DBName, withTLS); errWithTLS == nil {
		logrus.WithField("addr", address).Info("connected with TLS")
		db.conn = &mysqlConnection{inner: connWithTLS, queryTimeout: DefaultQueryTimeout}
	} else if errors.As(errWithTLS, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		return cerrors.NewUserError(mysqlErr, "incorrect username or password")
	} else if connWithoutTLS, errWithoutTLS := client.Connect(address, db.config.User, db.config.Password, db.config.Advanced.DBName); errWithoutTLS == nil {
		logrus.WithField("addr", address).Info("connected without TLS")
		db.conn = &mysqlConnection{inner: connWithoutTLS, queryTimeout: DefaultQueryTimeout}
	} else if errors.As(errWithoutTLS, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		logrus.WithFields(logrus.Fields{"withTLS": errWithTLS, "nonTLS": errWithoutTLS}).Error("unable to connect to database")
		return cerrors.NewUserError(mysqlErr, "incorrect username or password")
	} else if errors.As(errWithoutTLS, &mysqlErr) && mysqlErr.Code == mysqlErrorCodeSecureTransportRequired {
		return fmt.Errorf("unable to connect to database: %w", errWithTLS)
	} else {
		return fmt.Errorf("unable to connect to database: failed both with TLS (%w) and without TLS (%w)", errWithTLS, errWithoutTLS)
	}
	connectionTimer.Stop() // Connection successful, cancel the timeout

	// Debug logging hook so we can get the server config variables when needed
	if err := db.logServerVariables(); err != nil {
		logrus.WithField("err", err).Warn("failed to log server variables")
	}

	if db.config.Timezone != "" {
		// The user-entered timezone value is verified to parse without error in (*Config).Validate,
		// so this parsing is not expected to fail.
		loc, err := schedule.ParseTimezone(db.config.Timezone)
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
		var err error
		if tzName, err = queryTimeZone(db.conn); err == nil {
			var loc *time.Location
			if loc, err = schedule.ParseTimezone(tzName); err == nil {
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

	if err := db.queryDatabaseVersion(); err != nil {
		logrus.WithField("err", err).Warn("failed to query database version")
	}

	return nil
}

func (db *mysqlDatabase) logServerVariables() error {
	if !logrus.IsLevelEnabled(logrus.DebugLevel) {
		return nil
	}

	var results, err = db.conn.Execute("SHOW VARIABLES;")
	if err != nil {
		return fmt.Errorf("unable to query server variables: %w", err)
	}
	defer results.Close()

	if len(results.Fields) != 2 {
		var fieldNames []string
		for _, field := range results.Fields {
			fieldNames = append(fieldNames, string(field.Name))
		}
		return fmt.Errorf("unexpected result columns: got %q", fieldNames)
	}

	var fields = make(logrus.Fields)
	for _, row := range results.Values {
		var key = string(row[0].AsString())
		var val = row[1].Value()
		if bs, ok := val.([]byte); ok {
			val = string(bs)
		}
		fields[key] = val
	}
	logrus.WithFields(fields).Debug("queried server variables")
	return nil
}

func queryTimeZone(conn mysqlClient) (string, error) {
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

func queryStringVariable(conn mysqlClient, query string) (string, error) {
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
	// As a special case, the solitary value '*.*' means that nothing should be
	// backfilled. This makes certain sorts of operation which would otherwise
	// require listing every single table in the database easier.
	//
	// Note that we are currently just matching that one specific value, it doesn't
	// generalize in the obvious way to 'foobar.*' or to being part of a comma-
	// separated list. This was done for expediency and should probably be thought
	// out more if/when we add this to the other SQL CDC connectors.
	if db.config.Advanced.SkipBackfills == "*.*" {
		return false
	}

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
