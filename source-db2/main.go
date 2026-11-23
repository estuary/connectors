package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	log "github.com/sirupsen/logrus"

	_ "github.com/ibmdb/go_ibm_db"
)

func main() {
	boilerplate.RunMain(db2Driver)
}

var db2Driver = &db2CaptureDriver{Driver: sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-db2",
	Connect:          connectDB2,
}}

const defaultPort = "50000"

var featureFlagDefaults = map[string]common.FlagDefault{
	"use_schema_inference": common.FlagEnabled,
	"emit_sourced_schemas": common.FlagEnabled,
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address     string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached. The default port is 50000." jsonschema_extras:"order=0"`
	User        string         `json:"user,omitempty" jsonschema:"title=User,default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password    string         `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database    string         `json:"database" jsonschema:"title=Database,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	HistoryMode bool           `json:"historyMode,omitempty" jsonschema:"title=History Mode,default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=4,nonsensitive=true"`
	Advanced    advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	CaptureSchema     string   `json:"capture_schema,omitempty" jsonschema:"title=Capture Schema,default=ASN,description=The schema containing the SQL Replication control tables."`
	WatermarksTable   string   `json:"watermarksTable,omitempty" jsonschema:"title=Watermarks Table,description=The SQL Replication registered table used for watermark writes during backfills. Must be fully-qualified as '<schema>.<table>'. Defaults to '<USER>.FLOW_WATERMARKS'."`
	BackfillChunkSize int      `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,minimum=1,description=The number of rows fetched from the database in a single backfill query." jsonschema_extras:"nonsensitive=true"`
	PollingInterval   string   `json:"polling_interval,omitempty" jsonschema:"title=CDC Polling Interval,default=1s,description=The interval at which the connector polls for SQL Replication changes. Accepts duration strings like '1s' or '30s' or '1m'." jsonschema_extras:"nonsensitive=true"`
	DiscoverSchemas   []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If specified only tables in the selected schemas will be automatically discovered. Omit all entries to discover tables from all user schemas." jsonschema_extras:"nonsensitive=true"`
	Timezone          string   `json:"timezone,omitempty" jsonschema:"title=Time Zone,default=UTC,description=The IANA timezone name in which timestamp columns will be converted to RFC3339 timestamps. Defaults to UTC if left blank." jsonschema_extras:"nonsensitive=true"`
	SourceTag         string   `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document." jsonschema_extras:"nonsensitive=true"`
	FeatureFlags      string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"password", c.Password},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	for _, property := range [][]string{{"user", c.User}, {"database", c.Database}} {
		if strings.Contains(property[1], ";") {
			return fmt.Errorf("Db2 connection strings cannot carry ';' in the %s", property[0])
		}
	}
	if strings.Contains(c.Password, ";") {
		return fmt.Errorf("Db2 connection strings cannot carry ';' in the password; change the database user's password")
	}
	if c.Password != strings.TrimSpace(c.Password) {
		return fmt.Errorf("Db2 connection strings cannot carry leading or trailing whitespace in the password; change the database user's password")
	}
	var host, port, err = net.SplitHostPort(defaultAddress(c.Address))
	if err != nil || host == "" {
		return fmt.Errorf("invalid 'address' configuration %q: expected host or host:port", c.Address)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("invalid 'address' configuration %q: port must be between 1 and 65535", c.Address)
	}
	if c.Advanced.BackfillChunkSize < 0 {
		return fmt.Errorf("invalid 'backfill_chunk_size': must be greater than zero")
	}
	if c.Advanced.PollingInterval != "" {
		if interval, err := time.ParseDuration(c.Advanced.PollingInterval); err != nil {
			return fmt.Errorf("invalid 'polling_interval' configuration %q: %w", c.Advanced.PollingInterval, err)
		} else if interval <= 0 {
			return fmt.Errorf("invalid 'polling_interval' configuration %q: must be greater than zero", c.Advanced.PollingInterval)
		}
	}
	if c.Advanced.Timezone != "" {
		if _, err := schedule.ParseTimezone(c.Advanced.Timezone); err != nil {
			return fmt.Errorf("invalid 'timezone': %w", err)
		}
	}
	if c.Advanced.WatermarksTable != "" {
		var parts = strings.Split(c.Advanced.WatermarksTable, ".")
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return fmt.Errorf("invalid 'watermarksTable' configuration: table name %q must be fully-qualified as \"<schema>.<table>\"", c.Advanced.WatermarksTable)
		}
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	c.Address = defaultAddress(c.Address)
	if c.User == "" {
		c.User = "flow_capture"
	}
	if c.Advanced.CaptureSchema == "" {
		c.Advanced.CaptureSchema = "ASN"
	}
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = strings.ToUpper(c.User) + ".FLOW_WATERMARKS"
	}
	if c.Advanced.BackfillChunkSize == 0 {
		c.Advanced.BackfillChunkSize = 50000
	}
	if c.Advanced.PollingInterval == "" {
		c.Advanced.PollingInterval = "1s"
	}
	if c.Advanced.Timezone == "" {
		c.Advanced.Timezone = "UTC"
	}
}

func defaultAddress(address string) string {
	if !strings.Contains(address, ":") {
		return net.JoinHostPort(address, defaultPort)
	}
	if net.ParseIP(strings.Trim(address, "[]")) != nil {
		return net.JoinHostPort(strings.Trim(address, "[]"), defaultPort)
	}
	return address
}

// ToURI converts the Config to a Db2 semicolon-separated connection string.
func (c *Config) ToURI() string {
	var address = defaultAddress(c.Address)
	if c.NetworkTunnel.InUse() {
		address = "localhost:" + defaultPort
	}
	var host, port, _ = net.SplitHostPort(address)
	return fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s", host, port, c.Database, c.User, c.Password)
}

func configSchema() json.RawMessage {
	var schema, err = schemagen.GenerateSchema("Db2 Connection", &Config{}).MarshalJSON()
	if err != nil {
		panic(err)
	}
	return schema
}

func connectDB2(ctx context.Context, name string, cfg json.RawMessage, createdAt common.CreatedAt) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults()

	// Db2 supports case-sensitive quoted identifiers. Resource and stream names must agree.
	sqlcapture.LowercaseStreamIDs = false
	sqlcapture.LowercaseRecommendedNames = false
	var featureFlags = common.ResolveFlags(config.Advanced.FeatureFlags, featureFlagDefaults, createdAt)
	if config.Advanced.FeatureFlags != "" {
		log.WithField("flags", featureFlags).Info("parsed feature flags")
	}
	if config.NetworkTunnel.InUse() {
		if _, err := config.NetworkTunnel.Start(ctx, config.Address, defaultPort); err != nil {
			return nil, err
		}
	}

	var db = &db2Database{config: &config, featureFlags: featureFlags, name: name, now: time.Now}
	if err := db.connect(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

type db2Database struct {
	name                    string
	config                  *Config
	conn                    *sql.DB
	featureFlags            map[string]bool
	datetimeLocation        *time.Location
	activeTables            map[sqlcapture.StreamID]*tableReplicationInfo
	registrationError       error
	registrationChecksAfter time.Time
	bindingKeys             map[sqlcapture.StreamID][]string
	replication             *db2ReplicationStream
	now                     func() time.Time
}

var _ sqlcapture.Database = (*db2Database)(nil)

func (db *db2Database) connect(ctx context.Context) error {
	log.WithFields(log.Fields{
		"address":  db.config.Address,
		"user":     db.config.User,
		"database": db.config.Database,
	}).Info("connecting to database")

	var loc, err = schedule.ParseTimezone(db.config.Advanced.Timezone)
	if err != nil {
		return fmt.Errorf("invalid config timezone: %w", err)
	}
	var conn *sql.DB
	if conn, err = sql.Open("go_ibm_db", db.config.ToURI()); err != nil {
		return fmt.Errorf("error opening database connection: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		conn.Close()
		var errUpper = strings.ToUpper(err.Error())
		if strings.Contains(errUpper, "SQL30082N") && strings.Contains(errUpper, "USERNAME") {
			return cerrors.NewUserError(err, "incorrect username or password")
		}
		return fmt.Errorf("error pinging database: %w", err)
	}
	db.conn, db.datetimeLocation = conn, loc
	return nil
}

func (db *db2Database) Close(ctx context.Context) error {
	return db.conn.Close()
}

func (db *db2Database) HistoryMode() bool { return db.config.HistoryMode }

func (db *db2Database) ActivateStreamsBeforeCatchup() bool { return false }

func (db *db2Database) RediscoveryInterval() time.Duration { return 0 }

func (db *db2Database) ReplicationPollingInterval() time.Duration {
	var interval, _ = time.ParseDuration(db.config.Advanced.PollingInterval)
	return interval
}

func (db *db2Database) FallbackCollectionKey() []string {
	return []string{"/_meta/source/cursor", "/_meta/source/intent"}
}

func (db *db2Database) RequestTxIDs(schema, table string) {}

type db2SourceInfo struct {
	sqlcapture.SourceCommon
	Cursor string `json:"cursor" jsonschema:"description=The hexadecimal SQL Replication commit sequence for this change. Empty for backfills."`
	Intent string `json:"intent,omitempty" jsonschema:"description=The hexadecimal SQL Replication intent sequence for this change."`
	Tag    string `json:"tag,omitempty" jsonschema:"description=Optional source tag as defined in the endpoint configuration."`
}

func (s db2SourceInfo) Common() sqlcapture.SourceCommon { return s.SourceCommon }

func (db *db2Database) SourceMetadataSchema(writeSchema bool) *jsonschema.Schema {
	var schema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: writeSchema,
	}).Reflect(&db2SourceInfo{})
	schema.Version = ""
	if db.config.Advanced.SourceTag == "" {
		schema.Properties.Delete("tag")
	}
	return schema
}

func (db *db2Database) WatermarksTable() sqlcapture.StreamID {
	var parts = strings.SplitN(db.config.Advanced.WatermarksTable, ".", 2)
	return sqlcapture.JoinStreamID(parts[0], parts[1])
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}
