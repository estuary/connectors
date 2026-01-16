package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
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

	mssqldb "github.com/microsoft/go-mssqldb"
)

func main() {
	boilerplate.RunMain(sqlserverDriver)
}

var sqlserverDriver = &sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-sqlserver",
	Connect:          connectSQLServer,
}

const defaultPort = "1433"

var featureFlagDefaults = map[string]bool{
	// When set, discovered collection schemas will request that schema inference be
	// used _in addition to_ the full column/types discovery we already do.
	"use_schema_inference": true,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,

	// When set, discovery queries will use a variant with all identifiers capitalized.
	// We believe this should probably be a safe change (and it's required for discovery
	// to work in the Turkish_CI_AS locale), but didn't want to release it unconditionally
	// on a Friday without much testing so it's gated behind a flag for now.
	"uppercase_discovery_queries": false,

	// When true, the capture will use a fence mechanism based on observing CDC worker runs
	// and LSN positions rather than the old watermark write mechanism.
	"read_only": true,

	// When true, the connector will tolerate missed changes in the CDC stream and will not
	// trigger an automatic re-backfill if changes go missing. This may be useful if the CDC
	// event data starts to expire before it can be captured, but should generally only be
	// needed in exceptional circumstances when recovering from some sort of major breakage.
	"tolerate_missed_changes": false,

	// Force use of the 'replica fence' mechanism, which is normally used automatically
	// when the target database is detected as a replica.
	"replica_fencing": false,

	// Discover ROWVERSION / TIMESTAMP column types as {type: string, format: base64}.
	// Captures without this setting will discover them as the catch-all type {} though
	// the actual value will be a base64 string either way.
	"discover_rowversion_as_bytes": false,
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address     string `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User        string `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password    string `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database    string `json:"database" jsonschema:"description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Timezone    string `json:"timezone,omitempty" jsonschema:"title=Time Zone,default=UTC,description=The IANA timezone name in which datetime columns will be converted to RFC3339 timestamps. Defaults to UTC if left blank." jsonschema_extras:"order=4"`
	HistoryMode bool   `json:"historyMode" jsonschema:"default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=5"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	DiscoverOnlyEnabled         bool   `json:"discover_only_enabled,omitempty" jsonschema:"title=Discover Only CDC-Enabled Tables,description=When set the connector will only discover tables which have already had CDC capture instances enabled."`
	SkipBackfills               string `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize           int    `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,description=The number of rows which should be fetched from the database in a single backfill query."`
	PollingInterval             string `json:"polling_interval,omitempty" jsonschema:"title=CDC Polling Interval,default=500ms,description=The interval at which the connector polls for CDC changes. Accepts duration strings like '500ms' or '30s' or '1m'. Defaults to 500ms when unspecified." jsonschema_extras:"pattern=^[0-9]+(ms|s|m|h)$"`
	AutomaticChangeTableCleanup bool   `json:"change_table_cleanup,omitempty" jsonschema:"title=Automatic Change Table Cleanup,default=false,description=When set the connector will delete CDC change table entries as soon as they are persisted into Flow. Requires DBO permissions to use."`
	AutomaticCaptureInstances   bool   `json:"capture_instance_management,omitempty" jsonschema:"title=Automatic Capture Instance Management,default=false,description=When set the connector will respond to alterations of captured tables by automatically creating updated capture instances and deleting the old ones. Requires DBO permissions to use."`
	Filegroup                   string `json:"filegroup,omitempty" jsonschema:"title=CDC Instance Filegroup,description=When set the connector will create new CDC instances with the specified 'filegroup_name' argument. Has no effect if CDC instances are managed manually."`
	RoleName                    string `json:"role_name,omitempty" jsonschema:"title=CDC Instance Access Role,description=When set the connector will create new CDC instances with the specified 'role_name' argument as the gating role. When unset the capture user name is used as the 'role_name' instead. Has no effect if CDC instances are managed manually."`
	SourceTag                   string `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document."`
	FeatureFlags                string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
	WatermarksTable             string `json:"watermarksTable,omitempty" jsonschema:"default=dbo.flow_watermarks,description=This property is deprecated for new captures as they will no longer use watermark writes by default. The name of the table used for watermark writes during backfills. Must be fully-qualified in '<schema>.<table>' form."`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Timezone != "" {
		if _, err := schedule.ParseTimezone(c.Timezone); err != nil {
			return err
		}
	}

	if c.Advanced.PollingInterval != "" {
		if parsedInterval, err := time.ParseDuration(c.Advanced.PollingInterval); err != nil {
			return fmt.Errorf("invalid 'polling_interval' configuration %q: %w", c.Advanced.PollingInterval, err)
		} else if parsedInterval < 100*time.Millisecond {
			return fmt.Errorf("invalid 'polling_interval' configuration %q: must be at least 100ms", c.Advanced.PollingInterval)
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
		c.Advanced.WatermarksTable = "dbo.flow_watermarks"
	}
	if c.Advanced.BackfillChunkSize <= 0 {
		c.Advanced.BackfillChunkSize = 50000
	}
	if c.Advanced.PollingInterval == "" {
		c.Advanced.PollingInterval = "500ms"
	}
	if c.Timezone == "" {
		c.Timezone = "UTC"
	}

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the MS SQL
	// default of 1433.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":" + defaultPort
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost
	// to the target via the bastion server, so we use the tunnel's address.
	var address = c.Address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:" + defaultPort
	}

	var params = make(url.Values)
	params.Add("app name", "Flow CDC Connector")
	params.Add("encrypt", "true")
	params.Add("TrustServerCertificate", "true")
	params.Add("database", c.Database)
	var connectURL = &url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.User, c.Password),
		Host:     address,
		RawQuery: params.Encode(),
	}
	return connectURL.String()
}

func configSchema() json.RawMessage {
	var schema = schemagen.GenerateSchema("SQL Server Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return json.RawMessage(configSchema)
}

func connectSQLServer(ctx context.Context, name string, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults()

	var featureFlags = common.ParseFeatureFlags(config.Advanced.FeatureFlags, featureFlagDefaults)
	if config.Advanced.FeatureFlags != "" {
		log.WithField("flags", featureFlags).Info("parsed feature flags")
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
			LocalPort:   defaultPort,
		}
		var tunnel = sshConfig.CreateTunnel()

		// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
		// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
		if err := tunnel.Start(); err != nil {
			return nil, err
		}
	}

	var db = &sqlserverDatabase{
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

type sqlserverDatabase struct {
	config *Config
	conn   *sql.DB

	featureFlags          map[string]bool // Parsed feature flag settings with defaults applied
	datetimeLocation      *time.Location  // The location in which to interpret DATETIME column values as timestamps.
	initialBackfillCursor string          // When set, this cursor will be used instead of the current WAL end when a backfill resets the cursor
	forceResetCursor      string          // When set, this cursor will be used instead of the checkpointed one regardless of backfilling. DO NOT USE unless you know exactly what you're doing.

	tableStatistics map[sqlcapture.StreamID]*sqlserverTableStatistics // Cached table statistics for backfill progress tracking
}

func (db *sqlserverDatabase) HistoryMode() bool {
	return db.config.HistoryMode
}

func (db *sqlserverDatabase) connect(ctx context.Context) error {
	log.WithFields(log.Fields{
		"address": db.config.Address,
		"user":    db.config.User,
	}).Info("connecting to database")

	var conn, err = sql.Open("sqlserver", db.config.ToURI())
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		var mssqlErr mssqldb.Error
		if errors.As(err, &mssqlErr) {
			switch mssqlErr.Number {
			case 18456:
				return cerrors.NewUserError(err, "incorrect username or password")
			case 4063:
				return cerrors.NewUserError(err, fmt.Sprintf("cannot open database %q: database does not exist or user %q does not have access", db.config.Database, db.config.User))
			}
		}
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	db.conn = conn

	loc, err := schedule.ParseTimezone(db.config.Timezone)
	if err != nil {
		return fmt.Errorf("invalid config timezone: %w", err)
	}
	log.WithFields(log.Fields{
		"tzName": db.config.Timezone,
		"loc":    loc.String(),
	}).Debug("using datetime location from config")
	db.datetimeLocation = loc

	return nil
}

// Close shuts down the database connection.
func (db *sqlserverDatabase) Close(ctx context.Context) error {
	if err := db.conn.Close(); err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}
	return nil
}

func (db *sqlserverDatabase) SourceMetadataSchema(writeSchema bool) *jsonschema.Schema {
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: writeSchema,
	}).Reflect(&sqlserverSourceInfo{})
	sourceSchema.Version = ""
	if db.config.Advanced.SourceTag == "" {
		sourceSchema.Properties.Delete("tag")
	}
	return sourceSchema
}

func (db *sqlserverDatabase) FallbackCollectionKey() []string {
	return []string{"/_meta/source/lsn", "/_meta/source/seqval"}
}

func (db *sqlserverDatabase) RequestTxIDs(schema, table string) {}
