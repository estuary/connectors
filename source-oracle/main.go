package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/go/common"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	_ "github.com/sijms/go-ora/v2"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

var featureFlagDefaults = map[string]bool{
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

var oracleDriver = &sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-oracle",
	Connect:          connectOracle,
}

// The standard library `time.RFC3339Nano` is wrong for historical reasons, this
// format string is better because it always uses 9-digit fractional seconds, and
// thus it can be sorted lexicographically as bytes.
const sortableRFC3339Nano = "2006-01-02T15:04:05.000000000Z07:00"

func main() {
	boilerplate.RunMain(oracleDriver)
}

func connectOracle(ctx context.Context, name string, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults(name)

	var featureFlags = common.ParseFeatureFlags(config.Advanced.FeatureFlags, featureFlagDefaults)
	if config.Advanced.FeatureFlags != "" {
		log.WithField("flags", featureFlags).Info("parsed feature flags")
	}

	var db = &oracleDatabase{
		config:       &config,
		featureFlags: featureFlags,
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
			LocalPort:   "1521",
		}
		tunnel := sshConfig.CreateTunnel()

		// FIXME/question: do we need to shut down the tunnel manually if it is a child process?
		// at the moment tunnel.Stop is not being called anywhere, but if the connector shuts down, the child process also shuts down.
		if err := tunnel.Start(); err != nil {
			return nil, err
		}
	}

	if err := db.connect(ctx); err != nil {
		return nil, err
	}

	var isContainerDatabase string
	var row = db.conn.QueryRowContext(ctx, "SELECT CDB FROM V$DATABASE")
	if err := row.Scan(&isContainerDatabase); err != nil {
		return nil, fmt.Errorf("querying CDB status: %w", err)
	}

	if isContainerDatabase == "YES" {
		var containerName string
		var row = db.conn.QueryRowContext(ctx, "SELECT SYS_CONTEXT('USERENV', 'CON_NAME') AS CONTAINER_NAME FROM DUAL")

		if err := row.Scan(&containerName); err != nil {
			return nil, fmt.Errorf("querying current container name: %w", err)
		} else if containerName != "CDB$ROOT" && containerName != "DATABASE" {
			db.pdbName = containerName
		}
	}

	return db, nil
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address     string `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User        string `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password    string `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database    string `json:"database" jsonschema:"default=ORCL,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	HistoryMode bool   `json:"historyMode" jsonschema:"default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=5"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	SkipBackfills        string   `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	WatermarksTable      string   `json:"watermarksTable,omitempty" jsonschema:"description=The name of the table used for watermark writes during backfills. Must be fully-qualified in '<schema>.<table>' form."`
	BackfillChunkSize    int      `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,description=The number of rows which should be fetched from the database in a single backfill query."`
	IncrementalChunkSize int      `json:"incremental_chunk_size,omitempty" jsonschema:"title=Incremental Chunk Size,default=10000,description=The number of rows which should be fetched from the database in a single incremental query."`
	IncrementalSCNRange  int      `json:"incremental_scn_range,omitempty" jsonschema:"title=Incremental SCN Range,default=50000,description=The SCN range captured at every iteration."`
	DiscoverSchemas      []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	NodeID               uint32   `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value."`
	DictionaryMode       string   `json:"dictionary_mode,omitempty" jsonschema:"title=Dictionary Mode,description=How should dictionaries be used in Logminer: one of online or extract. When using online mode schema changes to the table may break the capture but resource usage is limited. When using extract mode schema changes are handled gracefully but more resources of your database (including disk) are used by the process. Defaults to smart which automatically switches between the two modes based on requirements.,enum=extract,enum=online,enum=smart"`
	FeatureFlags         string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
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

	if !slices.Contains([]string{"", DictionaryModeExtract, DictionaryModeOnline, DictionaryModeSmart}, c.Advanced.DictionaryMode) {
		return fmt.Errorf("dictionary mode must be one of %s or %s or %s", DictionaryModeExtract, DictionaryModeOnline, DictionaryModeSmart)
	}

	return nil
}

const (
	DictionaryModeExtract = "extract"
	DictionaryModeOnline  = "online"
	DictionaryModeSmart   = "smart"
)

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults(name string) {
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = strings.ToLower(c.User) + ".flow_watermarks"
	}
	if c.Advanced.BackfillChunkSize <= 0 {
		c.Advanced.BackfillChunkSize = 50000
	}
	if c.Advanced.IncrementalChunkSize <= 0 {
		c.Advanced.IncrementalChunkSize = 10000
	}
	if c.Advanced.IncrementalSCNRange <= 0 {
		c.Advanced.IncrementalSCNRange = 50000
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

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the Oracle
	// default 1521.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":1521"
	}

	if c.Advanced.DictionaryMode == "" {
		c.Advanced.DictionaryMode = DictionaryModeSmart
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI(prefetchRows int) string {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:1521"
	}
	var uri = url.URL{
		Scheme: "oracle",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	uri.RawQuery = fmt.Sprintf("PREFETCH_ROWS=%d", prefetchRows)
	return uri.String()
}

func configSchema() json.RawMessage {
	var schema = schemagen.GenerateSchema("Oracle Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return json.RawMessage(configSchema)
}

type oracleDatabase struct {
	config             *Config
	conn               *sql.DB
	pdbName            string                           // name of the Pluggable Database we are in, if this is a container Oracle instance
	explained          map[sqlcapture.StreamID]struct{} // Tracks tables which have had an `EXPLAIN` run on them during this connector invocation
	includeTxIDs       map[sqlcapture.StreamID]bool     // Tracks which tables should have XID properties in their replication metadata
	tableObjectMapping map[string]tableObject           // A mapping from streamID to objectID, dataObjectID
	featureFlags       map[string]bool                  // Parsed feature flag settings with defaults applied
}

func (db *oracleDatabase) IsRDS() bool {
	return strings.Contains(db.config.Address, "rds.amazonaws.com")
}

func (db *oracleDatabase) HistoryMode() bool {
	return db.config.HistoryMode
}

// This function is a no-op if there is no PDB name configured
func (db *oracleDatabase) switchToCDB(ctx context.Context) error {
	if db.pdbName == "" {
		return nil
	}

	if _, err := db.conn.ExecContext(ctx, "ALTER SESSION SET CONTAINER=CDB$ROOT"); err != nil {
		return fmt.Errorf("switching to CDB: %w", err)
	}

	return nil
}

// This function is a no-op if there is no PDB name configured
func (db *oracleDatabase) switchToPDB(ctx context.Context) error {
	if db.pdbName == "" {
		return nil
	}

	if _, err := db.conn.ExecContext(ctx, fmt.Sprintf("ALTER SESSION SET CONTAINER=%s", db.pdbName)); err != nil {
		return fmt.Errorf("switching to PDB %s: %w", db.pdbName, err)
	}

	return nil
}

func (db *oracleDatabase) connect(_ context.Context) error {
	logrus.WithFields(logrus.Fields{
		"address":        db.config.Address,
		"user":           db.config.User,
		"database":       db.config.Database,
		"dictionaryMode": db.config.Advanced.DictionaryMode,
	}).Info("initializing connector")

	var conn, err = sql.Open("oracle", db.config.ToURI(db.config.Advanced.BackfillChunkSize+1))
	if err != nil {
		return fmt.Errorf("unable to connect to database: %w", err)
	}
	db.conn = conn
	return nil
}

func (db *oracleDatabase) Close(ctx context.Context) error {
	if err := db.conn.Close(); err != nil {
		return fmt.Errorf("error closing database connection: %w", err)
	}

	return nil
}

func (db *oracleDatabase) EmptySourceMetadata() sqlcapture.SourceMetadata {
	return &oracleSource{}
}

func (db *oracleDatabase) FallbackCollectionKey() []string {
	return []string{"/_meta/source/rs_id", "/_meta/source/ssn"}
}

func encodeKeyFDB(key any, colType oracleColumnType) (tuple.TupleElement, error) {
	switch key := key.(type) {
	case [16]uint8:
		var id, err = uuid.FromBytes(key[:])
		if err != nil {
			return nil, fmt.Errorf("error parsing uuid: %w", err)
		}
		return id.String(), nil
	case time.Time:
		return key.Format(sortableRFC3339Nano), nil
	case string:
		if colType.JsonType == "integer" {
			return strconv.Atoi(key)
		} else if colType.JsonType == "number" {
			return strconv.ParseFloat(key, 64)
		}

		if colType.Format == "integer" {
			// prepend zeros so that string represented numbers are lexicographically consistent
			var leadingZeros = strings.Repeat("0", int(colType.Precision)-len(key))
			if key[0] == '-' {
				key = "-" + leadingZeros + key[1:]
			} else {
				key = leadingZeros + key
			}
		}
		return key, nil
	default:
		return key, nil
	}
}

func decodeKeyFDB(t tuple.TupleElement) (interface{}, error) {
	return t, nil
}

func (db *oracleDatabase) ShouldBackfill(streamID string) bool {
	// Allow the setting "*.*" to skip backfilling any tables.
	if db.config.Advanced.SkipBackfills == "*.*" {
		return false
	}

	if db.config.Advanced.SkipBackfills != "" {
		// This repeated splitting is a little inefficient, but this check is done at
		// most once per table during connector startup and isn't really worth caching.
		for _, skipTableName := range strings.Split(db.config.Advanced.SkipBackfills, ",") {
			if strings.EqualFold(streamID, skipTableName) {
				return false
			}
		}
	}
	return true
}

func (db *oracleDatabase) RequestTxIDs(schema, table string) {
	if db.includeTxIDs == nil {
		db.includeTxIDs = make(map[sqlcapture.StreamID]bool)
	}
	db.includeTxIDs[sqlcapture.JoinStreamID(schema, table)] = true
}

func quoteColumnName(name string) string {
	var u = strings.ToUpper(name)
	if slices.Contains(reservedWords, u) {
		return `"` + name + `"`
	}
	if name == u {
		return name
	}
	return `"` + name + `"`
}
