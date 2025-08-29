package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	iam "github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/invopop/jsonschema"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

var postgresDriver = &sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-postgresql",
	Connect:          connectPostgres,
}

// The standard library `time.RFC3339Nano` is wrong for historical reasons, this
// format string is better because it always uses 9-digit fractional seconds, and
// thus it can be sorted lexicographically as bytes.
const sortableRFC3339Nano = "2006-01-02T15:04:05.000000000Z07:00"

func main() {
	boilerplate.RunMain(postgresDriver)
}

func connectPostgres(ctx context.Context, name string, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults()

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
			LocalPort:   "5432",
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

	var db = &postgresDatabase{
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

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address     string            `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User        string            `json:"user" jsonschema:"title=User,description=The database user to authenticate as.,default=flow_capture" jsonschema_extras:"order=1"`
	Password    string            `json:"password,omitempty" jsonschema:"-"`
	Database    string            `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from." jsonschema_extras:"order=2"`
	HistoryMode bool              `json:"historyMode" jsonschema:"default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=3"`
	Credentials *credentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=4,x-iam-auth=true"`
	Advanced    advancedConfig    `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type authType string

const (
	UserPassword authType = "UserPassword"
	GCPIAM authType = "GCPIAM"
	AWSIAM authType = "AWSIAM"
	AzureIAM authType = "AzureIAM"
)

type userPassword struct {
	Password string `json:"password" jsonschema:"title=Password,description=Database user's password" jsonschema_extras:"secret=true"`
}

type credentialConfig struct {
	AuthType authType `json:"auth_type"`

	userPassword
	iam.IAMConfig
}

func (credentialConfig) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("Password", userPassword{}, string(UserPassword)),
	}
	subSchemas = append(subSchemas, (iam.IAMConfig{}).OneOfSubSchemas()...)
	
	schema := schemagen.OneOfSchema("Authentication", "", "auth_type", string(UserPassword), subSchemas...)

	return schema
}

type advancedConfig struct {
	PublicationName       string   `json:"publicationName,omitempty" jsonschema:"default=flow_publication,description=The name of the PostgreSQL publication to replicate from."`
	SlotName              string   `json:"slotName,omitempty" jsonschema:"default=flow_slot,description=The name of the PostgreSQL replication slot to replicate from."`
	WatermarksTable       string   `json:"watermarksTable,omitempty" jsonschema:"default=public.flow_watermarks,description=The name of the table used for watermark writes during backfills. Must be fully-qualified in '<schema>.<table>' form."`
	SkipBackfills         string   `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize     int      `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,description=The number of rows which should be fetched from the database in a single backfill query."`
	SSLMode               string   `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
	DiscoverSchemas       []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	DiscoverOnlyPublished bool     `json:"discover_only_published,omitempty" jsonschema:"title=Discover Only Published Tables,description=When set the capture will only discover tables which have already been added to the publication. This can be useful if you intend to manage which tables are captured by adding or removing them from the publication."`
	MinimumBackfillXID    string   `json:"min_backfill_xid,omitempty" jsonschema:"title=Minimum Backfill XID,description=Only backfill rows with XMIN values greater (in a 32-bit modular comparison) than the specified XID. Helpful for reducing re-backfill data volume in certain edge cases." jsonschema_extras:"pattern=^[0-9]+$"`
	ReadOnlyCapture       bool     `json:"read_only_capture,omitempty" jsonschema:"title=Read-Only Capture,description=When set the capture will operate in read-only mode and avoid operations such as watermark writes. This comes with some tradeoffs; consult the connector documentation for more information."`
	CaptureAsPartitions   bool     `json:"capture_as_partitions,omitempty" jsonschema:"title=Capture Partitioned Tables As Partitions,description=When set the capture will discover and capture partitioned tables as individual partitions rather than as a single root table. This requires the publication to be created without 'publish_via_partition_root'."`
	FeatureFlags          string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

var featureFlagDefaults = map[string]bool{
	// When set, discovered collection schemas will request that schema inference be
	// used _in addition to_ the full column/types discovery we already do.
	"use_schema_inference": true,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,

	// When true, DATE columns are captured as YYYY-MM-DD dates. Historically these
	// used to be captured as RFC3339 timestamps, which is usually not what users expect.
	"date_as_date": true,

	// When true, TIME WITHOUT TIME ZONE columns (also known as TIME) are captured as strings
	// satisfying `format: time`. Historically these used to be captured as Unix microseconds,
	// which is usually not what users expect.
	"time_as_time": true,

	// When true, array columns are captured as an actual multidimensional array of values.
	// Confusingly, this is not the opposite of the 'flatten_arrays' flag. There are three
	// different ways we can handle arrays:
	//
	// - multidimensional_arrays=false and flatten_arrays=false: The legacy behavior, in
	//   which arrays are captured as a `{dimensions, elements}` object which combines a
	//   dimensions array (of integers) with a flattened elements array (of the array type).
	//   This is the only way that we can represent PostgreSQL arrays with specific JSON
	//   schema value types _and_ without discarding dimensionality information, but it's
	//   a pain to actually use the resulting data for any purpose.
	//
	// - multidimensional_arrays=false and flatten_arrays=true: The current default behavior,
	//   in which arrays are captured as just the `elements` array. This preserves JSON schema
	//   value types, but discards the dimensionality information in favor of being easier to
	//   use in the common case (1D arrays, or arrays where the dimensionality is unimportant).
	//
	// - multidimensional_arrays=true: A new opt-in behavior, in which arrays are captured as
	//   actual nested arrays as necessary to represent a specific value. The problem is, since
	//   PostgreSQL array dimensionality can vary wildly on a per-row basis, the JSON schema type
	//   for this would be disgusting and not actually usable by a materialization, so instead we
	//   just schematize this as `{type: array}` without any information about element types or
	//   the fact that elements might themselves be either scalars or arrays of the same type.
	"multidimensional_arrays": false,

	// When true (and so long as 'multidimensional_arrays' isn't also set) array columns are
	// captured as a flat array of values in the JSON output.
	"flatten_arrays": true,

	// When true, the connector will manage replication slot creation and deletion automatically.
	// This is the default, but advanced users could set `no_create_replication_slot` if they want
	// to manage their replication slots manually instead.
	"create_replication_slot": true,
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	if c.Address == "" {
		return errors.New("missing 'address'")
	}
	if c.User == "" {
		return errors.New("missing 'user'")
	}

	if c.Credentials != nil {
		switch c.Credentials.AuthType {
		case UserPassword:
			if c.Credentials.Password == "" {
				return errors.New("missing 'password'")
			}
		default:
			if err := c.Credentials.ValidateIAM(); err != nil {
				return err
			}
		}
	}

	// Connection poolers like pgbouncer or the Supabase one are not supported. They will silently
	// ignore the `replication=database` option, meaning that captures can never be successful, and
	// they can introduce other weird failure modes which aren't worth trying to fix given that core
	// incompatibility.
	//
	// It might be useful to add a generic "are we connected via a pooler" heuristic if we can figure
	// one out. Until then all we can do is check for specific address patterns that match common ones
	// we see people trying to use. Even once we have a generic check this is probably still useful,
	// since it lets us name the specific thing they're using and give instructions for precisely how
	// to get the correct address instead.
	if strings.HasSuffix(c.Address, ".pooler.supabase.com:6543") || strings.HasSuffix(c.Address, ".pooler.supabase.com") {
		return cerrors.NewUserError(nil, fmt.Sprintf("address must be a direct connection: address %q is using the Supabase connection pooler, consult go.estuary.dev/supabase-direct-address for details", c.Address))
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
	if c.Advanced.SSLMode != "" {
		if !slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, c.Advanced.SSLMode) {
			return fmt.Errorf("invalid 'sslmode' configuration: unknown setting %q", c.Advanced.SSLMode)
		}
	}
	for _, s := range c.Advanced.DiscoverSchemas {
		if len(s) == 0 {
			return fmt.Errorf("discovery schema selection must not contain any entries that are blank: To discover tables from all schemas, remove all entries from the discovery schema selection list. To discover tables only from specific schemas, provide their names as non-blank entries")
		}
	}

	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.Advanced.SlotName == "" {
		c.Advanced.SlotName = "flow_slot"
	}
	if c.Advanced.PublicationName == "" {
		c.Advanced.PublicationName = "flow_publication"
	}
	if c.Advanced.WatermarksTable == "" {
		c.Advanced.WatermarksTable = "public.flow_watermarks"
	}
	if c.Advanced.BackfillChunkSize <= 0 {
		c.Advanced.BackfillChunkSize = 50000
	}

	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the PostgreSQL
	// default 5432.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":5432"
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI(ctx context.Context) (string, error) {
	var address = c.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:5432"
	}

	var user = c.User
	var pass = c.Password
	if c.Credentials != nil {
		switch c.Credentials.AuthType {
		case UserPassword:
			pass = c.Credentials.Password
		case AWSIAM:
			var err error
			pass, err = auth.BuildAuthToken(
				ctx,
				c.Address,
				c.Credentials.AWSRegion,
				user,
				c.Credentials.AWSCredentialsProvider(),
			)
			if err != nil {
				return "", fmt.Errorf("building AWS auth token: %w", err)
			}
		case GCPIAM:
			pass = c.Credentials.GoogleToken()
		case AzureIAM:
			pass = c.Credentials.AzureToken()
		}
	}

	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(user, pass),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	var params = make(url.Values)
	params.Set("application_name", "estuary_flow")

	// Set SSL mode - user configuration takes precedence, then cloud IAM defaults
	if c.Advanced.SSLMode != "" {
		params.Set("sslmode", c.Advanced.SSLMode)
	} else if c.Credentials != nil && (c.Credentials.AuthType == GCPIAM || c.Credentials.AuthType == AzureIAM) {
		// Enable SSL for cloud provider IAM connections by default when not explicitly set
		params.Set("sslmode", "require")
	}
	if len(params) > 0 {
		uri.RawQuery = params.Encode()
	}
	return uri.String(), nil
}

func configSchema() json.RawMessage {
	var schema = schemagen.GenerateSchema("PostgreSQL Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return json.RawMessage(configSchema)
}

type postgresDatabase struct {
	config          *Config
	conn            *pgxpool.Pool
	explained       map[sqlcapture.StreamID]struct{} // Tracks tables which have had an `EXPLAIN` run on them during this connector invocation
	includeTxIDs    map[sqlcapture.StreamID]bool     // Tracks which tables should have XID properties in their replication metadata
	tablesPublished map[sqlcapture.StreamID]bool     // Tracks which tables are part of the configured publication

	tableStatistics map[sqlcapture.StreamID]*postgresTableStatistics // Tracks table statistics for the current connector invocation

	featureFlags          map[string]bool // Parsed feature flag settings with defaults applied
	initialBackfillCursor string          // When set, this cursor will be used instead of the current WAL end when a backfill resets the cursor
	forceResetCursor      string          // When set, this cursor will be used instead of the checkpointed one regardless of backfilling. DO NOT USE unless you know exactly what you're doing.
}

func (db *postgresDatabase) HistoryMode() bool {
	return db.config.HistoryMode
}

func (db *postgresDatabase) connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"address":  db.config.Address,
		"user":     db.config.User,
		"database": db.config.Database,
		"slot":     db.config.Advanced.SlotName,
	}).Info("initializing connector")

	var connURI, err = db.config.ToURI(ctx)
	if err != nil {
		return fmt.Errorf("error generating connectiong URL: %w", err)
	}
	config, err := pgxpool.ParseConfig(connURI)
	if err != nil {
		return fmt.Errorf("error parsing database uri: %w", err)
	}
	if config.ConnConfig.ConnectTimeout == 0 {
		config.ConnConfig.ConnectTimeout = 20 * time.Second
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Register custom datatype handling details.
		if err := registerDatatypeTweaks(ctx, conn, conn.TypeMap()); err != nil {
			return err
		}

		// Attempt (non-fatal) to set the statement_timeout parameter to zero so backfill
		// queries never get interrupted.
		if _, err := conn.Exec(ctx, "SET statement_timeout = 0;"); err != nil {
			logrus.WithField("err", err).Debug("failed to set statement_timeout = 0")
		}

		// Ask the database never to use parallel workers for our queries. In general they shouldn't
		// be something we need, and sometimes the query planner makes really bad decisions when they
		// are used, most notably when it sees a simple `WHERE ctid > $1 AND ctid <= $2` keyless backfill
		// query and decides to use parallel workers scanning the entire table instead of a TID Range Scan.
		if _, err := conn.Exec(ctx, "SET max_parallel_workers_per_gather TO 0"); err != nil {
			logrus.WithField("err", err).Warn("error attempting to disable parallel workers")
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		var pgErr *pgconn.PgError

		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "28P01":
				return cerrors.NewUserError(err, "incorrect username or password")
			case "3D000":
				return cerrors.NewUserError(err, fmt.Sprintf("database %q does not exist", db.config.Database))
			case "42501":
				return cerrors.NewUserError(err, fmt.Sprintf("user %q does not have CONNECT privilege to database %q", db.config.User, db.config.Database))
			}
		}

		return fmt.Errorf("unable to connect to database: %w", err)
	}

	// Since we attempted in the AfterConnect function to set statement_timeout = 0, we
	// should log an informational message if it's now nonzero.
	var statementTimeout string
	if err := pool.QueryRow(ctx, "SHOW statement_timeout").Scan(&statementTimeout); err != nil {
		logrus.WithField("err", err).Warn("failed to query statement_timeout")
	} else if statementTimeout != "0" {
		logrus.WithField("timeout", statementTimeout).Info("nonzero statement_timeout")
	}

	db.conn = pool
	return nil
}

func (db *postgresDatabase) Close(ctx context.Context) error {
	db.conn.Close()
	return nil
}

func (db *postgresDatabase) SourceMetadataSchema(writeSchema bool) *jsonschema.Schema {
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: writeSchema,
	}).Reflect(&postgresSource{})
	sourceSchema.Version = ""
	return sourceSchema
}

func (db *postgresDatabase) FallbackCollectionKey() []string {
	return []string{"/_meta/source/loc/0", "/_meta/source/loc/1", "/_meta/source/loc/2"}
}

func (db *postgresDatabase) ShouldBackfill(streamID sqlcapture.StreamID) bool {
	// Allow the setting "*.*" to skip backfilling any tables.
	if db.config.Advanced.SkipBackfills == "*.*" {
		return false
	}

	if db.config.Advanced.SkipBackfills != "" {
		// This repeated splitting is a little inefficient, but this check is done at
		// most once per table during connector startup and isn't really worth caching.
		for _, skipStreamID := range strings.Split(db.config.Advanced.SkipBackfills, ",") {
			if strings.EqualFold(streamID.String(), skipStreamID) {
				return false
			}
		}
	}
	return true
}

func (db *postgresDatabase) RequestTxIDs(schema, table string) {
	if db.includeTxIDs == nil {
		db.includeTxIDs = make(map[sqlcapture.StreamID]bool)
	}
	db.includeTxIDs[sqlcapture.JoinStreamID(schema, table)] = true
}
