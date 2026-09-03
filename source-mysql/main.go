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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/estuary/connectors/go/auth/iam"
	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/invopop/jsonschema"
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
	"use_schema_inference": true,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,

	// When set, schema/table names will no longer be normalized to lowercase. This applies
	// in two specific circumstances:
	//   - Recommended Collection Names
	//   - Internal StreamIDs
	"case_sensitive_table_names": false,

	// When true, all prerequisite checks (both global and per-table) are skipped. This is intended
	// as an escape hatch for situations where the checks themselves are buggy or do not apply.
	"skip_prerequisites": false,

	// When true, per-table prerequisite checks are skipped. Implied by `skip_prerequisites`.
	"skip_table_prerequisites": false,
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
	config.normalizeCredentials()

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

	// This is a bit hacky but it works fine since there's only ever one capture running at a time.
	sqlcapture.LowercaseStreamIDs = !featureFlags["case_sensitive_table_names"]
	sqlcapture.LowercaseRecommendedNames = !featureFlags["case_sensitive_table_names"]

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

type AuthType string

const (
	UserPassword AuthType = "UserPassword"
	AWSIAM       AuthType = "AWSIAM"
	GCPIAM       AuthType = "GCPIAM"
	AzureIAM     AuthType = "AzureIAM"
)

type UserPasswordConfig struct {
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=1"`
}

// CredentialsConfig is a flat union of the supported authentication methods,
// discriminated by AuthType. The JSON Schema presents it as a oneOf with one
// branch per method.
type CredentialsConfig struct {
	AuthType AuthType `json:"auth_type"`

	UserPasswordConfig
	iam.IAMConfig
}

func (CredentialsConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(UserPassword),
		schemagen.OneOfSubSchema("Password", UserPasswordConfig{}, string(UserPassword)),
		schemagen.OneOfSubSchema("AWS IAM", iam.AWSConfig{}, string(AWSIAM)),
		schemagen.OneOfSubSchema("Google Cloud IAM", iam.GCPConfig{}, string(GCPIAM)),
		schemagen.OneOfSubSchema("Azure IAM", iam.AzureConfig{}, string(AzureIAM)),
	)
}

func (c *CredentialsConfig) Validate() error {
	switch c.AuthType {
	case UserPassword:
		if c.Password == "" {
			return errors.New("missing 'password'")
		}
		return nil
	case AWSIAM, GCPIAM, AzureIAM:
		// The embedded IAMConfig has its own auth_type field which JSON decoding
		// leaves empty (the outer field shadows it), so sync it before ValidateIAM
		// switches on it.
		c.IAMConfig.AuthType = iam.AuthType(c.AuthType)
		return c.ValidateIAM()
	}
	return fmt.Errorf("unknown 'auth_type' %q", c.AuthType)
}

// Config tells the connector how to connect to the source database and
// capture changes from it.
type Config struct {
	Address     string             `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User        string             `json:"user" jsonschema:"title=Login Username,default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password    string             `json:"password,omitempty" jsonschema:"-"`
	Credentials *CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=2,x-iam-auth=true,x-iam-azure-scope=https://ossrdbms-aad.database.windows.net/.default"`
	Timezone    string             `json:"timezone,omitempty" jsonschema:"title=Timezone,description=Timezone to use when capturing datetime columns. Should normally be left blank to use the database's 'time_zone' system variable. Only required if the 'time_zone' system variable cannot be read and columns with type datetime are being captured. Must be a valid IANA time zone name or +HH:MM offset. Takes precedence over the 'time_zone' system variable if both are set (go.estuary.dev/80J6rX)." jsonschema_extras:"order=3,nonsensitive=true"`
	HistoryMode bool               `json:"historyMode" jsonschema:"default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=4,nonsensitive=true"`

	DiscoveryFilters discoveryFilters `json:"discoveryFilters,omitempty" jsonschema:"title=Discovery Filters,description=Options that restrict which tables are visible to discovery."`
	Advanced         advancedConfig   `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	DBName                   string   `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SkipBinlogRetentionCheck bool     `json:"skip_binlog_retention_check,omitempty" jsonschema:"title=Skip Binlog Retention Sanity Check,default=false,description=Bypasses the 'dangerously short binlog retention' sanity check at startup. Only do this if you understand the danger and have a specific need." jsonschema_extras:"nonsensitive=true"`
	NodeID                   uint32   `json:"node_id,omitempty" jsonschema:"title=Node ID,description=Node ID for the capture. Each node in a replication cluster must have a unique 32-bit ID. The specific value doesn't matter so long as it is unique. If unset or zero the connector will pick a value." jsonschema_extras:"nonsensitive=true"`
	SkipBackfills            string   `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	BackfillChunkSize        int      `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=50000,description=The number of rows which should be fetched from the database in a single backfill query." jsonschema_extras:"nonsensitive=true"`
	DiscoverSchemas          []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas." jsonschema_extras:"nonsensitive=true"`
	SourceTag                string   `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document." jsonschema_extras:"nonsensitive=true"`
	RediscoveryInterval      string   `json:"rediscovery_interval,omitempty" jsonschema:"title=Rediscovery Interval,default=15m,description=How often the connector re-runs discovery while a capture is running to notice schema changes and newly added tables. Accepts duration strings like '15m' or '1h'. Defaults to 15m when unspecified." jsonschema_extras:"pattern=^[0-9]+(ms|s|m|h)$"`
	FeatureFlags             string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
	StatementTimeout         string   `json:"statement_timeout,omitempty" jsonschema:"title=Statement Timeout,description=Overrides the default statement timeout used by the connector. The default of zero disables statement timeouts entirely.,enum=,enum=30s,enum=1m,enum=5m,enum=30m,default=" jsonschema_extras:"nonsensitive=true"`

	// Deprecated config options which no longer do much of anything.
	WatermarksTable   string `json:"watermarks_table,omitempty" jsonschema:"title=Watermarks Table Name,default=flow.watermarks,description=This property is deprecated and will be removed in the near future. Previously named the table to be used for watermark writes. Currently the only effect of this setting is to exclude the watermarks table from discovery if present."`
	HeartbeatInterval string `json:"heartbeat_interval,omitempty" jsonschema:"title=Heartbeat Interval,default=60s,description=This property is deprecated and will be removed in the near future. Has no effect." jsonschema_extras:"pattern=^[-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+$,nonsensitive=true"`
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Validation runs before normalizeCredentials, so both the legacy flat
	// password and the credentials union must be accepted here.
	var passwordInUse string
	if c.Credentials == nil {
		if c.Password == "" {
			return errors.New("missing 'credentials'")
		}
		passwordInUse = c.Password
	} else {
		if err := c.Credentials.Validate(); err != nil {
			return err
		}
		passwordInUse = c.Credentials.Password
	}
	if len(passwordInUse) > 32 {
		return fmt.Errorf("passwords used as part of replication cannot exceed 32 characters in length due to an internal limitation in MySQL: password length of %d characters is too long, please use a shorter password", len(passwordInUse))
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
	if c.Advanced.StatementTimeout != "" {
		if _, err := time.ParseDuration(c.Advanced.StatementTimeout); err != nil {
			return fmt.Errorf("invalid statement timeout %q: %w", c.Advanced.StatementTimeout, err)
		}
	}
	if err := sqlcapture.ValidateRediscoveryInterval(c.Advanced.RediscoveryInterval); err != nil {
		return err
	}
	if err := c.DiscoveryFilters.Validate(); err != nil {
		return err
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

// normalizeCredentials folds a legacy top-level password into c.Credentials as a
// UserPassword credential, so that code downstream of it only ever reads one
// credentials shape. When both are supplied the explicit credentials win.
func (c *Config) normalizeCredentials() {
	var legacyPassword = c.Password
	c.Password = ""

	if c.Credentials != nil {
		if legacyPassword != "" {
			logrus.Warn("both legacy 'password' and 'credentials' are set; the former will be ignored in favour of the latter")
		}
		return
	}
	c.Credentials = &CredentialsConfig{
		AuthType:           UserPassword,
		UserPasswordConfig: UserPasswordConfig{Password: legacyPassword},
	}
}

// EffectivePassword returns the secret to present as the MySQL password for the
// configured authentication method. It requires normalizeCredentials to have run,
// and only reads the credentials union. For AWS IAM the password is an RDS auth
// token minted here from the session credentials the control plane injects, valid
// for fifteen minutes, so callers should invoke this per connection attempt rather
// than caching the result. For Google Cloud and Azure IAM the password is an access
// token injected into the config by the control plane, which restarts the connector
// with a fresh token before expiry.
func (c *Config) EffectivePassword(ctx context.Context) (string, error) {
	switch c.Credentials.AuthType {
	case UserPassword:
		return c.Credentials.Password, nil
	case AWSIAM:
		credProvider, err := c.Credentials.AWSCredentialsProvider()
		if err != nil {
			return "", err
		}
		// The token is bound to the endpoint it will be presented to, so it's built
		// from the configured address rather than the network tunnel's local address.
		token, err := auth.BuildAuthToken(ctx, c.Address, c.Credentials.AWSRegion, c.User, credProvider)
		if err != nil {
			return "", fmt.Errorf("building AWS IAM auth token: %w", err)
		}
		return token, nil
	case GCPIAM:
		// Cloud SQL IAM database authentication presents this token via the
		// 'mysql_clear_password' plugin, which the server only allows over TLS.
		var token = c.Credentials.GoogleToken()
		if token == "" {
			return "", errors.New("missing 'gcp_access_token'")
		}
		return token, nil
	case AzureIAM:
		var token = c.Credentials.AzureToken()
		if token == "" {
			return "", errors.New("missing 'azure_access_token'")
		}
		return token, nil
	}
	return "", fmt.Errorf("unsupported 'auth_type' %q", c.Credentials.AuthType)
}

// requiresTLS reports whether the configured authentication method forbids falling
// back to an unencrypted connection.
func (c *Config) requiresTLS() bool {
	switch c.Credentials.AuthType {
	case UserPassword:
		return false
	default:
		return true
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

// From https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
const mysqlErrorCodeSecureTransportRequired = 3159

func withTLS(c *client.Conn) error {
	c.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
	return nil
}

func withAttrs(c *client.Conn) error {
	c.SetAttributes(map[string]string{"program_name": "Estuary source-mysql"})
	return nil
}

// connectTLSOnly establishes a TLS connection and never falls back to an
// unencrypted one. This is the IAM path.
func (db *mysqlDatabase) connectTLSOnly(address, password string) (*client.Conn, error) {
	var conn, err = client.Connect(address, db.config.User, password, db.config.Advanced.DBName, withTLS, withAttrs)
	if err == nil {
		logrus.WithField("addr", address).Info("connected with TLS")
		return conn, nil
	}
	var mysqlErr *mysql.MyError
	if errors.As(err, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		return nil, cerrors.NewUserError(mysqlErr, "incorrect username or password")
	}
	return nil, fmt.Errorf("unable to connect to database with TLS: %w", err)
}

// connectPreferringTLS tries TLS first and falls back to an unencrypted connection,
// which is acceptable for password authentication.
func (db *mysqlDatabase) connectPreferringTLS(address, password string) (*client.Conn, error) {
	// The following if-else chain looks somewhat complicated but it's really very simple.
	// * We'd prefer to use TLS, so we first try to connect with TLS, and then if that fails
	//   we try again without.
	// * If either error is an incorrect username/password then we just report that.
	// * Otherwise we report both errors because it's better to be clear what failed and how.
	// * Except if the non-TLS connection specifically failed because TLS is required then
	//   we don't need to mention that and just return the with-TLS error.
	var mysqlErr *mysql.MyError
	if connWithTLS, errWithTLS := client.Connect(address, db.config.User, password, db.config.Advanced.DBName, withTLS, withAttrs); errWithTLS == nil {
		logrus.WithField("addr", address).Info("connected with TLS")
		return connWithTLS, nil
	} else if errors.As(errWithTLS, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		return nil, cerrors.NewUserError(mysqlErr, "incorrect username or password")
	} else if connWithoutTLS, errWithoutTLS := client.Connect(address, db.config.User, password, db.config.Advanced.DBName, withAttrs); errWithoutTLS == nil {
		logrus.WithField("addr", address).Info("connected without TLS")
		return connWithoutTLS, nil
	} else if errors.As(errWithoutTLS, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		logrus.WithFields(logrus.Fields{"withTLS": errWithTLS, "nonTLS": errWithoutTLS}).Error("unable to connect to database")
		return nil, cerrors.NewUserError(mysqlErr, "incorrect username or password")
	} else if errors.As(errWithoutTLS, &mysqlErr) && mysqlErr.Code == mysqlErrorCodeSecureTransportRequired {
		return nil, fmt.Errorf("unable to connect to database: %w", errWithTLS)
	} else {
		return nil, fmt.Errorf("unable to connect to database: failed both with TLS (%w) and without TLS (%w)", errWithTLS, errWithoutTLS)
	}
}

func (db *mysqlDatabase) connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"addr":     db.config.Address,
		"dbName":   db.config.Advanced.DBName,
		"user":     db.config.User,
		"serverID": db.config.Advanced.NodeID,
	}).Info("connecting to database")

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

	var password, err = db.config.EffectivePassword(ctx)
	if err != nil {
		return err
	}

	var conn *client.Conn
	if db.config.requiresTLS() {
		conn, err = db.connectTLSOnly(address, password)
	} else {
		conn, err = db.connectPreferringTLS(address, password)
	}
	if err != nil {
		return err
	}
	db.conn = &mysqlConnection{inner: conn, queryTimeout: DefaultQueryTimeout}
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

	// Set collation to utf8mb4 for the connection so UTF-8 data is transferred correctly.
	// This should work on every MySQL/MariaDB version we care about, but just in case we
	// log a warning and continue (presumably with the default server setting) if it fails.
	if _, err := db.conn.Execute("SET NAMES utf8mb4"); err != nil {
		logrus.WithField("err", err).Warn("failed to set connection charset to utf8mb4")
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

	// Apply statement timeout. MySQL and MariaDB use different variable names
	// and units, so we need to handle them separately. The default of zero
	// disables statement timeouts, overriding any database-level default.
	var statementTimeout time.Duration
	if db.config.Advanced.StatementTimeout != "" {
		// The timeout string is validated in Config.Validate so this shouldn't fail.
		statementTimeout, _ = time.ParseDuration(db.config.Advanced.StatementTimeout)
	}

	var setTimeoutQuery string
	if db.versionProduct == "MariaDB" {
		// MariaDB uses max_statement_time in seconds (as a double)
		setTimeoutQuery = fmt.Sprintf("SET SESSION max_statement_time = %.3f;", statementTimeout.Seconds())
	} else {
		// MySQL uses max_execution_time in milliseconds
		setTimeoutQuery = fmt.Sprintf("SET SESSION max_execution_time = %d;", statementTimeout.Milliseconds())
	}

	if _, err := db.conn.Execute(setTimeoutQuery); err != nil {
		logrus.WithFields(logrus.Fields{
			"err":     err,
			"timeout": statementTimeout.String(),
			"query":   setTimeoutQuery,
		}).Warn("failed to set statement timeout")
	} else if statementTimeout > 0 {
		logrus.WithFields(logrus.Fields{
			"timeout": statementTimeout.String(),
			"product": db.versionProduct,
		}).Info("configured statement timeout")
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

func (db *mysqlDatabase) SourceMetadataSchema(writeSchema bool) *jsonschema.Schema {
	var sourceSchema = (&jsonschema.Reflector{
		ExpandedStruct:            true,
		DoNotReference:            true,
		AllowAdditionalProperties: writeSchema,
	}).Reflect(&mysqlSourceInfo{})
	sourceSchema.Version = ""
	if db.config.Advanced.SourceTag == "" {
		sourceSchema.Properties.Delete("tag")
	}
	return sourceSchema
}

func (db *mysqlDatabase) FallbackCollectionKey() []string {
	return []string{"/_meta/source/cursor"}
}

func (db *mysqlDatabase) RediscoveryInterval() time.Duration {
	return sqlcapture.ParseRediscoveryInterval(db.config.Advanced.RediscoveryInterval)
}

func (db *mysqlDatabase) ReplicationPollingInterval() time.Duration {
	return 0 // Binlog replication events arrive continuously rather than being polled for.
}

func (db *mysqlDatabase) ShouldBackfill(streamID sqlcapture.StreamID) bool {
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
			if strings.EqualFold(streamID.String(), skipStreamID) {
				return false
			}
		}
	}
	return true
}

func (db *mysqlDatabase) RequestTxIDs(schema, table string) {
	if db.includeTxIDs == nil {
		db.includeTxIDs = make(map[sqlcapture.StreamID]bool)
	}
	db.includeTxIDs[sqlcapture.JoinStreamID(schema, table)] = true
}
