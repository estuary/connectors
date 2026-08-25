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

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/estuary/connectors/sqlcapture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sirupsen/logrus"
)

// CockroachDB speaks the PostgreSQL wire protocol but is not PostgreSQL: it has no logical
// replication (no replication slots, no `pgoutput`), so CDC is performed by consuming a
// sinkless ("core") changefeed streamed back over the SQL connection rather than by decoding
// a logical replication stream. See replication.go for the changefeed consumer.

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

var cockroachDriver = &sqlcapture.Driver{
	ConfigSchema:     configSchema(),
	DocumentationURL: "https://go.estuary.dev/source-cockroachdb",
	Connect:          connectCockroachDB,
}

func main() {
	boilerplate.RunMain(cockroachDriver)
}

func connectCockroachDB(ctx context.Context, name string, cfg json.RawMessage) (sqlcapture.Database, error) {
	var config Config
	if err := pf.UnmarshalStrict(cfg, &config); err != nil {
		return nil, fmt.Errorf("error parsing config json: %w", err)
	}
	config.SetDefaults()

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections.
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
			LocalPort:   "26257",
		}
		var tunnel = sshConfig.CreateTunnel()
		if err := tunnel.Start(); err != nil {
			return nil, err
		}
	}

	var featureFlags = common.ParseFeatureFlags(config.Advanced.FeatureFlags, featureFlagDefaults)
	if config.Advanced.FeatureFlags != "" {
		logrus.WithField("flags", featureFlags).Info("parsed feature flags")
	}

	var db = &cockroachdbDatabase{
		config:       &config,
		featureFlags: featureFlags,
	}
	if err := db.connect(ctx); err != nil {
		return nil, err
	}
	return db, nil
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address     string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User        string         `json:"user" jsonschema:"title=User,description=The database user to authenticate as.,default=flow_capture" jsonschema_extras:"order=1"`
	Password    string         `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database    string         `json:"database" jsonschema:"title=Database,default=defaultdb,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	HistoryMode bool           `json:"historyMode" jsonschema:"default=false,description=Capture change events without reducing them to a final state." jsonschema_extras:"order=4"`
	Advanced    advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	SSLMode           string   `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
	ResolvedInterval  string   `json:"resolved_interval,omitempty" jsonschema:"title=Changefeed Resolved Interval,description=How frequently the changefeed should emit resolved-timestamp checkpoints. Smaller values reduce end-to-end latency at the cost of slightly more overhead.,default=1s"`
	BackfillChunkSize int      `json:"backfill_chunk_size,omitempty" jsonschema:"title=Backfill Chunk Size,default=250000,description=The number of rows which should be fetched from the database in a single backfill query."`
	SkipBackfills     string   `json:"skip_backfills,omitempty" jsonschema:"title=Skip Backfills,description=A comma-separated list of fully-qualified table names which should not be backfilled."`
	DiscoverSchemas   []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	SourceTag         string   `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document."`
	FeatureFlags      string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
	StatementTimeout  string   `json:"statement_timeout,omitempty" jsonschema:"title=Statement Timeout,description=Overrides the default statement timeout used by the connector. The default of zero disables statement timeouts entirely.,enum=,enum=30s,enum=1m,enum=5m,enum=30m,default="`

	RediscoveryInterval string `json:"rediscovery_interval,omitempty" jsonschema:"title=Rediscovery Interval,default=15m,description=How often the connector re-runs discovery while a capture is running to notice schema changes and newly added tables. Accepts duration strings like '15m' or '1h'. Defaults to 15m when unspecified." jsonschema_extras:"pattern=^[0-9]+(ms|s|m|h)$"`
}

var featureFlagDefaults = map[string]bool{
	// When set, discovered collection schemas will request that schema inference be
	// used _in addition to_ the full column/types discovery we already do.
	"use_schema_inference": true,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,

	// When true, all prerequisite checks are skipped. This is intended as an escape hatch
	// for situations where the checks themselves are buggy or do not apply.
	"skip_prerequisites": false,
}

// Validate checks that the configuration possesses all required properties.
func (c *Config) Validate() error {
	if c.Address == "" {
		return errors.New("missing 'address'")
	}
	if c.User == "" {
		return errors.New("missing 'user'")
	}
	// A password is required even against an insecure test cluster (which ignores it),
	// so that the common managed-cluster case always has a credential available.
	if c.Password == "" {
		return errors.New("missing 'password'")
	}
	if c.Advanced.SSLMode != "" {
		if !slices.Contains([]string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}, c.Advanced.SSLMode) {
			return fmt.Errorf("invalid 'sslmode' configuration: unknown setting %q", c.Advanced.SSLMode)
		}
	}
	if c.Advanced.SkipBackfills != "" {
		for _, skipStreamID := range strings.Split(c.Advanced.SkipBackfills, ",") {
			if !strings.Contains(skipStreamID, ".") {
				return fmt.Errorf("invalid 'skipBackfills' configuration: table name %q must be fully-qualified as \"<schema>.<table>\"", skipStreamID)
			}
		}
	}
	for _, s := range c.Advanced.DiscoverSchemas {
		if len(s) == 0 {
			return fmt.Errorf("discovery schema selection must not contain any entries that are blank: To discover tables from all schemas, remove all entries from the discovery schema selection list. To discover tables only from specific schemas, provide their names as non-blank entries")
		}
	}
	if c.Advanced.ResolvedInterval != "" {
		if _, err := time.ParseDuration(c.Advanced.ResolvedInterval); err != nil {
			return fmt.Errorf("invalid resolved interval %q: %w", c.Advanced.ResolvedInterval, err)
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
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	if c.Advanced.BackfillChunkSize <= 0 {
		// Larger than the PostgreSQL connector's 50k default on purpose: each backfill chunk is
		// followed by a fence, and a CockroachDB changefeed fence costs several seconds (it can
		// only advance on the cluster's closed-timestamp cadence) versus sub-millisecond for a
		// PostgreSQL watermark write. Fewer, larger chunks therefore dramatically reduce backfill
		// wall-clock time on CockroachDB without affecting correctness.
		c.Advanced.BackfillChunkSize = 250000
	}
	if c.Advanced.ResolvedInterval == "" {
		c.Advanced.ResolvedInterval = "1s"
	}

	// The address config property should accept a host or host:port value, and if the
	// port is unspecified it should be the CockroachDB default 26257.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":26257"
	}
}

// ToURI converts the Config to a DSN connection string.
func (c *Config) ToURI() string {
	var address = c.Address
	// If an SSH tunnel is configured we connect to the local forwarded port instead.
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:26257"
	}

	var uri = url.URL{
		Scheme: "postgres",
		Host:   address,
		User:   url.UserPassword(c.User, c.Password),
	}
	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	var params = make(url.Values)
	params.Set("application_name", "estuary_flow")
	if c.Advanced.SSLMode != "" {
		params.Set("sslmode", c.Advanced.SSLMode)
	}
	if len(params) > 0 {
		uri.RawQuery = params.Encode()
	}
	return uri.String()
}

func configSchema() json.RawMessage {
	var schema = schemagen.GenerateSchema("CockroachDB Connection", &Config{})
	var configSchema, err = schema.MarshalJSON()
	if err != nil {
		panic(err)
	}
	return json.RawMessage(configSchema)
}

func (db *cockroachdbDatabase) connect(ctx context.Context) error {
	logrus.WithFields(logrus.Fields{
		"address":  db.config.Address,
		"user":     db.config.User,
		"database": db.config.Database,
	}).Info("connecting to database")

	var config, err = pgxpool.ParseConfig(db.config.ToURI())
	if err != nil {
		return fmt.Errorf("error parsing database uri: %w", err)
	}
	if config.ConnConfig.ConnectTimeout == 0 {
		config.ConnConfig.ConnectTimeout = 20 * time.Second
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Default to no statement timeout so that backfill queries are never interrupted,
		// unless the user has explicitly configured one.
		var statementTimeout = "0"
		if db.config.Advanced.StatementTimeout != "" {
			if timeout, err := time.ParseDuration(db.config.Advanced.StatementTimeout); err == nil {
				statementTimeout = fmt.Sprintf("%d", timeout.Milliseconds())
			}
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %s;", statementTimeout)); err != nil {
			logrus.WithFields(logrus.Fields{"err": err, "timeout": statementTimeout}).Debug("failed to set statement_timeout")
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
	db.conn = pool
	return nil
}
