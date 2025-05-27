package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strings"
	"text/template"

	"github.com/estuary/connectors/go/common"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"

	_ "github.com/sijms/go-ora/v2"
)

var featureFlagDefaults = map[string]bool{
	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string         `json:"database" jsonschema:"default=ORCL,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '5m' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	DiscoverSchemas []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	SSLMode         string   `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
	FeatureFlags    string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`

	parsedFeatureFlags map[string]bool // Parsed feature flags setting with defaults applied
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
	if c.Advanced.PollSchedule != "" {
		if err := schedule.Validate(c.Advanced.PollSchedule); err != nil {
			return fmt.Errorf("invalid default polling schedule %q: %w", c.Advanced.PollSchedule, err)
		}
	}

	// Strictly speaking this feature-flag parsing isn't validation at all, but it's a convenient
	// method that we can be sure always gets called before the config is used.
	c.Advanced.parsedFeatureFlags = common.ParseFeatureFlags(c.Advanced.FeatureFlags, featureFlagDefaults)
	if c.Advanced.FeatureFlags != "" {
		log.WithField("flags", c.Advanced.parsedFeatureFlags).Info("parsed feature flags")
	}
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the Oracle
	// default 1521.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":1521"
	}

	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "5m"
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	var address = c.Address
	if c.NetworkTunnel.InUse() {
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
	var params = make(url.Values)
	if c.Advanced.SSLMode != "" {
		params.Set("sslmode", c.Advanced.SSLMode)
	}
	if len(params) > 0 {
		uri.RawQuery = params.Encode()
	}
	return uri.String()
}

func connectOracle(ctx context.Context, cfg *Config) (*sql.DB, error) {
	log.WithFields(log.Fields{
		"address":  cfg.Address,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")

	// If a network tunnel is configured, then try to start it before establishing connections.
	if cfg.NetworkTunnel.InUse() {
		if _, err := cfg.NetworkTunnel.Start(ctx, cfg.Address, "1521"); err != nil {
			return nil, err
		}
	}

	var db, err = sql.Open("oracle", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	} else if _, err := db.ExecContext(ctx, "SELECT 1 FROM dual"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return db, nil
}

// A discussion on the use of ORA_ROWSCN as cursor
//
// ORA_ROWSCN indicates the SCN (System Change Number) of the latest modification to a row, however
// it by default is an indicator of the "block" of the row, rather than the row itself, it means multiple rows can
// share a ROWSCN. This is fine for incremental capture as long as we ensure that we only commit a checkpoint after
// having read all rows with the same SCN.
// It follows that this column cannot be used as a primary key, but it can be used
// as a cursor.
const tableQueryTemplate = `{{if .IsFirstQuery -}}
  SELECT ORA_ROWSCN AS TXID, {{quoteTableName .Owner .TableName}}.* FROM {{quoteTableName .Owner .TableName}} ORDER BY ORA_ROWSCN
{{- else -}}
  SELECT ORA_ROWSCN AS TXID, {{quoteTableName .Owner .TableName}}.* FROM {{quoteTableName .Owner .TableName}}
    WHERE ORA_ROWSCN > :1
    ORDER BY ORA_ROWSCN
{{- end}}`

func selectQueryTemplate(res *Resource) (string, error) {
	if res.Template != "" {
		return res.Template, nil
	}
	return tableQueryTemplate, nil
}

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

var templateFuncs = template.FuncMap{
	"quoteTableName":  quoteTableName,
	"quoteIdentifier": quoteIdentifier,
}

func generateOracleResource(resourceName, owner, tableName, tableType string) (*Resource, error) {
	return &Resource{
		Name:      resourceName,
		Owner:     owner,
		TableName: tableName,
		Cursor:    []string{"TXID"},
	}, nil
}

func translateOracleValue(val any, databaseTypeName string) (any, error) {
	if val, ok := val.(float64); ok {
		if math.IsNaN(val) {
			return "NaN", nil
		} else if math.IsInf(val, +1) {
			return "Infinity", nil
		} else if math.IsInf(val, -1) {
			return "-Infinity", nil
		}
		return val, nil
	}
	return val, nil
}

var oracleDriver = &BatchSQLDriver{
	DocumentationURL:    "https://go.estuary.dev/source-oracle-batch",
	ConfigSchema:        generateConfigSchema(),
	Connect:             connectOracle,
	GenerateResource:    generateOracleResource,
	TranslateValue:      translateOracleValue,
	SelectQueryTemplate: selectQueryTemplate,
}

func generateConfigSchema() json.RawMessage {
	var configSchema, err = schemagen.GenerateSchema("Batch SQL", &Config{}).MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("generating endpoint schema: %w", err))
	}
	return json.RawMessage(configSchema)
}

func main() {
	boilerplate.RunMain(oracleDriver)
}
