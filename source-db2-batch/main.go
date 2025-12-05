package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"

	_ "github.com/ibmdb/go_ibm_db"
)

var featureFlagDefaults = map[string]bool{
	// No feature flags yet
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"title=User,default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string         `json:"database" jsonschema:"title=Database,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	DiscoverViews   bool     `json:"discover_views,omitempty" jsonschema:"title=Discover Views,default=false,description=When set views will be automatically discovered as resources. If unset only tables will be discovered."`
	Timezone        string   `json:"timezone,omitempty" jsonschema:"title=Time Zone,default=UTC,description=The IANA timezone name in which datetime columns will be converted to RFC3339 timestamps. Defaults to UTC if left blank."`
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	DiscoverSchemas []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	SourceTag       string   `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document."`
	FeatureFlags    string   `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`

	parsedFeatureFlags map[string]bool // Parsed feature flags setting with defaults applied
	datetimeLocation   *time.Location  // Parsed location in which DATETIME column values will be interpreted.
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
	if c.Advanced.PollSchedule != "" {
		if err := schedule.Validate(c.Advanced.PollSchedule); err != nil {
			return fmt.Errorf("invalid default polling schedule %q: %w", c.Advanced.PollSchedule, err)
		}
	}

	if c.Advanced.Timezone == "" {
		c.Advanced.datetimeLocation = time.UTC
	} else if loc, err := schedule.ParseTimezone(c.Advanced.Timezone); err != nil {
		return err
	} else {
		c.Advanced.datetimeLocation = loc
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
	// value, and if the port is unspecified it should be the DB2
	// default 50000.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":50000"
	}

	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "24h"
	}

	if c.Advanced.Timezone == "" {
		c.Advanced.Timezone = "UTC"
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost
	// to the target via the bastion server, so we use the tunnel's address.
	var address = c.Address
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:50000"
	}

	// DB2 uses a semicolon-separated connection string format
	var host, port string
	if idx := strings.LastIndex(address, ":"); idx != -1 {
		host = address[:idx]
		port = address[idx+1:]
	} else {
		host = address
		port = "50000"
	}

	return fmt.Sprintf("HOSTNAME=%s;PORT=%s;DATABASE=%s;UID=%s;PWD=%s",
		host, port, c.Database, c.User, c.Password)
}

func connectDB2(ctx context.Context, cfg *Config) (*sql.DB, error) {
	log.WithFields(log.Fields{
		"address":  cfg.Address,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")

	// If a network tunnel is configured, then try to start it before establishing connections.
	if cfg.NetworkTunnel.InUse() {
		if _, err := cfg.NetworkTunnel.Start(ctx, cfg.Address, "50000"); err != nil {
			return nil, err
		}
	}

	var db, err = sql.Open("go_ibm_db", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		// SQL30082N is DB2's error code for security processing failures, with
		// various sub-reasons. Check for the specific username/password error.
		var errUpper = strings.ToUpper(err.Error())
		if strings.Contains(errUpper, "SQL30082N") && strings.Contains(errUpper, "USERNAME") {
			return nil, cerrors.NewUserError(err, "incorrect username or password")
		}
		return nil, fmt.Errorf("error pinging database: %w", err)
	} else if _, err := db.ExecContext(ctx, "SELECT 1 FROM SYSIBM.SYSDUMMY1"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return db, nil
}

func selectQueryTemplate(res *Resource) (string, error) {
	if res.Template != "" {
		return res.Template, nil
	}
	return tableQueryTemplateCursor, nil
}

const tableQueryTemplateCursor = `
{{- if not .CursorFields -}}
  SELECT * FROM {{quoteTableName .SchemaName .TableName}};
{{- else -}}
  {{- if .IsFirstQuery -}}
    SELECT * FROM {{quoteTableName .SchemaName .TableName}}
  {{- else -}}
    SELECT * FROM {{quoteTableName .SchemaName .TableName}}
    {{- range $i, $k := $.CursorFields -}}
      {{- if eq $i 0}} WHERE ({{else}}) OR ({{end -}}
      {{- range $j, $n := $.CursorFields -}}
        {{- if lt $j $i -}}{{quoteIdentifier $n}} = @flow_cursor_value[{{$j}}] AND {{end -}}
      {{- end -}}
      {{quoteIdentifier $k}} > @flow_cursor_value[{{$i}}]
    {{- end -}})
  {{- end}} ORDER BY {{range $i, $k := $.CursorFields}}{{if gt $i 0}}, {{end}}{{quoteIdentifier $k}}{{end -}};
{{- end -}}
`

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

var templateFuncs = template.FuncMap{
	"add":             func(a, b int) int { return a + b },
	"quoteTableName":  quoteTableName,
	"quoteIdentifier": quoteIdentifier,
}

func generateDB2Resource(cfg *Config, resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	// DB2 SYSCAT.TABLES TYPE values:
	//   'T' = Table (untyped)
	//   'U' = Typed table
	//   'V' = View (untyped)
	//   'W' = Typed view
	// We include tables unconditionally and views only when DiscoverViews is enabled.
	var isTable = strings.EqualFold(tableType, "T") || strings.EqualFold(tableType, "U")
	var isView = strings.EqualFold(tableType, "V") || strings.EqualFold(tableType, "W")

	if isTable || (isView && cfg.Advanced.DiscoverViews) {
		return &Resource{
			Name:       resourceName,
			SchemaName: schemaName,
			TableName:  tableName,
		}, nil
	}
	if isView {
		return nil, fmt.Errorf("views require discover_views option to be enabled")
	}
	return nil, fmt.Errorf("entity type %q not supported for discovery", tableType)
}

func translateDB2Value(cfg *Config, val any, databaseTypeName string) (any, error) {
	switch strings.ToUpper(databaseTypeName) {
	case "CHAR", "VARCHAR", "CLOB", "GRAPHIC", "VARGRAPHIC", "DBCLOB", "XML":
		// String types may come through as []byte, convert to string
		if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
	case "DECIMAL":
		if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
	case "DECFLOAT":
		if bs, ok := val.([]byte); ok {
			var s = string(bs)
			if s == "sNaN" {
				// Convert the Db2 signalling NaN value `"sNaN"` into plain `"NaN"` for
				// output since Flow {format: number} validation has no such concept.
				return "NaN", nil
			}
			return s, nil
		}
	case "DATE":
		// DATE should be formatted as YYYY-MM-DD per JSON Schema "date" format
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01-02"), nil
		}
	case "TIME":
		// TIME should be formatted as bare HH:MM:SS without time zone offset.
		// DB2 TIME does not have fractional seconds
		if t, ok := val.(time.Time); ok {
			return t.Format("15:04:05"), nil
		}
	case "TIMESTAMP":
		// TIMESTAMP values are timezone-naive. We interpret them in the configured
		// timezone location and then format as RFC3339.
		if t, ok := val.(time.Time); ok {
			// The driver gives us a time.Time in UTC representing the literal
			// YYYY-MM-DD HH:MM:SS.NNNNNN value from the database. We reinterpret
			// this in the configured location.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(),
				t.Nanosecond(), cfg.Advanced.datetimeLocation)
			return t.Format(time.RFC3339Nano), nil
		}
	}
	return val, nil
}

// translateDB2Cursor translates values specifically for cursor persistence and round-tripping.
// Cursor values need to be in a format that DB2 will accept in a WHERE clause comparison.
func translateDB2Cursor(cfg *Config, val any, databaseTypeName string) (any, error) {
	switch strings.ToUpper(databaseTypeName) {
	case "BINARY", "VARBINARY", "BLOB":
		// Binary types cannot be used as cursor columns because []byte is serialized as
		// base64 in JSON, and when deserialized it becomes a string. When passed back to
		// DB2 as a query parameter, the string type doesn't match the expected binary type,
		// resulting in SQLSTATE=22005 "Error in assignment".
		//
		// This could be fixed someday by translating a binary value to a JSON object such
		// as `{"type": "binary", "data": "<base64>"}` and adding logic to translate such
		// a tagged object back into bytes when loading a state checkpoint, but it's not
		// clear whether anyone actually needs binary columns as cursors anyway.
		return nil, fmt.Errorf("binary column types (%s) cannot be used as cursor columns", databaseTypeName)
	case "DATE":
		// DB2 DATE cursor format: YYYY-MM-DD
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01-02"), nil
		}
	case "TIME":
		// DB2 TIME cursor format: HH:MM:SS
		if t, ok := val.(time.Time); ok {
			return t.Format("15:04:05"), nil
		}
	case "TIMESTAMP":
		// DB2 TIMESTAMP cursor format: YYYY-MM-DD HH:MM:SS.NNNNNN
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01-02 15:04:05.000000"), nil
		}
	}
	return translateDB2Value(cfg, val, databaseTypeName)
}

var db2Driver = &BatchSQLDriver{
	DocumentationURL:    "https://go.estuary.dev/source-db2-batch",
	ConfigSchema:        generateConfigSchema(),
	Connect:             connectDB2,
	GenerateResource:    generateDB2Resource,
	TranslateValue:      translateDB2Value,
	TranslateCursor:     translateDB2Cursor,
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
	boilerplate.RunMain(db2Driver)
}
