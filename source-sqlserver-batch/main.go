package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"text/template"
	"time"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	mssqldb "github.com/microsoft/go-mssqldb"
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
	Database string         `json:"database" jsonschema:"description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`

	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	DiscoverViews   bool     `json:"discover_views,omitempty" jsonschema:"title=Discover Views,description=When set views will be automatically discovered as resources. If unset only tables will be discovered."`
	Timezone        string   `json:"timezone,omitempty" jsonschema:"title=Time Zone,default=UTC,description=The IANA timezone name in which datetime columns will be converted to RFC3339 timestamps. Defaults to UTC if left blank."`
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	DiscoverSchemas []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
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
	// value, and if the port is unspecified it should be the SQL Server
	// default 1433.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":1433"
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
		address = "localhost:1433"
	}

	var params = make(url.Values)
	params.Add("app name", "Flow Batch Connector")
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

func connectSQLServer(ctx context.Context, cfg *Config) (*sql.DB, error) {
	log.WithFields(log.Fields{
		"address":  cfg.Address,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")

	// If a network tunnel is configured, then try to start it before establishing connections.
	if cfg.NetworkTunnel.InUse() {
		if _, err := cfg.NetworkTunnel.Start(ctx, cfg.Address, "1433"); err != nil {
			return nil, err
		}
	}

	var db, err = sql.Open("sqlserver", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		var mssqlErr mssqldb.Error
		if errors.As(err, &mssqlErr) {
			switch mssqlErr.Number {
			case 18456:
				return nil, cerrors.NewUserError(err, "incorrect username or password")
			case 4063:
				return nil, cerrors.NewUserError(err, fmt.Sprintf("cannot open database %q: database does not exist or user %q does not have access", cfg.Database, cfg.User))
			}
		}
		return nil, fmt.Errorf("error pinging database: %w", err)
	} else if _, err := db.ExecContext(ctx, "SELECT 1;"); err != nil {
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

const tableQueryTemplateCursor = `{{if .CursorFields -}}
  {{- if .IsFirstQuery -}}
    SELECT * FROM {{quoteTableName .SchemaName .TableName}}
  {{- else -}}
    SELECT * FROM {{quoteTableName .SchemaName .TableName}}
	{{- range $i, $k := $.CursorFields -}}
	  {{- if eq $i 0}} WHERE ({{else}}) OR ({{end -}}
      {{- range $j, $n := $.CursorFields -}}
		{{- if lt $j $i -}}
		  {{$n}} = @p{{add $j 1}} AND {{end -}}
	  {{- end -}}
	  {{$k}} > @p{{add $i 1}}
	{{- end -}}
	) 
  {{- end}} ORDER BY {{range $i, $k := $.CursorFields}}{{if gt $i 0}}, {{end}}{{$k}}{{end -}};
{{- else -}}
  SELECT * FROM {{quoteTableName .SchemaName .TableName}};
{{- end}}
`

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

var templateFuncs = template.FuncMap{
	"add":             func(a, b int) int { return a + b },
	"quoteTableName":  quoteTableName,
	"quoteIdentifier": quoteIdentifier,
}

func generateSQLServerResource(cfg *Config, resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	if strings.EqualFold(tableType, "BASE TABLE") || (strings.EqualFold(tableType, "VIEW") && cfg.Advanced.DiscoverViews) {
		return &Resource{
			Name:       resourceName,
			SchemaName: schemaName,
			TableName:  tableName,
		}, nil
	}
	return nil, fmt.Errorf("unsupported entity type %q", tableType)
}

func translateSQLServerValue(cfg *Config, val any, databaseTypeName string) (any, error) {
	switch strings.ToUpper(databaseTypeName) {
	case "UNIQUEIDENTIFIER":
		if bs, ok := val.([]byte); ok {
			// Correct Microsoft's insane fieldwise-little-endian UUID representation.
			bs[0], bs[1], bs[2], bs[3] = bs[3], bs[2], bs[1], bs[0]
			bs[4], bs[5] = bs[5], bs[4]
			bs[6], bs[7] = bs[7], bs[6]
			var u, err = uuid.FromBytes(bs)
			if err != nil {
				return nil, fmt.Errorf("error parsing uniqueidentifier: %w", err)
			}
			return u.String(), nil
		}
		return val, nil
	case "DECIMAL", "MONEY", "SMALLMONEY":
		if bs, ok := val.([]byte); ok {
			return string(bs), nil
		}
		return val, nil
	case "DATE":
		if t, ok := val.(time.Time); ok {
			return t.Format("2006-01-02"), nil
		}
		return val, nil
	case "TIME":
		if t, ok := val.(time.Time); ok {
			return t.Format("15:04:05.9999999Z07:00"), nil
		}
		return val, nil
	case "DATETIMEOFFSET":
		// The DATETIMEOFFSET column type includes an actual UTC timezone offset, so we
		// should leave that in place and just serialize it appropriately.
		if t, ok := val.(time.Time); ok {
			return t.Format(time.RFC3339Nano), nil
		}
		return val, nil
	case "DATETIME", "DATETIME2", "SMALLDATETIME":
		if t, ok := val.(time.Time); ok {
			// The SQL Server DATETIME column types don't have any innate time zone information.
			// We receive these as Go time.Time values in the UTC location, and want to reinterpret
			// the same YYYY-MM-DD HH:MM:SS.NNN values in the desired location instead.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), cfg.Advanced.datetimeLocation)
			return t.Format(time.RFC3339Nano), nil
		}
		return val, nil
	}
	return val, nil
}

var sqlserverDriver = &BatchSQLDriver{
	DocumentationURL:    "https://go.estuary.dev/source-sqlserver-batch",
	ConfigSchema:        generateConfigSchema(),
	Connect:             connectSQLServer,
	GenerateResource:    generateSQLServerResource,
	TranslateValue:      translateSQLServerValue,
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
	boilerplate.RunMain(sqlserverDriver)
}
