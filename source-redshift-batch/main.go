package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	log "github.com/sirupsen/logrus"

	_ "github.com/jackc/pgx/v4/stdlib"
)

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string         `json:"database" jsonschema:"default=dev,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	// TODO(wgd): Add network tunnel support
}

type advancedConfig struct {
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	DiscoverSchemas []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	SSLMode         string   `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
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
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the Redshift
	// default 5439.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":5439"
	}

	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "24h"
	}
}

// ToURI converts the Config to a DSN string.
func (c *Config) ToURI() string {
	var address = c.Address
	var uri = url.URL{
		Scheme: "postgres",
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

func connectRedshift(ctx context.Context, cfg *Config) (*sql.DB, error) {
	log.WithFields(log.Fields{
		"address":  cfg.Address,
		"user":     cfg.User,
		"database": cfg.Database,
	}).Info("connecting to database")

	var db, err = sql.Open("pgx", cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	} else if _, err := db.ExecContext(ctx, "SELECT true;"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return db, nil
}

const redshiftQueryTemplate = `{{if .CursorFields -}}
  {{- if .IsFirstQuery -}}
    SELECT * FROM %[1]s
  {{- else -}}
    SELECT * FROM %[1]s
	{{- range $i, $k := $.CursorFields -}}
	  {{- if eq $i 0}} WHERE ({{else}}) OR ({{end -}}
      {{- range $j, $n := $.CursorFields -}}
		{{- if lt $j $i -}}
		  {{$n}} = ${{add $j 1}} AND {{end -}}
	  {{- end -}}
	  {{$k}} > ${{add $i 1}}
	{{- end -}}
	) 
  {{- end}} ORDER BY {{range $i, $k := $.CursorFields}}{{if gt $i 0}}, {{end}}{{$k}}{{end -}};
{{- else -}}
  SELECT * FROM %[1]s;
{{- end}}
`

func quoteTableName(schema, table string) string {
	return fmt.Sprintf(`"%s"."%s"`, schema, table)
}

func generateRedshiftResource(resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	var queryTemplate string
	if strings.EqualFold(tableType, "BASE TABLE") {
		queryTemplate = fmt.Sprintf(redshiftQueryTemplate, quoteTableName(schemaName, tableName))
	} else {
		return nil, fmt.Errorf("discovery will not autogenerate resource configs for entities of type %q, but you may add them manually", tableType)
	}

	return &Resource{
		Name:     resourceName,
		Template: queryTemplate,
	}, nil
}

func translateRedshiftValue(val any, databaseTypeName string) (any, error) {
	if val, ok := val.([]byte); ok {
		switch {
		case strings.EqualFold(databaseTypeName, "JSON"):
			return json.RawMessage(val), nil
		case strings.EqualFold(databaseTypeName, "JSONB"):
			return json.RawMessage(val), nil
		}
	}
	return val, nil
}

var redshiftDriver = &BatchSQLDriver{
	DocumentationURL: "https://go.estuary.dev/source-redshift-batch",
	ConfigSchema:     generateConfigSchema(),
	Connect:          connectRedshift,
	GenerateResource: generateRedshiftResource,
	TranslateValue:   translateRedshiftValue,
}

func generateConfigSchema() json.RawMessage {
	var configSchema, err = schemagen.GenerateSchema("Batch SQL", &Config{}).MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("generating endpoint schema: %w", err))
	}
	return json.RawMessage(configSchema)
}

func main() {
	boilerplate.RunMain(redshiftDriver)
}
