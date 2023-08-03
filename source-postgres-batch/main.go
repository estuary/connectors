package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"

	_ "github.com/jackc/pgx/v4/stdlib"
)

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string         `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	// TODO(wgd): Add network tunnel support
}

type advancedConfig struct {
	SSLMode string `json:"sslmode,omitempty" jsonschema:"title=SSL Mode,description=Overrides SSL connection behavior by setting the 'sslmode' parameter.,enum=disable,enum=allow,enum=prefer,enum=require,enum=verify-ca,enum=verify-full"`
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
	return nil
}

// SetDefaults fills in the default values for unset optional parameters.
func (c *Config) SetDefaults() {
	// The address config property should accept a host or host:port
	// value, and if the port is unspecified it should be the PostgreSQL
	// default 5432.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":5432"
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

func connectPostgres(ctx context.Context, configJSON json.RawMessage) (*sql.DB, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(configJSON, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

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
	}
	return db, nil
}

const tableQueryTemplateTemplate = `{{if .IsFirstQuery -}}
  SELECT xmin, * FROM %[1]s;
{{- else -}}
  SELECT xmin, * FROM %[1]s WHERE xmin::text::bigint > $1;
{{- end}}`

// This is currently unused, but is a useful example of how the function
// generatePostgresResource might be extended to support view discovery.
const viewQueryTemplateTemplate = `{{if .IsFirstQuery -}}
  {{- /* This query will be executed first, without cursor values */ -}}
  SELECT * FROM %[1]s;
{{- else -}}
  {{- /**************************************************************
       * Subsequently (if cursor columns are listed in the resource *
	   * config) this query will be executed with the value(s) from *
	   * the last row of the previous query provided as parameters. *
	   *                                                            *
	   * Consult the connector documentation for examples.          *
       **************************************************************/ -}}
  SELECT * FROM %[1]s WHERE columnName > queryParameter;
{{- end}}`

func quoteTableName(schema, table string) string {
	return fmt.Sprintf(`"%s"."%s"`, schema, table)
}

func generatePostgresResource(resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	var queryTemplate string
	var cursorColumns []string
	if strings.EqualFold(tableType, "BASE TABLE") {
		queryTemplate = fmt.Sprintf(tableQueryTemplateTemplate, quoteTableName(schemaName, tableName))
		cursorColumns = []string{"xmin"}
	} else {
		return nil, fmt.Errorf("discovery will not autogenerate resource configs for entities of type %q, but you may add them manually", tableType)
	}

	return &Resource{
		Name:     resourceName,
		Template: queryTemplate,
		Cursor:   cursorColumns,
	}, nil
}

func main() {
	var endpointSchema, err = schemagen.GenerateSchema("Batch SQL", &Config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
		return
	}

	var drv = &BatchSQLDriver{
		DocumentationURL: "https://go.estuary.dev/source-postgres-batch",
		ConfigSchema:     json.RawMessage(endpointSchema),
		Connect:          connectPostgres,
		GenerateResource: generatePostgresResource,
		TranslateValue: func(val any, databaseTypeName string) (any, error) {
			return val, nil
		},
	}
	boilerplate.RunMain(drv)
}
