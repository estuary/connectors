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

	_ "github.com/go-mysql-org/go-mysql/driver"
)

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	// TODO(wgd): Add network tunnel support
}

type advancedConfig struct {
	DBName string `json:"dbname,omitempty" jsonschema:"title=Database Name,default=mysql,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
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
	// value, and if the port is unspecified it should be the MySQL
	// default 3306.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":3306"
	}
}

// ToDSN converts the Config to a DSN string.
func (c *Config) ToDSN() string {
	// go-mysql dsn format: "user:password@addr?dbname"
	// For simplicity we use the URL struct and strip the leading 'mysql://'
	// Ironically, the client library does the exact inverse to parse it.
	var address = c.Address
	var uri = url.URL{
		Scheme: "mysql",
		User:   url.UserPassword(c.User, c.Password),
		Host:   address,
	}
	if c.Advanced.DBName != "" {
		uri.Path = "/" + c.Advanced.DBName
	}
	return strings.TrimPrefix(uri.String(), "mysql://")
}

func connectMySQL(ctx context.Context, configJSON json.RawMessage) (*sql.DB, error) {
	var cfg Config
	if err := pf.UnmarshalStrict(configJSON, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	cfg.SetDefaults()

	log.WithFields(log.Fields{
		"address": cfg.Address,
		"user":    cfg.User,
	}).Info("connecting to database")

	var db, err = sql.Open("mysql", cfg.ToDSN())
	if err != nil {
		return nil, fmt.Errorf("error opening database connection: %w", err)
	} else if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}
	return db, nil
}

const tableQueryTemplateTemplate = `{{/***********************************************************
   * This is a generic query template which is provided so that *
   * discovered bindings can have a vaguely reasonable default  *
   * behavior.                                                  *
   *                                                            *
   * You are entirely free to delete this template and replace  *
   * it with whatever query you want to execute. Just be aware  *
   * that this query will be executed over and over every poll  *
   * interval, and if you intend to lower the polling interval  *
   * from its default of 24h you should probably try and use a  *
   * cursor to capture only new rows each time.                 *
   *                                                            *
   * By default this template generates a 'SELECT * FROM table' *
   * query which will read the whole table on each poll.        *
   *                                                            *
   * If the table has a suitable "cursor" column (or columns)   *
   * which can be used to identify only changed rows, add the   *
   * name(s) to the "Cursor" property of this binding. If you   *
   * do that, the generated query will have the form:           *
   *                                                            *
   *     SELECT * FROM table                                    *
   *       WHERE (ka > :0) OR (ka = :0 AND kb > :1)             *
   *       ORDER BY ka, kb;                                     *
   *                                                            *
   * This can be used to incrementally capture new rows if the  *
   * table has a serial ID column or a 'created_at' timestamp,  *
   * or it could be used to capture updated rows if the table   *
   * has an 'updated_at' timestamp.                             *
   ***********************************************************/ -}}
{{if .CursorFields -}}
  {{- if .IsFirstQuery -}}
    SELECT * FROM %[1]s;
  {{- else -}}
    SELECT * FROM %[1]s
	{{- range $i, $k := $.CursorFields -}}
	  {{- if eq $i 0}} WHERE ({{else}}) OR ({{end -}}
      {{- range $j, $n := $.CursorFields -}}
		{{- if lt $j $i -}}
		  {{$n}} = :{{$j}} AND {{end -}}
	  {{- end -}}
	  {{$k}} > :{{$i}}
	{{- end -}}
	) ORDER BY {{range $i, $k := $.CursorFields}}{{if gt $i 0}}, {{end}}{{$k}}{{end -}}
	;
  {{- end -}}
{{- else -}}
  SELECT * FROM %[1]s;
{{- end}}`

func quoteTableName(schema, table string) string {
	return fmt.Sprintf("`%s`.`%s`", schema, table)
}

func generateMySQLResource(resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	var queryTemplate string
	if strings.EqualFold(tableType, "BASE TABLE") {
		queryTemplate = fmt.Sprintf(tableQueryTemplateTemplate, quoteTableName(schemaName, tableName))
	} else {
		return nil, fmt.Errorf("discovery will not autogenerate resource configs for entities of type %q, but you may add them manually", tableType)
	}

	return &Resource{
		Name:         resourceName,
		Template:     queryTemplate,
		PollInterval: "24h",
	}, nil
}

func translateMySQLValue(val any, databaseTypeName string) (any, error) {
	if val, ok := val.([]byte); ok {
		return string(val), nil
	}
	return val, nil
}

var mysqlDriver = &BatchSQLDriver{
	DocumentationURL: "https://go.estuary.dev/source-mysql-batch",
	ConfigSchema:     generateConfigSchema(),
	Connect:          connectMySQL,
	GenerateResource: generateMySQLResource,
	TranslateValue:   translateMySQLValue,
	ExcludedSystemSchemas: []string{
		"information_schema",
		"mysql",
		"performance_schema",
		"sys",
	},
}

func generateConfigSchema() json.RawMessage {
	var configSchema, err = schemagen.GenerateSchema("Batch SQL", &Config{}).MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("generating endpoint schema: %w", err))
	}
	return json.RawMessage(configSchema)
}

func main() {
	boilerplate.RunMain(mysqlDriver)
}
