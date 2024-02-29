package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	perrors "github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
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
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	DiscoverSchemas []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	DBName          string   `json:"dbname,omitempty" jsonschema:"title=Database Name,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
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
	// value, and if the port is unspecified it should be the MySQL
	// default 3306.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":3306"
	}

	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "24h"
	}
}

func translateMySQLValue(val any) (any, error) {
	if val, ok := val.([]byte); ok {
		return string(val), nil
	}
	return val, nil
}

func connectMySQL(ctx context.Context, cfg *Config) (*client.Conn, error) {
	log.WithFields(log.Fields{
		"address": cfg.Address,
		"user":    cfg.User,
	}).Info("connecting to database")

	var conn *client.Conn
	var err error
	var withTLS = func(c *client.Conn) { c.SetTLSConfig(&tls.Config{InsecureSkipVerify: true}) }
	if conn, err = client.Connect(cfg.Address, cfg.User, cfg.Password, cfg.Advanced.DBName, withTLS); err == nil {
		log.WithField("addr", cfg.Address).Debug("connected with TLS")
	} else if conn, err = client.Connect(cfg.Address, cfg.User, cfg.Password, cfg.Advanced.DBName); err == nil {
		log.WithField("addr", cfg.Address).Warn("connected without TLS")
	} else if err, ok := perrors.Cause(err).(*mysql.MyError); ok && err.Code == mysql.ER_ACCESS_DENIED_ERROR {
		return nil, cerrors.NewUserError(err, "incorrect username or password")
	} else {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	if _, err := conn.Execute("SELECT true;"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return conn, nil
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
    SELECT * FROM %[1]s
  {{- else -}}
    SELECT * FROM %[1]s
	{{- range $i, $k := $.CursorFields -}}
	  {{- if eq $i 0}} WHERE ({{else}}) OR ({{end -}}
      {{- range $j, $n := $.CursorFields -}}
		{{- if lt $j $i -}}
		  {{$n}} = @flow_cursor_value[{{$j}}] AND {{end -}}
	  {{- end -}}
	  {{$k}} > @flow_cursor_value[{{$i}}]
	{{- end -}}
	) 
  {{- end}} ORDER BY {{range $i, $k := $.CursorFields}}{{if gt $i 0}}, {{end}}{{$k}}{{end -}};
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
		Name:     resourceName,
		Template: queryTemplate,
	}, nil
}

var mysqlDriver = &BatchSQLDriver{
	DocumentationURL: "https://go.estuary.dev/source-mysql-batch",
	ConfigSchema:     generateConfigSchema(),
	Connect:          connectMySQL,
	GenerateResource: generateMySQLResource,
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
