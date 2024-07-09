package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"text/template"

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
	Database string         `json:"database" jsonschema:"default=postgres,description=Logical database name to capture from." jsonschema_extras:"order=3"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
	// TODO(wgd): Add network tunnel support
}

type advancedConfig struct {
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '5m' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
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
	// value, and if the port is unspecified it should be the PostgreSQL
	// default 5432.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":5432"
	}

	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "5m"
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

func connectPostgres(ctx context.Context, cfg *Config) (*sql.DB, error) {
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

// A discussion on the use of XIDs as query cursors:
//
// XID values are 32-bit unsigned integers with implicit wraparound on overflow and underflow.
// The lowest 'normal' XID value is 3. The values 0-2 are reserved as sentinels, with 2 being
// the "Frozen XID" value.
//
// While Postgres has a comparison function `TransactionIdPrecedes()` internally, this is not
// exposed in the form of a comparison predicate or ordering method. So we have to cast XIDs
// to integers (by way of text) and then reimplement the desired ordering behavior ourselves.
//
// Given a particular "cursor" value obtained from a previous polling query execution, we can
// begin by assuming that there are fewer than 2^31 "live" XIDs greater than that cursor. The
// issue is that with XID wraparound some of these values may be numerically smaller. Given a
// wrapping uint32 wrapping subtraction operator the expression `xmin - cursor` would compute
// an orderable count of "how far past the previous cursor" a given row is.
//
// We can emulate wrapping uint32 subtraction using PostgreSQL int64 values by simply writing
// the expression `((x::bigint - y::bigint)<<32)>>32`.
//
// The xmin polling query below assumes that the source table is updated more frequently than
// the XID epoch wraps around. If this assumption is violated it would in principle be doable
// to `SELECT txid_current() as polled_txid, ...` and use "polled_txid" as the cursor value.
const tableQueryTemplate = `{{if .IsFirstQuery -}}
  SELECT xmin AS txid, * FROM {{quoteTableName .SchemaName .TableName}} ORDER BY xmin::text::bigint;
{{- else -}}
  SELECT xmin AS txid, * FROM {{quoteTableName .SchemaName .TableName}}
    WHERE (((xmin::text::bigint - $1::bigint)<<32)>>32) > 0 AND xmin::text::bigint >= 3
    ORDER BY (((xmin::text::bigint - $1::bigint)<<32)>>32);
{{- end}}`

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

var templateFuncs = template.FuncMap{
	"quoteTableName":  quoteTableName,
	"quoteIdentifier": quoteIdentifier,
}

func generatePostgresResource(resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	if !strings.EqualFold(tableType, "BASE TABLE") {
		return nil, fmt.Errorf("discovery will not autogenerate resource configs for entities of type %q, but you may add them manually", tableType)
	}

	return &Resource{
		Name:       resourceName,
		SchemaName: schemaName,
		TableName:  tableName,
		Cursor:     []string{"txid"},
	}, nil
}

func translatePostgresValue(val any, databaseTypeName string) (any, error) {
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

var postgresDriver = &BatchSQLDriver{
	DocumentationURL:     "https://go.estuary.dev/source-postgres-batch",
	ConfigSchema:         generateConfigSchema(),
	Connect:              connectPostgres,
	GenerateResource:     generatePostgresResource,
	TranslateValue:       translatePostgresValue,
	DefaultQueryTemplate: tableQueryTemplate,
}

func generateConfigSchema() json.RawMessage {
	var configSchema, err = schemagen.GenerateSchema("Batch SQL", &Config{}).MarshalJSON()
	if err != nil {
		panic(fmt.Errorf("generating endpoint schema: %w", err))
	}
	return json.RawMessage(configSchema)
}

func main() {
	boilerplate.RunMain(postgresDriver)
}
