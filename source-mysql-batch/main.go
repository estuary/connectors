package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/estuary/connectors/go/capture/mysql/spatial"
	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	log "github.com/sirupsen/logrus"
)

var featureFlagDefaults = map[string]bool{
	// When true, the fallback collection key for keyless source tables will be
	// ["/_meta/row_id"] instead of ["/_meta/polled", "/_meta/index"].
	"keyless_row_id": true,

	// When set, discovered collection schemas will be emitted as SourcedSchema messages
	// so that Flow can have access to 'official' schema information from the source DB.
	"emit_sourced_schemas": true,
}

// Config tells the connector how to connect to and interact with the source database.
type Config struct {
	Address  string         `json:"address" jsonschema:"title=Server Address,description=The host or host:port at which the database can be reached." jsonschema_extras:"order=0"`
	User     string         `json:"user" jsonschema:"default=flow_capture,description=The database user to authenticate as." jsonschema_extras:"order=1"`
	Password string         `json:"password" jsonschema:"description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`

	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	DiscoverViews   bool     `json:"discover_views,omitempty" jsonschema:"title=Discover Views,description=When set views will be automatically discovered as resources. If unset only tables will be discovered."`
	PollSchedule    string   `json:"poll,omitempty" jsonschema:"title=Default Polling Schedule,description=When and how often to execute fetch queries. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$"`
	DiscoverSchemas []string `json:"discover_schemas,omitempty" jsonschema:"title=Discovery Schema Selection,description=If this is specified only tables in the selected schema(s) will be automatically discovered. Omit all entries to discover tables from all schemas."`
	DBName          string   `json:"dbname,omitempty" jsonschema:"title=Database Name,description=The name of database to connect to. In general this shouldn't matter. The connector can discover and capture from all databases it's authorized to access."`
	SourceTag       string   `json:"source_tag,omitempty" jsonschema:"title=Source Tag,description=When set the capture will add this value as the property 'tag' in the source metadata of each document."`
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
	// value, and if the port is unspecified it should be the MySQL
	// default 3306.
	if !strings.Contains(c.Address, ":") {
		c.Address += ":3306"
	}

	if c.Advanced.PollSchedule == "" {
		c.Advanced.PollSchedule = "24h"
	}
}

func translateMySQLValue(val any, fieldType byte, fieldFlag uint16) (any, error) {
	if bs, ok := val.([]byte); ok {
		// For binary column types (blobs, geometry, and binary/varbinary), keep as
		// []byte so that JSON encoding will base64-encode the data. For text column
		// types, convert to string.
		//
		// BINARY/VARBINARY columns use MYSQL_TYPE_STRING/MYSQL_TYPE_VAR_STRING with
		// the BINARY_FLAG set to distinguish them from CHAR/VARCHAR.
		//
		// TEXT types (TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT) are stored internally as
		// BLOB types but without the BINARY_FLAG. BLOB types have the BINARY_FLAG set.
		switch fieldType {
		case mysql.MYSQL_TYPE_GEOMETRY:
			// Parse MySQL's internal geometry format and return as WKT string
			wkt, err := spatial.ParseToWKT(bs)
			if err != nil {
				return nil, fmt.Errorf("parsing spatial value: %w", err)
			}
			return wkt, nil
		case mysql.MYSQL_TYPE_TINY_BLOB, mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB:
			// TEXT vs BLOB: TEXT types don't have BINARY_FLAG, BLOB types do
			if fieldFlag&mysql.BINARY_FLAG != 0 {
				return bs, nil
			}
			return string(bs), nil
		case mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VAR_STRING:
			// CHAR/VARCHAR vs BINARY/VARBINARY: binary types have BINARY_FLAG
			if fieldFlag&mysql.BINARY_FLAG != 0 {
				return bs, nil
			}
			return string(bs), nil
		default:
			return string(bs), nil
		}
	}
	return val, nil
}

func connectMySQL(ctx context.Context, cfg *Config) (*client.Conn, error) {
	log.WithFields(log.Fields{
		"address": cfg.Address,
		"user":    cfg.User,
	}).Info("connecting to database")

	// If a network tunnel is configured, then try to start it before establishing connections.
	var address = cfg.Address
	if cfg.NetworkTunnel.InUse() {
		if _, err := cfg.NetworkTunnel.Start(ctx, cfg.Address, "3306"); err != nil {
			return nil, err
		}
		address = "localhost:3306"
	}

	// We want to protect against hangs during the initial connection process, but
	// use a more lenient timeout for normal operation. This timer enforces a strict
	// deadline on connection establishment only.
	var connectionTimer = time.AfterFunc(60*time.Second, func() {
		log.WithField("addr", address).Fatal("failed to connect before deadline")
	})
	defer connectionTimer.Stop()

	var conn *client.Conn

	const mysqlErrorCodeSecureTransportRequired = 3159 // From https://dev.mysql.com/doc/mysql-errors/8.4/en/server-error-reference.html
	var mysqlErr *mysql.MyError
	var withTLS = func(c *client.Conn) error {
		c.SetTLSConfig(&tls.Config{InsecureSkipVerify: true})
		return nil
	}
	var withTimeouts = func(c *client.Conn) error {
		// Some polling queries can take a long time to start yielding results,
		// especially for large tables with unindexed cursor columns. While we
		// recommend that users add indexes as necessary, it's better to be
		// forgiving of these delays on our end.
		c.ReadTimeout = 30 * time.Minute
		c.WriteTimeout = 60 * time.Second
		return nil
	}
	// The following if-else chain looks somewhat complicated but it's really very simple.
	// * We'd prefer to use TLS, so we first try to connect with TLS, and then if that fails
	//   we try again without.
	// * If either error is an incorrect username/password then we just report that.
	// * Otherwise we report both errors because it's better to be clear what failed and how.
	// * Except if the non-TLS connection specifically failed because TLS is required then
	//   we don't need to mention that and just return the with-TLS error.
	if connWithTLS, errWithTLS := client.Connect(address, cfg.User, cfg.Password, cfg.Advanced.DBName, withTimeouts, withTLS); errWithTLS == nil {
		log.WithField("addr", cfg.Address).Info("connected with TLS")
		conn = connWithTLS
	} else if errors.As(errWithTLS, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		return nil, cerrors.NewUserError(mysqlErr, "incorrect username or password")
	} else if connWithoutTLS, errWithoutTLS := client.Connect(address, cfg.User, cfg.Password, cfg.Advanced.DBName, withTimeouts); errWithoutTLS == nil {
		log.WithField("addr", cfg.Address).Info("connected without TLS")
		conn = connWithoutTLS
	} else if errors.As(errWithoutTLS, &mysqlErr) && mysqlErr.Code == mysql.ER_ACCESS_DENIED_ERROR {
		log.WithFields(log.Fields{"withTLS": errWithTLS, "nonTLS": errWithoutTLS}).Error("unable to connect to database")
		return nil, cerrors.NewUserError(mysqlErr, "incorrect username or password")
	} else if errors.As(errWithoutTLS, &mysqlErr) && mysqlErr.Code == mysqlErrorCodeSecureTransportRequired {
		return nil, fmt.Errorf("unable to connect to database: %w", errWithTLS)
	} else {
		return nil, fmt.Errorf("unable to connect to database: failed both with TLS (%w) and without TLS (%w)", errWithTLS, errWithoutTLS)
	}

	if _, err := conn.Execute("SELECT true;"); err != nil {
		return nil, fmt.Errorf("error executing no-op query: %w", err)
	}
	return conn, nil
}

func selectQueryTemplate(res *Resource) (string, error) {
	if res.Template != "" {
		return res.Template, nil
	}
	return tableQueryTemplate, nil
}

const tableQueryTemplate = `{{if .CursorFields -}}
  {{- if .IsFirstQuery -}}
    SELECT * FROM {{quoteTableName .SchemaName .TableName}}
  {{- else -}}
    SELECT * FROM {{quoteTableName .SchemaName .TableName}}
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
  SELECT * FROM {{quoteTableName .SchemaName .TableName}};
{{- end}}`

var templateFuncs = template.FuncMap{
	"quoteTableName":  quoteTableName,
	"quoteIdentifier": quoteIdentifier,
}

func quoteTableName(schema, table string) string {
	return quoteIdentifier(schema) + "." + quoteIdentifier(table)
}

func quoteIdentifier(name string) string {
	// Per https://dev.mysql.com/doc/refman/8.0/en/identifiers.html, the identifier quote character
	// is the backtick (`). If the identifier itself contains a backtick, it must be doubled.
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func generateMySQLResource(cfg *Config, resourceName, schemaName, tableName, tableType string) (*Resource, error) {
	if strings.EqualFold(tableType, "BASE TABLE") || (strings.EqualFold(tableType, "VIEW") && cfg.Advanced.DiscoverViews) {
		return &Resource{
			Name:       resourceName,
			SchemaName: schemaName,
			TableName:  tableName,
		}, nil
	}
	return nil, fmt.Errorf("unsupported entity type %q", tableType)
}

var mysqlDriver = &BatchSQLDriver{
	DocumentationURL:    "https://go.estuary.dev/source-mysql-batch",
	ConfigSchema:        generateConfigSchema(),
	Connect:             connectMySQL,
	GenerateResource:    generateMySQLResource,
	SelectQueryTemplate: selectQueryTemplate,
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
