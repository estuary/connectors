package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	sf "github.com/snowflakedb/gosnowflake"
)

func main() {
	boilerplate.RunMain(new(snowflakeDriver))
}

type snowflakeDriver struct{}

// config represents the endpoint configuration for Snowflake.
type config struct {
	Host      string         `json:"host" jsonschema:"title=Host URL,description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0"`
	Account   string         `json:"account" jsonschema:"title=Account,description=The Snowflake account identifier." jsonschema_extras:"order=1"`
	User      string         `json:"user" jsonschema:"title=User,description=The Snowflake user login name." jsonschema_extras:"order=2"`
	Password  string         `json:"password" jsonschema:"title=Password,description=The password for the provided user." jsonschema_extras:"secret=true,order=3"`
	Database  string         `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=4"`
	Warehouse string         `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries. Uses the default warehouse for the Snowflake user if left blank." jsonschema_extras:"order=5"`
	Advanced  advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	FlowSchema string `json:"flowSchema,omitempty" jsonschema:"default=FLOW,description=The schema in which Flow will create and manage its streams and staging tables."`
}

var hostRe = regexp.MustCompile(`(?i)^.+.snowflakecomputing\.com$`)

// Validate checks that the configuration possesses all required properties.
func (c *config) Validate() error {
	// Required properties must be present
	var requiredProperties = [][]string{
		{"account", c.Account},
		{"host", c.Host},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	// Host must look correct
	hasProtocol := strings.Contains(c.Host, "://")
	missingDomain := !hostRe.MatchString(c.Host)
	if hasProtocol && missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com and not include a protocol)", c.Host)
	} else if hasProtocol {
		return fmt.Errorf("invalid host %q (must not include a protocol)", c.Host)
	} else if missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", c.Host)
	}

	return nil
}

// SetDefaults fills in default values for optional parameters.
func (c *config) SetDefaults() {
	// Note these are 1:1 with 'omitempty' in Config field tags,
	// which cause these fields to be emitted as non-required.
	if c.Advanced.FlowSchema == "" {
		c.Advanced.FlowSchema = "FLOW"
	}
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	// Build a DSN connection string.
	var cfg = &sf.Config{
		Account:   c.Account,
		Host:      c.Host,
		User:      c.User,
		Password:  c.Password,
		Database:  c.Database,
		Warehouse: c.Warehouse,
		Params:    make(map[string]*string),
	}

	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	var trueString = "true"
	cfg.Params["client_session_keep_alive"] = &trueString

	dsn, err := sf.DSN(cfg)
	if err != nil {
		panic(fmt.Errorf("internal error building snowflake dsn: %w", err))
	}
	return dsn
}

type resource struct {
	Schema string `json:"schema" jsonschema:"title=Schema,description=The schema in which the table resides."`
	Table  string `json:"table" jsonschema:"title=Table Name,description=The name of the table to be captured."`
}

func (r resource) Validate() error {
	if r.Schema == "" {
		return fmt.Errorf("table schema must be specified")
	}
	if r.Table == "" {
		return fmt.Errorf("table name must be specified")
	}
	return nil
}

func (r *resource) SetDefaults() {}

func (snowflakeDriver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Snowflake Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Snowflake Resource", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-snowflake",
		ResourcePathPointers:     []string{"/schema", "/table"},
	}, nil
}

func (snowflakeDriver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{}, nil
}
