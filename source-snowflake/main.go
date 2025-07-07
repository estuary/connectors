package main

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
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
	Host        string                           `json:"host" jsonschema:"title=Host URL,description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0,pattern=^[^/:]+.snowflakecomputing.com$"`
	Database    string                           `json:"database" jsonschema:"title=Database,description=The database name to capture from." jsonschema_extras:"order=1"`
	Warehouse   string                           `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries. Uses the default warehouse for the Snowflake user if left blank." jsonschema_extras:"order=2"`
	Role        string                           `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=3"`
	Credentials *snowflake_auth.CredentialConfig `json:"credentials" jsonschema:"title=Authentication"`
	Advanced    advancedConfig                   `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	FlowSchema        string `json:"flowSchema,omitempty" jsonschema:"default=ESTUARY_STAGING,description=The schema in which Flow will create and manage its streams and staging tables."`
	FlowDB            string `json:"flowDatabase,omitempty" jsonschema:"default=,description=The database in which Flow will create and manage its streams and staging tables. Defaults to the capture database if unset."`
	FullCopySnapshots bool   `json:"fullCopySnapshots,omitempty" jsonschema:"default=false,description=If set the initial snapshot of a table will be a full copy rather than a zero-copy clone."`
}

var hostRe = regexp.MustCompile(`(?i)^.+.snowflakecomputing\.com$`)

// Validate checks that the configuration possesses all required properties.
func (c *config) Validate() error {
	// Required properties must be present
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if err := c.Credentials.Validate(); err != nil {
		return err
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
		c.Advanced.FlowSchema = "ESTUARY_STAGING"
	}
	if c.Advanced.FlowDB == "" {
		c.Advanced.FlowDB = c.Database // Default to the capture database.
	}
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() (string, error) {
	var uri = url.URL{
		Host: c.Host + ":443",
	}

	var trueString = "true"
	var jsonString = "json"

	queryParams := make(url.Values)

	// Required params
	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	queryParams.Add("client_session_keep_alive", trueString)
	queryParams.Add("database", c.Database)
	// GO_QUERY_RESULT_FORMAT returns query results as individual JSON documents
	// representing rows rather than as *batches* of Arrow records.
	queryParams.Add("GO_QUERY_RESULT_FORMAT", jsonString)

	// Optional params
	if c.Warehouse != "" {
		queryParams.Add("warehouse", c.Warehouse)
	}

	if c.Role != "" {
		queryParams.Add("role", c.Role)
	}

	// Authentication
	var user string
	if c.Credentials.AuthType == snowflake_auth.UserPass {
		user = url.QueryEscape(c.Credentials.User) + ":" + url.QueryEscape(c.Credentials.Password)
	} else if c.Credentials.AuthType == snowflake_auth.JWT {
		// We run this as part of validate to ensure that there is no error, so
		// this is not expected to error here.
		if key, err := c.Credentials.ParsePrivateKey(); err != nil {
			return "", err
		} else if privateKey, err := x509.MarshalPKCS8PrivateKey(key); err != nil {
			return "", fmt.Errorf("parsing private key: %w", err)
		} else {
			privateKeyString := base64.URLEncoding.EncodeToString(privateKey)
			queryParams.Add("privateKey", privateKeyString)
			queryParams.Add("authenticator", strings.ToLower(sf.AuthTypeJwt.String()))
		}
		user = url.QueryEscape(c.Credentials.User)
	} else {
		return "", fmt.Errorf("unknown auth type: %s", c.Credentials.AuthType)
	}

	dsn := user + "@" + uri.Hostname() + ":" + uri.Port() + "?" + queryParams.Encode()
	return dsn, nil
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
