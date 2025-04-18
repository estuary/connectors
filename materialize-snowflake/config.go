package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/estuary/connectors/go/dbt"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/invopop/jsonschema"
	sf "github.com/snowflakedb/gosnowflake"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

var featureFlagDefaults = map[string]bool{
	// Use Snowpipe streaming for delta-updates bindings that use JWT
	// authentication.
	"snowpipe_streaming": false,
}

type config struct {
	Host          string                     `json:"host" jsonschema:"title=Host (Account URL),description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0,pattern=^[^/:]+.snowflakecomputing.com$"`
	Database      string                     `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=3"`
	Schema        string                     `json:"schema" jsonschema:"title=Schema,description=Database schema for bound collection tables (unless overridden within the binding resource configuration)." jsonschema_extras:"order=4"`
	Warehouse     string                     `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries. Uses the default warehouse for the Snowflake user if left blank." jsonschema_extras:"order=5"`
	Role          string                     `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=6"`
	Account       string                     `json:"account,omitempty" jsonschema:"title=Account,description=Optional Snowflake account identifier." jsonschema_extras:"order=7"`
	HardDelete    bool                       `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=8"`
	Credentials   credentialConfig           `json:"credentials" jsonschema:"title=Authentication"`
	Schedule      boilerplate.ScheduleConfig `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger dbt.JobConfig              `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

// toURI manually builds the DSN connection string.
func (c *config) toURI(tenant string) (string, error) {
	var uri = url.URL{
		Host: c.Host + ":443",
	}

	queryParams := make(url.Values)

	// Required params
	queryParams.Add("application", fmt.Sprintf("%s_EstuaryFlow", tenant))
	queryParams.Add("database", c.Database)
	queryParams.Add("schema", c.Schema)
	// GO_QUERY_RESULT_FORMAT is json in order to enable stream downloading of load results.
	queryParams.Add("GO_QUERY_RESULT_FORMAT", "json")
	// By default Snowflake expects the number of statements to be provided
	// with every request. By setting this parameter to zero we are allowing a
	// variable number of statements to be executed in a single request.
	queryParams.Add("MULTI_STATEMENT_COUNT", "0")
	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	queryParams.Add("client_session_keep_alive", "true")

	// Optional params
	if c.Warehouse != "" {
		queryParams.Add("warehouse", c.Warehouse)
	}

	if c.Role != "" {
		queryParams.Add("role", c.Role)
	}

	if c.Account != "" {
		queryParams.Add("account", c.Account)
	}

	// Authentication
	var user string
	if c.Credentials.AuthType == UserPass {
		user = url.QueryEscape(c.Credentials.User) + ":" + url.QueryEscape(c.Credentials.Password)
	} else if c.Credentials.AuthType == JWT {
		// We run this as part of validate to ensure that there is no error, so
		// this is not expected to error here.
		if key, err := c.Credentials.privateKey(); err != nil {
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

func (c *credentialConfig) privateKey() (*rsa.PrivateKey, error) {
	if c.AuthType == JWT {
		// When providing the PEM file in a JSON file, newlines can't be specified unless
		// escaped, so here we allow an escape hatch to parse these PEM files
		var pkString = strings.ReplaceAll(c.PrivateKey, "\\n", "\n")
		var block, _ = pem.Decode([]byte(pkString))
		if block == nil {
			return nil, fmt.Errorf("invalid private key: must be PEM format")
		} else if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			return nil, fmt.Errorf("parsing private key: %w", err)
		} else {
			return key.(*rsa.PrivateKey), nil
		}
	}

	return nil, fmt.Errorf("only supported with JWT authentication")
}

var hostRe = regexp.MustCompile(`(?i)^.+.snowflakecomputing\.com$`)

func validHost(h string) error {
	hasProtocol := strings.Contains(h, "://")
	missingDomain := !hostRe.MatchString(h)

	if hasProtocol && missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com and not include a protocol)", h)
	} else if hasProtocol {
		return fmt.Errorf("invalid host %q (must not include a protocol)", h)
	} else if missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", h)
	}

	return nil
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"host", c.Host},
		{"database", c.Database},
		{"schema", c.Schema},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	if err := c.Credentials.Validate(); err != nil {
		return err
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return validHost(c.Host)
}

const (
	UserPass = "user_password" // username and password
	JWT      = "jwt"           // JWT, needs a private key
)

type credentialConfig struct {
	AuthType   string `json:"auth_type"`
	User       string `json:"user"`
	Password   string `json:"password"`
	PrivateKey string `json:"private_key"`
}

func (c *credentialConfig) Validate() error {
	switch c.AuthType {
	case UserPass:
		return c.validateUserPassCreds()
	case JWT:
		return c.validateJWTCreds()
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (c *credentialConfig) validateUserPassCreds() error {
	if c.User == "" {
		return fmt.Errorf("missing user")
	}
	if c.Password == "" {
		return fmt.Errorf("missing password")
	}

	return nil
}

func (c *credentialConfig) validateJWTCreds() error {
	if c.User == "" {
		return fmt.Errorf("missing user")
	}
	if c.PrivateKey == "" {
		return fmt.Errorf("missing private_key")
	}

	if _, err := c.privateKey(); err != nil {
		return err
	}

	return nil
}

// JSONSchema allows for the schema to be (semi-)manually specified when used with the
// github.com/invopop/jsonschema package in go-schema-gen, to fullfill the required schema shape for
// our oauth
func (credentialConfig) JSONSchema() *jsonschema.Schema {
	uProps := orderedmap.New[string, *jsonschema.Schema]()
	uProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: UserPass,
		Const:   UserPass,
	})
	uProps.Set("user", &jsonschema.Schema{
		Title:       "User",
		Description: "The Snowflake user login name",
		Type:        "string",
		Extras: map[string]interface{}{
			"order": 1,
		},
	})
	uProps.Set("password", &jsonschema.Schema{
		Title:       "Password",
		Description: "The password for the provided user",
		Type:        "string",
		Extras: map[string]interface{}{
			"secret": true,
			"order":  2,
		},
	})

	jwtProps := orderedmap.New[string, *jsonschema.Schema]()
	jwtProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: JWT,
		Const:   JWT,
	})
	jwtProps.Set("user", &jsonschema.Schema{
		Title:       "User",
		Description: "The Snowflake user login name",
		Type:        "string",
		Extras: map[string]interface{}{
			"order": 1,
		},
	})
	jwtProps.Set("private_key", &jsonschema.Schema{
		Title:       "Private Key",
		Description: "Private Key to be used to sign the JWT token",
		Type:        "string",
		Extras: map[string]interface{}{
			"secret":    true,
			"multiline": true,
			"order":     2,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "Snowflake Credentials",
		Default:     map[string]string{"auth_type": UserPass},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "User Password",
				Required:   []string{"auth_type", "user", "password"},
				Properties: uProps,
			},
			{
				Title:      "Private Key (JWT)",
				Required:   []string{"auth_type", "private_key"},
				Properties: jwtProps,
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "auth_type"},
		},
		Type: "object",
	}
}
