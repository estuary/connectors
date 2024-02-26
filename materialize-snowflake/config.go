package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/invopop/jsonschema"
	"regexp"
	"strings"

	m "github.com/estuary/connectors/go/protocols/materialize"
	sf "github.com/snowflakedb/gosnowflake"
)

// config represents the endpoint configuration for snowflake.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	// TODO(mahdi): the Host and Account config is very confusing since Snowflake has multiple ways and very specific requirements
	// for specifying the url of an instance. Ideally we should just accept the URL directly and use that, that would save us and
	// users some headache
	// See: https://docs.snowflake.com/en/user-guide/admin-account-identifier#non-vps-account-locator-formats-by-cloud-platform-and-region
	Host      string `json:"host" jsonschema:"title=Host URL,description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0,pattern=^[^/:]+.snowflakecomputing.com$"`
	Account   string `json:"account" jsonschema:"title=Account,description=The Snowflake account identifier." jsonschema_extras:"order=1"`
	User      string `json:"user" jsonschema:"-"`
	Password  string `json:"password" jsonschema:"-"`
	Database  string `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=4"`
	Schema    string `json:"schema" jsonschema:"title=Schema,description=Database schema for bound collection tables (unless overridden within the binding resource configuration) as well as associated materialization metadata tables." jsonschema_extras:"order=5"`
	Warehouse string `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries. Uses the default warehouse for the Snowflake user if left blank." jsonschema_extras:"order=6"`
	Role      string `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=7"`

	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce active warehouse time by increasing the delay between updates. Defaults to 30 minutes if unset.,enum=0s,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI(tenant string) string {
	// Build a DSN connection string.
	var configCopy = c.asSnowflakeConfig(tenant)
	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	// The Params map will not have been initialized if the endpoint config didn't specify
	// it, so we check and initialize here if needed.
	if configCopy.Params == nil {
		configCopy.Params = make(map[string]*string)
	}
	configCopy.Params["client_session_keep_alive"] = &trueString
	dsn, err := sf.DSN(&configCopy)
	if err != nil {
		panic(fmt.Errorf("building snowflake dsn: %w", err))
	}

	return dsn
}

func (c *credentialConfig) privateKey() (*rsa.PrivateKey, error) {
	if c.AuthType == JWT {
		// When providing the PEM file in a JSON file, newlines can't be specified unless
		// escaped, so here we allow an escape hatch to parse these PEM files
		var pkString = strings.ReplaceAll(c.PrivateKey, "\\n", "\n")
		var block, _ = pem.Decode([]byte(pkString))
		if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
			return nil, fmt.Errorf("parsing private key: %w", err)
		} else {
			return key.(*rsa.PrivateKey), nil
		}
	}

	return nil, fmt.Errorf("only supported with JWT authentication")
}

func (c *config) asSnowflakeConfig(tenant string) sf.Config {
	var maxStatementCount string = "0"
	var json string = "json"

	var conf = sf.Config{
		Account:     c.Account,
		Host:        c.Host,
		Database:    c.Database,
		Schema:      c.Schema,
		Warehouse:   c.Warehouse,
		Role:        c.Role,
		Application: fmt.Sprintf("%s_EstuaryFlow", tenant),
		Params: map[string]*string{
			// By default Snowflake expects the number of statements to be provided
			// with every request. By setting this parameter to zero we are allowing a
			// variable number of statements to be executed in a single request
			"MULTI_STATEMENT_COUNT":  &maxStatementCount,
			"GO_QUERY_RESULT_FORMAT": &json,
		},
	}

	if c.Credentials.AuthType == UserPass {
		conf.Authenticator = sf.AuthTypeSnowflake
		conf.User = c.Credentials.User
		conf.Password = c.Credentials.Password
	} else if c.Credentials.AuthType == JWT {
		conf.Authenticator = sf.AuthTypeJwt
		// We run this as part of validate to ensure that there is no error, so
		// this is not expected to error here
		if key, err := c.Credentials.privateKey(); err != nil {
			panic(err)
		} else {
			conf.PrivateKey = key
		}
		conf.User = c.Credentials.User
	} else {
		conf.Authenticator = sf.AuthTypeSnowflake
		conf.User = c.User
		conf.Password = c.Password
	}

	return conf
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
		{"account", c.Account},
		{"host", c.Host},
		{"database", c.Database},
		{"schema", c.Schema},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if _, err := m.ParseDelay(c.Advanced.UpdateDelay); err != nil {
		return err
	}

	if c.Password != "" {
		// If they have both old user and password and new ones, ask them to remove the old ones
		if c.Credentials.AuthType != "" || c.Credentials.Password != "" {
			return fmt.Errorf("User and password in the root config are deprecated, please omit them and use the `credentials` config object only.")
		}

		c.Credentials.AuthType = UserPass
		c.Credentials.Password = c.Password
		c.Credentials.User = c.User
	}

	if err := c.Credentials.Validate(); err != nil {
		return err
	}

	return validHost(c.Host)
}

const (
	UserPass = "user_password" // username and password
	JWT      = "jwt"           // JWT, needs a private key
)

type credentialConfig struct {
	AuthType string `json:"auth_type"`

	User string `json:"user"`

	Password string `json:"password"`

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
	uProps := orderedmap.New()
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

	jwtProps := orderedmap.New()
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
