package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/invopop/jsonschema"
	pf "github.com/estuary/flow/go/protocols/flow"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

const (
	UserPass = "user_password" // username and password
	JWT      = "jwt"           // JWT, needs a private key
	OAuth2   = "oauth2"        // OAuth2, needs client credentials and refresh token
)

type CredentialConfig struct {
	AuthType     string `json:"auth_type"`
	User         string `json:"user"`
	Password     string `json:"password"`
	PrivateKey   string `json:"private_key"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
}

func (c *CredentialConfig) Validate() error {
	switch c.AuthType {
	case UserPass:
		return c.validateUserPassCreds()
	case JWT:
		return c.validateJWTCreds()
	case OAuth2:
		return c.validateOAuth2Creds()
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (c *CredentialConfig) validateUserPassCreds() error {
	if c.User == "" {
		return fmt.Errorf("missing user")
	}
	if c.Password == "" {
		return fmt.Errorf("missing password")
	}

	return nil
}

func (c *CredentialConfig) validateJWTCreds() error {
	if c.User == "" {
		return fmt.Errorf("missing user")
	}
	if c.PrivateKey == "" {
		return fmt.Errorf("missing private_key")
	}

	if _, err := c.ParsePrivateKey(); err != nil {
		return err
	}

	return nil
}

func (c *CredentialConfig) validateOAuth2Creds() error {
	if c.RefreshToken == "" {
		return fmt.Errorf("missing refresh_token for oauth2")
	}
	if c.ClientID == "" {
		return fmt.Errorf("missing client_id for oauth2")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("missing client_secret for oauth2")
	}

	return nil
}

func (c *CredentialConfig) ParsePrivateKey() (*rsa.PrivateKey, error) {
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

// JSONSchema allows for the schema to be (semi-)manually specified when used with the
// github.com/invopop/jsonschema package in go-schema-gen, to fullfill the required schema shape for
// our oauth
func (CredentialConfig) JSONSchema() *jsonschema.Schema {
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

	oauth2Props := orderedmap.New[string, *jsonschema.Schema]()
	oauth2Props.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: OAuth2,
		Const:   OAuth2,
	})
	oauth2Props.Set("client_id", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})
	oauth2Props.Set("client_secret", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})
	oauth2Props.Set("refresh_token", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "Snowflake Credentials",
		Default:     map[string]string{"auth_type": JWT},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "Private Key (JWT)",
				Required:   []string{"auth_type", "private_key"},
				Properties: jwtProps,
			},
			{
				Title:      "OAuth2",
				Required:   []string{"auth_type", "client_id", "client_secret", "refresh_token"},
				Properties: oauth2Props,
				Extras: map[string]interface{}{
					"x-oauth2-provider": "snowflake",
				},
			},
			{
				Title:      "User Password",
				Required:   []string{"auth_type", "user", "password"},
				Properties: uProps,
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "auth_type"},
		},
		Type: "object",
	}
}

// oauthSpec returns the OAuth2 specification for Snowflake.
// The OAuth endpoints use the account-specific host, which is provided via template variables.
// The scope parameter determines what access the token will have - typically "refresh_token" is needed
// for offline access, and optionally "session:role:<rolename>" to limit access to a specific role.
func OAuthSpec() *pf.OAuth2 {
	// Snowflake OAuth endpoints are account-specific and constructed from the host configuration.
	// Template variables like {{{ config.host }}} will be populated by the control plane
	// with the actual host value from the endpoint configuration.
	//
	// For Snowflake, we use a default scope of "refresh_token" to enable offline access.
	// Additional scopes like "session:role:ROLE_NAME" can be added if needed in the future.
	scope := "refresh_token"

	return &pf.OAuth2{
		Provider: "snowflake",
		// Authorization endpoint: https://<account_url>/oauth/authorize
		// Include scope parameter for refresh_token
		AuthUrlTemplate: fmt.Sprintf("https://{{{ config.host }}}/oauth/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&scope={{#urlencode}}%s{{/urlencode}}&state={{#urlencode}}{{{ state }}}{{/urlencode}}", scope),
		// Token endpoint: https://<account_url>/oauth/token-request
		AccessTokenUrlTemplate: "https://{{{ config.host }}}/oauth/token-request",
		// Token request body
		AccessTokenBody: "grant_type=authorization_code&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}",
		// Token request headers
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{
			"content-type": json.RawMessage(`"application/x-www-form-urlencoded"`),
		},
		// Response mapping - Snowflake returns refresh_token which we need to store
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"refresh_token": json.RawMessage(`"/refresh_token"`),
		},
	}
}
