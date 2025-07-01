package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"

	"github.com/invopop/jsonschema"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

const (
	UserPass = "user_password" // username and password
	JWT      = "jwt"           // JWT, needs a private key
)

type CredentialConfig struct {
	AuthType   string `json:"auth_type"`
	User       string `json:"user"`
	Password   string `json:"password"`
	PrivateKey string `json:"private_key"`
}

func (c *CredentialConfig) Validate() error {
	switch c.AuthType {
	case UserPass:
		return c.validateUserPassCreds()
	case JWT:
		return c.validateJWTCreds()
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
