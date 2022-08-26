package google_auth

import (
	"context"
	"fmt"
	"time"

	"github.com/iancoleman/orderedmap"
	"github.com/invopop/jsonschema"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

const (
	SERVICE_AUTH_TYPE = "Service" // For service accounts
	CLIENT_AUTH_TYPE  = "Client"  // For clients to use oauth 2
)

type CredentialConfig struct {
	AuthType        string `json:"auth_type"`
	ClientID        string `json:"client_id,omitempty"`
	ClientSecret    string `json:"client_secret,omitempty"`
	RefreshToken    string `json:"refresh_token,omitempty"`
	CredentialsJSON string `json:"credentials_json,omitempty"`
}

func (c *CredentialConfig) Validate() error {
	switch c.AuthType {
	case SERVICE_AUTH_TYPE:
		return c.validateServiceCreds()
	case CLIENT_AUTH_TYPE:
		return c.validateClientCreds()
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (c *CredentialConfig) validateServiceCreds() error {
	if c.CredentialsJSON == "" {
		return fmt.Errorf("missing service account credentials JSON")
	}

	return nil
}

func (c *CredentialConfig) validateClientCreds() error {
	if c.RefreshToken == "" {
		return fmt.Errorf("missing refresh token for oauth2")
	} else if c.ClientID == "" {
		return fmt.Errorf("missing client ID for oauth2")
	} else if c.ClientSecret == "" {
		return fmt.Errorf("missing client secret for oauth2")
	}

	return nil
}

func (c *CredentialConfig) GoogleCredentials(ctx context.Context, scopes ...string) (*google.Credentials, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}

	if c.AuthType == SERVICE_AUTH_TYPE {
		return google.CredentialsFromJSON(ctx, []byte(c.CredentialsJSON), scopes...)
	}

	oauthConfig := &oauth2.Config{
		ClientID:     c.ClientID,
		ClientSecret: c.ClientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       scopes,
	}

	token := &oauth2.Token{
		RefreshToken: c.RefreshToken,
		Expiry:       time.Now(), // Require a new access token to be obtained.
	}
	tokenSource := oauthConfig.TokenSource(ctx, token)

	return &google.Credentials{
		TokenSource: tokenSource,
	}, nil
}

// JSONSchema allows for the schema to be (semi-)manually specified. I couldn't figure out a way to
// get a schema in the correct shape to be generated with only struct annotations.
func (CredentialConfig) JSONSchema() *jsonschema.Schema {
	serviceAccountProps := orderedmap.New()
	serviceAccountProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: SERVICE_AUTH_TYPE,
		Const:   SERVICE_AUTH_TYPE,
	})
	serviceAccountProps.Set("service_account_info", &jsonschema.Schema{
		Title:       "Service Account JSON",
		Description: "The JSON key of the service account to use for authorization. See this setup guide for more details https://go.estuary.dev/RP7TxO",
		Type:        "string",
		Examples: []interface{}{
			"{ \"type\": \"service_account\" \"project_id\": YOUR_PROJECT_ID, \"private_key_id\": YOUR_PRIVATE_KEY, ... }",
		},
		Extras: map[string]interface{}{
			"secret":    true,
			"multiline": true,
		},
	})

	oauthProps := orderedmap.New()
	oauthProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: CLIENT_AUTH_TYPE,
		Const:   CLIENT_AUTH_TYPE,
	})
	oauthProps.Set("client_id", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})
	oauthProps.Set("client_secret", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})
	oauthProps.Set("refresh_token", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "Google API Credentials",
		Default:     map[string]string{"auth_type": CLIENT_AUTH_TYPE},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "Google OAuth",
				Required:   []string{"auth_type", "client_id", "client_secret", "refresh_token"},
				Properties: oauthProps,
				Extras: map[string]interface{}{
					"x-oauth2-provider": "google",
				},
			},
			{
				Title:      "Service Account Key",
				Required:   []string{"auth_type", "service_account_info"},
				Properties: serviceAccountProps,
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "auth_type"},
		},
	}
}
