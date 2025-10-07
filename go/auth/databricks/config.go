package databricks

import (
	"encoding/json"
	"fmt"

	"github.com/invopop/jsonschema"
	pf "github.com/estuary/flow/go/protocols/flow"
	orderedmap "github.com/wk8/go-ordered-map/v2"
)

const (
	PAT    = "PAT"    // Personal Access Token
	OAuth2 = "oauth2" // OAuth2, needs client credentials and refresh token
)

type CredentialConfig struct {
	AuthType string `json:"auth_type"`

	// PAT fields
	PersonalAccessToken string `json:"personal_access_token"`

	// OAuth2 fields
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	AccessToken  string `json:"access_token"`
}

func (c *CredentialConfig) Validate() error {
	switch c.AuthType {
	case PAT:
		return c.validatePATCreds()
	case OAuth2:
		return c.validateOAuth2Creds()
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (c *CredentialConfig) validatePATCreds() error {
	if c.PersonalAccessToken == "" {
		return fmt.Errorf("missing personal_access_token")
	}

	return nil
}

func (c *CredentialConfig) validateOAuth2Creds() error {
	if c.AccessToken == "" {
		return fmt.Errorf("missing access_token for oauth2")
	}
	if c.ClientID == "" {
		return fmt.Errorf("missing client_id for oauth2")
	}
	if c.ClientSecret == "" {
		return fmt.Errorf("missing client_secret for oauth2")
	}

	return nil
}

// JSONSchema allows for the schema to be (semi-)manually specified when used with the
// github.com/invopop/jsonschema package in go-schema-gen, to fulfill the required schema shape for
// our oauth
func (CredentialConfig) JSONSchema() *jsonschema.Schema {
	patProps := orderedmap.New[string, *jsonschema.Schema]()
	patProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: PAT,
		Const:   PAT,
	})
	patProps.Set("personal_access_token", &jsonschema.Schema{
		Title:       "Personal Access Token",
		Description: "Your personal access token for accessing the SQL warehouse",
		Type:        "string",
		Extras: map[string]interface{}{
			"secret": true,
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
	oauth2Props.Set("access_token", &jsonschema.Schema{
		Type: "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "Databricks Credentials",
		Default:     map[string]string{"auth_type": PAT},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "Personal Access Token",
				Required:   []string{"auth_type", "personal_access_token"},
				Properties: patProps,
			},
			{
				Title:      "OAuth2",
				Required:   []string{"auth_type", "client_id", "client_secret", "access_token"},
				Properties: oauth2Props,
				Extras: map[string]interface{}{
					"x-oauth2-provider": "databricks",
				},
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "auth_type"},
		},
		Type: "object",
	}
}

// OAuthSpec returns the OAuth2 specification for Databricks.
// The OAuth endpoints are workspace-specific and constructed from the address configuration.
// According to Databricks documentation, the default client ID is "databricks-cli" and
// the scope is "all-apis" for API access.
func OAuthSpec() *pf.OAuth2 {
	// Databricks OAuth endpoints are workspace-specific and constructed from the address.
	// Template variables like {{{ config.address }}} will be populated by the control plane
	// with the actual address value from the endpoint configuration.
	//
	// For Databricks, we use scope "all-apis" to enable full API access
	return &pf.OAuth2{
		Provider: "databricks",
		// Authorization endpoint: https://<workspace-host>/oidc/v1/authorize
		// Include scope parameter for all-apis
		AuthUrlTemplate: "https://{{{ config.address }}}/oidc/v1/authorize?client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&response_type=code&scope={{#urlencode}}all-apis{{/urlencode}}&state={{#urlencode}}{{{ state }}}{{/urlencode}}",
		// Token endpoint: https://<workspace-host>/oidc/v1/token
		AccessTokenUrlTemplate: "https://{{{ config.address }}}/oidc/v1/token",
		// Token request body
		AccessTokenBody: "grant_type=authorization_code&client_id={{#urlencode}}{{{ client_id }}}{{/urlencode}}&client_secret={{#urlencode}}{{{ client_secret }}}{{/urlencode}}&redirect_uri={{#urlencode}}{{{ redirect_uri }}}{{/urlencode}}&code={{#urlencode}}{{{ code }}}{{/urlencode}}",
		// Token request headers
		AccessTokenHeadersJsonMap: map[string]json.RawMessage{
			"content-type": json.RawMessage(`"application/x-www-form-urlencoded"`),
		},
		// Response mapping - Databricks returns access_token which we need to store
		AccessTokenResponseJsonMap: map[string]json.RawMessage{
			"access_token": json.RawMessage(`"/access_token"`),
		},
	}
}
