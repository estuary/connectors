package main

import (
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/invopop/jsonschema"
)

var featureFlagDefaults = map[string]bool{}

type AuthType string

const (
	OAuth2AuthType AuthType = "OAuth2"
)

type OAuth2Credentials struct {
	ClientID             string `json:"client_id,default=missing" jsonschema_extras:"secret=true"`
	ClientSecret         string `json:"client_secret,default=missing" jsonschema_extras:"secret=true"`
	RefreshToken         string `json:"refresh_token,default=missing" jsonschema_extras:"secret=true"`
	AccessToken          string `json:"access_token,default=missing" jsonschema_extras:"secret=true"`
	AccessTokenExpiresAt string `json:"access_token_expires_at,default=missing"`
}

type Credentials struct {
	AuthType AuthType `json:"auth_type"`

	OAuth2Credentials
}

func (Credentials) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("OAuth2", OAuth2Credentials{}, string(OAuth2AuthType)).WithOAuth2Provider(oauth2Provider),
	}

	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(OAuth2AuthType), subSchemas...)
}

type AdvancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

type Config struct {
	Credentials *Credentials `json:"credentials" jsonschema:"title=Authentication"`

	Advanced AdvancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

func (c *Config) Parse() (*ValidConfig, error) {
	return &ValidConfig{}, nil
}

func (c *Config) Validate() error {
	_, err := c.Parse()
	return err
}

func (c *Config) DefaultNamespace() string {
	return ""
}

func (c *Config) FeatureFlags() (raw string, defaults map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

type ValidConfig struct {
}

type Resource struct {
	Path   string `json:"path" jsonschema_extras:"x-collection-name=true"`
	Object string `json:"object" jsonschema:"title=Object type,enum=Company,enum=Contact,default=Contact"`
}

func (r *Resource) Validate() error {
	return nil
}

func (r *Resource) WithDefaults(config *Config) *Resource {
	return r
}

func (r *Resource) Parameters() ([]string, bool, error) {
	return []string{r.Path}, false, nil
}
