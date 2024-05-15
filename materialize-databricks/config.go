package main

import (
	"fmt"
	"net/url"
	"strings"

	m "github.com/estuary/connectors/go/protocols/materialize"
	"github.com/iancoleman/orderedmap"
	"github.com/invopop/jsonschema"
)

// config represents the endpoint configuration for sql server.
type config struct {
	Address     string `json:"address" jsonschema:"title=Address,description=Host and port of the SQL warehouse (in the form of host[:port]). Port 443 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	HTTPPath    string `json:"http_path" jsonschema:"title=HTTP path,description=HTTP path of your SQL warehouse"`
	CatalogName string `json:"catalog_name" jsonschema:"title=Catalog Name,description=Name of your Unity Catalog."`
	SchemaName  string `json:"schema_name" jsonschema:"title=Schema Name,description=Default schema to materialize to,default=default"`

	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce active warehouse time by increasing the delay between updates. Defaults to 30 minutes if unset.,enum=0s,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`

	HardDelete bool `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled, items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false"`
}

const (
	// TODO: support Azure, GCP and OAuth authentication
	PAT_AUTH_TYPE = "PAT" // personal access token
)

type credentialConfig struct {
	AuthType string `json:"auth_type"`

	PersonalAccessToken string `json:"personal_access_token"`
}

func (c *credentialConfig) Validate() error {
	switch c.AuthType {
	case PAT_AUTH_TYPE:
		return c.validatePATCreds()
	default:
		return fmt.Errorf("invalid credentials auth type %q", c.AuthType)
	}
}

func (c *credentialConfig) validatePATCreds() error {
	if c.PersonalAccessToken == "" {
		return fmt.Errorf("missing personal_access_token")
	}

	return nil
}

// JSONSchema allows for the schema to be (semi-)manually specified when used with the
// github.com/invopop/jsonschema package in go-schema-gen, to fullfill the required schema shape for
// our oauth
func (credentialConfig) JSONSchema() *jsonschema.Schema {
	patProps := orderedmap.New()
	patProps.Set("auth_type", &jsonschema.Schema{
		Type:    "string",
		Default: PAT_AUTH_TYPE,
		Const:   PAT_AUTH_TYPE,
	})
	patProps.Set("personal_access_token", &jsonschema.Schema{
		Title:       "Personal Access Token",
		Description: "Personal Access Token,description=Your personal access token for accessing the SQL warehouse",
		Type:        "string",
		Extras: map[string]interface{}{
			"secret": true,
		},
	})

	return &jsonschema.Schema{
		Title:       "Authentication",
		Description: "Databricks Credentials",
		Default:     map[string]string{"auth_type": PAT_AUTH_TYPE},
		OneOf: []*jsonschema.Schema{
			{
				Title:      "Personal Access Token",
				Required:   []string{"auth_type", "personal_access_token"},
				Properties: patProps,
			},
		},
		Extras: map[string]interface{}{
			"discriminator": map[string]string{"propertyName": "auth_type"},
		},
		Type: "object",
	}
}

// Validate the configuration.
func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"http_path", c.HTTPPath},
		{"catalog_name", c.CatalogName},
		{"schema_name", c.SchemaName},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if _, err := m.ParseDelay(c.Advanced.UpdateDelay); err != nil {
		return err
	}

	return c.Credentials.Validate()
}

// ToURI puts together address and http_path to form the full workspace URL
func (c *config) ToURI() string {
	var address = c.Address
	if !strings.Contains(address, ":") {
		address = address + ":" + defaultPort
	}

	var params = make(url.Values)
	params.Add("catalog", c.CatalogName)
	params.Add("schema", c.SchemaName)
	params.Add("userAgentEntry", "Estuary Technologies Flow")

	var uri = url.URL{
		Host:     address,
		Path:     c.HTTPPath,
		User:     url.UserPassword("token", c.Credentials.PersonalAccessToken),
		RawQuery: params.Encode(),
	}

	return strings.TrimLeft(uri.String(), "/")
}
