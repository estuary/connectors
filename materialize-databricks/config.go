package main

import (
	"fmt"
	"strings"
	"net/url"
	"github.com/invopop/jsonschema"
	"github.com/iancoleman/orderedmap"
)

// config represents the endpoint configuration for sql server.
type config struct {
	Address     string           `json:"address" jsonschema:"title=Address,description=Host and port of the SQL warehouse (in the form of host[:port]). Port 443 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	HTTPPath    string           `json:"http_path" jsonschema:"title=HTTP path,description=HTTP path of your SQL warehouse"`
	CatalogName string           `json:"catalog_name" jsonschema:"title=Catalog Name,description=Name of your Unity Catalog."`
	SchemaName  string           `json:"schema_name" jsonschema:"title=Schema Name,description=Default schema to materialize to,default=default"`

	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication"`

}

const (
	// TODO: support Azure, GCP and OAuth authentication
	PAT_AUTH_TYPE  = "PAT" // personal access token
)

type credentialConfig struct {
	AuthType            string `json:"auth_type"`

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
			"secret":    true,
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
	params.Add("userAgentEntry", "Estuary Technologies Flow")

	var uri = url.URL{
		Host: address,
		Path: c.HTTPPath,
		User: url.UserPassword("token", c.Credentials.PersonalAccessToken),
		RawQuery: params.Encode(),
	}

	return strings.TrimLeft(uri.String(), "/")
}

