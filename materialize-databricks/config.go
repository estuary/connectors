package main

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/estuary/connectors/go/dbt"
	m "github.com/estuary/connectors/go/materialize"
)

var featureFlagDefaults = map[string]bool{
	"datetime_keys_as_string": true,
	"retain_existing_data_on_backfill": false,
}

// config represents the endpoint configuration for sql server.
type config struct {
	Address       string            `json:"address" jsonschema:"title=Address,description=Host and port of the SQL warehouse (in the form of host[:port]). Port 443 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	HTTPPath      string            `json:"http_path" jsonschema:"title=HTTP path,description=HTTP path of your SQL warehouse" jsonschema_extras:"order=1"`
	CatalogName   string            `json:"catalog_name" jsonschema:"title=Catalog Name,description=Name of your Unity Catalog." jsonschema_extras:"order=2"`
	SchemaName    string            `json:"schema_name" jsonschema:"title=Schema Name,description=Default schema to materialize to,default=default" jsonschema_extras:"order=3"`
	HardDelete    bool              `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=4"`
	Credentials   *CredentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=5"`
	Schedule      m.ScheduleConfig  `json:"syncSchedule,omitempty" jsonschema:"title=Sync Schedule,description=Configure schedule of transactions for the materialization."`
	DBTJobTrigger dbt.JobConfig     `json:"dbt_job_trigger,omitempty" jsonschema:"title=dbt Cloud Job Trigger,description=Trigger a dbt Job when new data is available"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type advancedConfig struct {
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

// Validate the configuration.
func (c config) Validate() error {
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

	if err := c.Schedule.Validate(); err != nil {
		return err
	}

	if err := c.DBTJobTrigger.Validate(); err != nil {
		return err
	}

	return c.Credentials.Validate()
}

func (c config) DefaultNamespace() string {
	return c.SchemaName
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

// ToURI puts together address and http_path to form the full workspace URL
func (c config) ToURI() string {
	var address = c.Address
	if !strings.Contains(address, ":") {
		address = address + ":" + defaultPort
	}

	var params = make(url.Values)
	params.Add("catalog", c.CatalogName)
	params.Add("schema", c.SchemaName)
	params.Add("userAgentEntry", "Estuary Technologies Flow")

	var token string
	if c.Credentials.AuthType == PAT {
		token = c.Credentials.PersonalAccessToken
	} else if c.Credentials.AuthType == OAuth2 {
		// OAuth2 authentication using access token
		// The token is used similar to PAT
		token = c.Credentials.AccessToken
	}

	var uri = url.URL{
		Host:     address,
		Path:     c.HTTPPath,
		User:     url.UserPassword("token", token),
		RawQuery: params.Encode(),
	}

	return strings.TrimLeft(uri.String(), "/")
}
