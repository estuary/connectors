package schemagen

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

type testConfig struct {
	Password string `json:"password" jsonschema:"title=Password,description=Secret password." jsonschema_extras:"secret=true,order=1"`
	Username string `json:"username" jsonschema:"title=Username,description=Test user." jsonschema_extras:"order=0"`
	Advanced struct {
		LongAdvanced          string `json:"long_advanced,omitempty" jsonschema:"title=Example,description=Some long description." jsonschema_extras:"multiline=true"`
		SecretOrderedAdvanced string `json:"secret_advanced,omitempty" jsonschema:"title=Secret Advanced,description=Some secret advanced config with ordering." jsonschema_extras:"secret=true,order=0"`
	} `json:"advanced,omitempty" jsonschema_extras:"advanced=true"`
	Credentials struct {
		Type           string `json:"type" jsonschema:"title=Authentication Type,enum=username_password,enum=service_account" jsonschema_extras:"oneOf_discriminator=true,oneOf_group=1,oneOf_group=2"`
		Username       string `json:"username" jsonschema:"title=Username" jsonschema_extras:"oneOf_group=1"`
		Password       string `json:"password" jsonschema:"title=Password" jsonschema_extras:"oneOf_group=1"`
		ServiceAccount string `json:"service_account" jsonschema:"title=Service Account" jsonschema_extras:"oneOf_group=2"`
	} `json:"credentials" jsonschema:"title=Credentials" jsonschema_extras:"oneOf=true,oneOf_titles=Username and Password+Service Account"`
}

func TestGenerateSchema(t *testing.T) {
	got := GenerateSchema("Test Schema", testConfig{})
	formatted, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}
