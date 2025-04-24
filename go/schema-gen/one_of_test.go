package schemagen

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/invopop/jsonschema"
	"github.com/stretchr/testify/require"
)

func TestOneOfSchema(t *testing.T) {
	got := GenerateSchema("Test Schema", endpointConfig{})
	formatted, err := json.MarshalIndent(got, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}

type endpointConfig struct {
	SomeStandardInput string `json:"some_standard_input"`

	Authentication authenticationConfig `json:"authentication"`
}

type authenticationConfig struct {
	AuthType string `json:"auth_type"`

	userPassword
	jwt
	apiKey
}

func (authenticationConfig) JSONSchema() *jsonschema.Schema {
	return OneOfSchema("Authentication", "This is an example schema of oneOf combinations for authentication", "auth_type", "user_password",
		OneOfSubSchema("User Name and Password", userPassword{}, "user_password"),
		OneOfSubSchema("JSON Web Token (JWT)", jwt{}, "jwt"),
		OneOfSubSchema("API Key", apiKey{}, "api_key"),
	)
}

type userPassword struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type jwt struct {
	JWT string `json:"jwt"`
}

type apiKey struct {
	APIKey string `json:"api_key"`
}
