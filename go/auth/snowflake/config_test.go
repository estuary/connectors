package snowflake

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/stretchr/testify/require"
)

func TestConfigSchema(t *testing.T) {
	t.Parallel()

	schema := schemagen.GenerateSchema("Test Config Schema", CredentialConfig{})
	formatted, err := json.MarshalIndent(schema, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, formatted)
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		conf CredentialConfig
		want error
	}{
		{
			name: "invalid auth type",
			conf: CredentialConfig{
				AuthType: "Invalid",
			},
			want: fmt.Errorf("invalid credentials auth type %q", "Invalid"),
		},
		{
			name: "missing user",
			conf: CredentialConfig{
				AuthType: UserPass,
				Password: "mypassword",
			},
			want: fmt.Errorf("missing user"),
		},
		{
			name: "missing password",
			conf: CredentialConfig{
				AuthType: UserPass,
				User:     "myuser",
			},
			want: fmt.Errorf("missing password"),
		},
		{
			name: "invalid private key",
			conf: CredentialConfig{
				AuthType:   JWT,
				User:       "myuser",
				PrivateKey: "some-invalid-key",
			},
			want: fmt.Errorf("invalid private key: must be PEM format"),
		},
		{
			name: "missing private key",
			conf: CredentialConfig{
				AuthType: JWT,
				User:     "myuser",
			},
			want: fmt.Errorf("missing private_key"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.conf.Validate()
			require.Equal(t, tt.want, got)
		})
	}
}
