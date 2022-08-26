package google_auth

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/stretchr/testify/require"
)

func TestConfigSchema(t *testing.T) {
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
			name: "valid service credentials",
			conf: CredentialConfig{
				AuthType:        SERVICE_AUTH_TYPE,
				CredentialsJSON: "something",
			},
			want: nil,
		},
		{
			name: "valid client credentials",
			conf: CredentialConfig{
				AuthType:     CLIENT_AUTH_TYPE,
				ClientID:     "something",
				ClientSecret: "something",
				RefreshToken: "something",
			},
			want: nil,
		},
		{
			name: "invalid auth type",
			conf: CredentialConfig{AuthType: "Invalid"},
			want: fmt.Errorf("invalid credentials auth type %q", "Invalid"),
		},
		{
			name: "missing credentials json",
			conf: CredentialConfig{AuthType: SERVICE_AUTH_TYPE},
			want: fmt.Errorf("missing service account credentials JSON"),
		},
		{
			name: "missing refresh token",
			conf: CredentialConfig{
				AuthType:     CLIENT_AUTH_TYPE,
				ClientID:     "something",
				ClientSecret: "something",
			},
			want: fmt.Errorf("missing refresh token for oauth2"),
		},
		{
			name: "missing client id",
			conf: CredentialConfig{
				AuthType:     CLIENT_AUTH_TYPE,
				ClientSecret: "something",
				RefreshToken: "something",
			},
			want: fmt.Errorf("missing client ID for oauth2"),
		},
		{
			name: "missing client secret",
			conf: CredentialConfig{
				AuthType:     CLIENT_AUTH_TYPE,
				ClientID:     "something",
				RefreshToken: "something",
			},
			want: fmt.Errorf("missing client secret for oauth2"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.conf.Validate()
			require.Equal(t, tt.want, got)
		})
	}
}
