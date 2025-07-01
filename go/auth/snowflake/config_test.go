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

	// fakeKeyForTesting is a dummy RSA private key used only in unit tests.
	// This key is NOT valid for any real environment.
	const fakeKeyForTesting string = `-----BEGIN PRIVATE KEY-----
MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAsQmnve+TbAnN+qb7
44ROzbTrgrwWTgy94zeObm4hxu8DD12aTg15T6zLiqgKcW+LmFfK0LxuMic04vlk
96umOwIDAQABAkByrsSA369qLzfFhWJq0gySaP6fI+R3Dv37MMQNeT5cNOiAevl5
TYivoAwml4UvSsWWwtUhB6dy2nOpkOo+tcyhAiEA3g/AGGz1PNSdC2hAWc9vlgD8
2kQCnxxXtf+3lUKAD7kCIQDMGFRgBl3Mjk4XMUbyt6LNPax3d447nHArXxANbwEX
kwIgMGRqTmhgQTNsTfIYI/pKrPvmHoK3t4jrrGPf1A077zECID+5zQaalkkbmdh2
A3Af1I5/Fk3LH7cPNprkOM/o/A9PAiAD5MeunU6xq6y88cjdR0EHgVxg08xS49zV
24T49VaHUw==
-----END PRIVATE KEY-----`

	tests := []struct {
		name string
		conf CredentialConfig
		want error
	}{
		{
			name: "valid user & password credentials",
			conf: CredentialConfig{
				AuthType:        UserPass,
				User:    "myuser",
				Password: "mypassword",
			},
			want: nil,
		},
		{
			name: "valid jwt credentials",
			conf: CredentialConfig{
				AuthType:   JWT,
				User:       "something",
				PrivateKey: fakeKeyForTesting,
			},
			want: nil,
		},
		{
			name: "invalid auth type",
			conf: CredentialConfig{AuthType: "Invalid"},
			want: fmt.Errorf("invalid credentials auth type %q", "Invalid"),
		},
		{
			name: "missing user for UserPass",
			conf: CredentialConfig{AuthType: UserPass, Password: "mypassword"},
			want: fmt.Errorf("missing user"),
		},
		{
			name: "missing password",
			conf: CredentialConfig{AuthType: UserPass, User: "myuser"},
			want: fmt.Errorf("missing password"),
		},
		{
			name: "missing user for JWT",
			conf: CredentialConfig{AuthType: JWT, PrivateKey: fakeKeyForTesting},
			want: fmt.Errorf("missing user"),
		},
		{
			name: "missing private key",
			conf: CredentialConfig{
				AuthType:     JWT,
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
