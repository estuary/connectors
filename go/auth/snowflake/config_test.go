package snowflake

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
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

func TestValidatePrivateKeyType(t *testing.T) {
	t.Parallel()

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	_, ed25519Key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	tests := []struct {
		name    string
		key     any
		wantErr string
	}{
		{name: "rsa", key: rsaKey},
		{
			name:    "ecdsa",
			key:     ecdsaKey,
			wantErr: "invalid private key: must be an RSA key, got *ecdsa.PrivateKey",
		},
		{
			name:    "ed25519",
			key:     ed25519Key,
			wantErr: "invalid private key: must be an RSA key, got ed25519.PrivateKey",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			der, err := x509.MarshalPKCS8PrivateKey(tt.key)
			require.NoError(t, err)

			conf := CredentialConfig{
				AuthType:   JWT,
				User:       "myuser",
				PrivateKey: string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
			}

			err = conf.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tt.wantErr)
			}
		})
	}
}
