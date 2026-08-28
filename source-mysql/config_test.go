package main

import (
	"strings"
	"testing"

	"github.com/estuary/connectors/go/auth/iam"
	"github.com/stretchr/testify/require"
)

func TestNormalizeCredentials(t *testing.T) {
	t.Run("LegacyPasswordPromotes", func(t *testing.T) {
		var cfg = Config{Address: "example.com:3306", User: "flow_capture", Password: "secret1234"}

		cfg.normalizeCredentials()

		require.Empty(t, cfg.Password)
		require.NotNil(t, cfg.Credentials)
		require.Equal(t, UserPassword, cfg.Credentials.AuthType)
		require.Equal(t, "secret1234", cfg.Credentials.Password)
	})

	t.Run("CredentialsWinOverLegacyPassword", func(t *testing.T) {
		var cfg = Config{
			Address:  "example.com:3306",
			User:     "flow_capture",
			Password: "stale-legacy-password",
			Credentials: &CredentialsConfig{
				AuthType:           UserPassword,
				UserPasswordConfig: UserPasswordConfig{Password: "current-password"},
			},
		}

		cfg.normalizeCredentials()

		require.Empty(t, cfg.Password)
		require.Equal(t, "current-password", cfg.Credentials.Password)
	})
}

func TestConfigValidate(t *testing.T) {
	var valid = func() Config {
		return Config{Address: "example.com:3306", User: "flow_capture", Credentials: &CredentialsConfig{
			AuthType:           UserPassword,
			UserPasswordConfig: UserPasswordConfig{Password: "secret1234"},
		}}
	}

	t.Run("UserPasswordCredentials", func(t *testing.T) {
		var cfg = valid()

		require.NoError(t, cfg.Validate())
	})

	t.Run("LegacyPasswordOnly", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = nil
		cfg.Password = "secret1234"

		require.NoError(t, cfg.Validate())
	})

	t.Run("NoCredentialsAtAll", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = nil

		require.ErrorContains(t, cfg.Validate(), "missing 'credentials'")
	})

	t.Run("EmptyPasswordCredentials", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials.Password = ""

		require.ErrorContains(t, cfg.Validate(), "missing 'password'")
	})

	t.Run("OverlongPasswordCredentials", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials.Password = strings.Repeat("x", 33)

		require.ErrorContains(t, cfg.Validate(), "cannot exceed 32 characters")
	})

	t.Run("OverlongLegacyPassword", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = nil
		cfg.Password = strings.Repeat("x", 33)

		require.ErrorContains(t, cfg.Validate(), "cannot exceed 32 characters")
	})

	t.Run("AzureIAMValid", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: AzureIAM}
		cfg.Credentials.AzureClientID = "11111111-2222-3333-4444-555555555555"
		cfg.Credentials.AzureTenantID = "66666666-7777-8888-9999-000000000000"

		require.NoError(t, cfg.Validate())
	})

	t.Run("AzureIAMMissingClientID", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: AzureIAM}
		cfg.Credentials.AzureTenantID = "66666666-7777-8888-9999-000000000000"

		require.ErrorContains(t, cfg.Validate(), "missing 'azure_client_id'")
	})

	t.Run("UnknownAuthType", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: "Bogus"}

		require.ErrorContains(t, cfg.Validate(), "unknown 'auth_type'")
	})
}

func TestEffectivePassword(t *testing.T) {
	t.Run("UserPassword", func(t *testing.T) {
		var cfg = Config{Address: "example.com:3306", User: "flow_capture", Password: "secret1234"}
		cfg.normalizeCredentials()

		password, err := cfg.EffectivePassword()
		require.NoError(t, err)
		require.Equal(t, "secret1234", password)
	})

	t.Run("AzureIAMToken", func(t *testing.T) {
		// Entra access tokens are far longer than 32 characters, which is fine:
		// the replication password length limit only applies to password auth.
		var token = strings.Repeat("token", 400)
		var cfg = Config{
			Address: "example.mysql.database.azure.com:3306",
			User:    "flow_capture",
			Credentials: &CredentialsConfig{
				AuthType: AzureIAM,
				IAMConfig: iam.IAMConfig{
					AzureConfig: iam.AzureConfig{
						AzureClientID: "11111111-2222-3333-4444-555555555555",
						AzureTenantID: "66666666-7777-8888-9999-000000000000",
					},
					IAMTokens: iam.IAMTokens{AzureTokens: iam.AzureTokens{AzureAccessToken: token}},
				},
			},
		}
		cfg.normalizeCredentials()

		require.NoError(t, cfg.Validate())
		password, err := cfg.EffectivePassword()
		require.NoError(t, err)
		require.Equal(t, token, password)
	})

	t.Run("AzureIAMMissingToken", func(t *testing.T) {
		var cfg = Config{
			Address:     "example.mysql.database.azure.com:3306",
			User:        "flow_capture",
			Credentials: &CredentialsConfig{AuthType: AzureIAM},
		}
		cfg.normalizeCredentials()

		_, err := cfg.EffectivePassword()
		require.ErrorContains(t, err, "missing 'azure_access_token'")
	})

	t.Run("UnsupportedAuthType", func(t *testing.T) {
		var cfg = Config{
			Address:     "example.com:3306",
			User:        "flow_capture",
			Credentials: &CredentialsConfig{AuthType: "Bogus"},
		}

		_, err := cfg.EffectivePassword()
		require.ErrorContains(t, err, `unsupported 'auth_type' "Bogus"`)
	})
}

func TestRequiresTLS(t *testing.T) {
	// Pinned per auth type so that adding another IAM method cannot silently reopen
	// the plaintext fallback. The unknown-type case pins the fail-closed default.
	for _, tc := range []struct {
		authType AuthType
		expect   bool
	}{
		{UserPassword, false},
		{AzureIAM, true},
		{AuthType(iam.AWSIAM), true},
		{AuthType(iam.GCPIAM), true},
		{AuthType("Bogus"), true},
	} {
		t.Run(string(tc.authType), func(t *testing.T) {
			var cfg = Config{Credentials: &CredentialsConfig{AuthType: tc.authType}}
			require.Equal(t, tc.expect, cfg.requiresTLS())
		})
	}
}
