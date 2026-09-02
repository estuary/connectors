package main

import (
	"context"
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

	t.Run("AWSIAMValid", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: AWSIAM}
		cfg.Credentials.AWSRegion = "us-east-1"
		cfg.Credentials.AWSRole = "arn:aws:iam::123456789012:role/flow-capture"

		require.NoError(t, cfg.Validate())
	})

	t.Run("AWSIAMMissingRegion", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: AWSIAM}
		cfg.Credentials.AWSRole = "arn:aws:iam::123456789012:role/flow-capture"

		require.ErrorContains(t, cfg.Validate(), "missing 'aws_region'")
	})

	t.Run("AWSIAMMissingRoleARN", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: AWSIAM}
		cfg.Credentials.AWSRegion = "us-east-1"

		require.ErrorContains(t, cfg.Validate(), "missing 'aws_role_arn'")
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

// awsIAMConfig is a config authenticating with AWS IAM, with the session credentials
// the control plane would have injected alongside the user's region and role.
func awsIAMConfig() Config {
	return Config{
		Address: "example.abcdefg.us-east-1.rds.amazonaws.com:3306",
		User:    "flow_capture",
		Credentials: &CredentialsConfig{
			AuthType: AWSIAM,
			IAMConfig: iam.IAMConfig{
				AWSConfig: iam.AWSConfig{
					AWSRegion: "us-east-1",
					AWSRole:   "arn:aws:iam::123456789012:role/flow-capture",
				},
				IAMTokens: iam.IAMTokens{AWSTokens: iam.AWSTokens{
					AWSAccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
					AWSSecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					AWSSessionToken:    "FwoGZXIvYXdzEXAMPLESESSIONTOKEN",
				}},
			},
		},
	}
}

func TestEffectivePassword(t *testing.T) {
	t.Run("UserPassword", func(t *testing.T) {
		var cfg = Config{Address: "example.com:3306", User: "flow_capture", Password: "secret1234"}
		cfg.normalizeCredentials()

		password, err := cfg.EffectivePassword(context.Background())
		require.NoError(t, err)
		require.Equal(t, "secret1234", password)
	})

	t.Run("AWSIAMToken", func(t *testing.T) {
		var cfg = awsIAMConfig()
		cfg.normalizeCredentials()

		require.NoError(t, cfg.Validate())
		password, err := cfg.EffectivePassword(context.Background())
		require.NoError(t, err)
		// The RDS auth token is a presigned request against the configured endpoint,
		// and is far longer than the 32 characters allowed for a real password.
		require.Greater(t, len(password), 32)
		require.Contains(t, password, "example.abcdefg.us-east-1.rds.amazonaws.com:3306")
		require.Contains(t, password, "X-Amz-Signature=")
		require.Contains(t, password, "DBUser=flow_capture")
	})

	t.Run("AWSIAMTokenIsFreshlyMinted", func(t *testing.T) {
		// Tokens expire fifteen minutes after they are built, so every call must mint
		// a new one rather than hand back a value cached at startup.
		var cfg = awsIAMConfig()
		cfg.normalizeCredentials()
		cfg.Credentials.AWSRegion = "us-west-2"

		password, err := cfg.EffectivePassword(context.Background())
		require.NoError(t, err)
		require.Contains(t, password, "us-west-2%2Frds-db%2Faws4_request")
	})

	t.Run("AWSIAMMissingSessionCredentials", func(t *testing.T) {
		var cfg = awsIAMConfig()
		cfg.Credentials.AWSAccessKeyID = ""
		cfg.normalizeCredentials()

		_, err := cfg.EffectivePassword(context.Background())
		require.ErrorContains(t, err, "missing iam session 'aws_access_key_id'")
	})

	t.Run("NormalizedShapeMatchesLegacy", func(t *testing.T) {
		var legacy = Config{Address: "example.com:3306", User: "flow_capture", Password: "secret1234"}
		var union = Config{
			Address: "example.com:3306",
			User:    "flow_capture",
			Credentials: &CredentialsConfig{
				AuthType:           UserPassword,
				UserPasswordConfig: UserPasswordConfig{Password: "secret1234"},
			},
		}

		legacy.normalizeCredentials()
		union.normalizeCredentials()

		legacyPassword, err := legacy.EffectivePassword(context.Background())
		require.NoError(t, err)
		unionPassword, err := union.EffectivePassword(context.Background())
		require.NoError(t, err)
		require.Equal(t, unionPassword, legacyPassword)
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
		password, err := cfg.EffectivePassword(context.Background())
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

		_, err := cfg.EffectivePassword(context.Background())
		require.ErrorContains(t, err, "missing 'azure_access_token'")
	})

	t.Run("UnsupportedAuthType", func(t *testing.T) {
		var cfg = Config{
			Address:     "example.com:3306",
			User:        "flow_capture",
			Credentials: &CredentialsConfig{AuthType: "Bogus"},
		}

		_, err := cfg.EffectivePassword(context.Background())
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
		{AWSIAM, true},
		{AzureIAM, true},
		{AuthType(iam.GCPIAM), true},
		{AuthType("Bogus"), true},
	} {
		t.Run(string(tc.authType), func(t *testing.T) {
			var cfg = Config{Credentials: &CredentialsConfig{AuthType: tc.authType}}
			require.Equal(t, tc.expect, cfg.requiresTLS())
		})
	}
}
