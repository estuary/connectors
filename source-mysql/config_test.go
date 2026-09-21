package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/auth/iam"
	mysqltls "github.com/estuary/connectors/go/mysql/tls"
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

	t.Run("GCPIAMValid", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: GCPIAM}
		cfg.Credentials.GCPServiceAccount = "flow-capture@example-project.iam.gserviceaccount.com"
		cfg.Credentials.GCPWorkloadAudience = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider"

		require.NoError(t, cfg.Validate())
	})

	t.Run("GCPIAMMissingServiceAccount", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: GCPIAM}
		cfg.Credentials.GCPWorkloadAudience = "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider"

		require.ErrorContains(t, cfg.Validate(), "missing 'gcp_service_account_to_impersonate'")
	})

	t.Run("GCPIAMMissingWorkloadAudience", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: GCPIAM}
		cfg.Credentials.GCPServiceAccount = "flow-capture@example-project.iam.gserviceaccount.com"

		require.ErrorContains(t, cfg.Validate(), "missing 'gcp_workload_identity_pool_audience'")
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

	t.Run("GCPIAMToken", func(t *testing.T) {
		// Cloud SQL login tokens are far longer than 32 characters, which is fine:
		// the replication password length limit only applies to password auth.
		var token = strings.Repeat("token", 200)
		var cfg = Config{
			Address: "203.0.113.10:3306",
			User:    "flow-capture",
			Credentials: &CredentialsConfig{
				AuthType: GCPIAM,
				IAMConfig: iam.IAMConfig{
					GCPConfig: iam.GCPConfig{
						GCPServiceAccount:   "flow-capture@example-project.iam.gserviceaccount.com",
						GCPWorkloadAudience: "//iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/test-pool/providers/test-provider",
					},
					IAMTokens: iam.IAMTokens{GCPTokens: iam.GCPTokens{GCPAccessToken: token}},
				},
			},
		}
		cfg.normalizeCredentials()

		require.NoError(t, cfg.Validate())
		password, err := cfg.EffectivePassword(context.Background())
		require.NoError(t, err)
		require.Equal(t, token, password)
	})

	t.Run("GCPIAMMissingToken", func(t *testing.T) {
		var cfg = Config{
			Address:     "203.0.113.10:3306",
			User:        "flow-capture",
			Credentials: &CredentialsConfig{AuthType: GCPIAM},
		}
		cfg.normalizeCredentials()

		_, err := cfg.EffectivePassword(context.Background())
		require.ErrorContains(t, err, "missing 'gcp_access_token'")
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

func TestDefaultSSLMode(t *testing.T) {
	// Pinned per auth type so that adding another IAM method cannot silently reopen
	// the plaintext fallback. The unknown-type case pins the fail-closed default.
	for _, tc := range []struct {
		authType AuthType
		expect   string
	}{
		{UserPassword, mysqltls.ModePreferred},
		{AWSIAM, mysqltls.ModeRequired},
		{GCPIAM, mysqltls.ModeRequired},
		{AzureIAM, mysqltls.ModeRequired},
		{AuthType("Bogus"), mysqltls.ModeRequired},
	} {
		t.Run(string(tc.authType), func(t *testing.T) {
			var cfg = Config{Credentials: &CredentialsConfig{AuthType: tc.authType}}
			require.Equal(t, tc.expect, cfg.sslSettings().Mode)
			require.Equal(t, tc.expect == mysqltls.ModePreferred, cfg.sslSettings().AllowsPlaintextFallback())
		})
	}

	t.Run("LegacyPasswordIsPreferred", func(t *testing.T) {
		var cfg = Config{Password: "secret"}
		require.Equal(t, mysqltls.ModePreferred, cfg.sslSettings().Mode)
	})
	t.Run("ExplicitModeWins", func(t *testing.T) {
		var cfg = Config{Credentials: &CredentialsConfig{AuthType: AWSIAM}}
		cfg.Advanced.SSLMode = mysqltls.ModeVerifyIdentity
		require.Equal(t, mysqltls.ModeVerifyIdentity, cfg.sslSettings().Mode)
	})
}

func TestSSLModeValidation(t *testing.T) {
	var base = func() Config {
		return Config{Address: "db.example.com:3306", User: "flow_capture", Password: "secret"}
	}
	t.Run("UnsetIsValid", func(t *testing.T) {
		var cfg = base()
		require.NoError(t, cfg.Validate())
	})
	t.Run("UnknownMode", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.SSLMode = "VERIFY_CA"
		require.ErrorContains(t, cfg.Validate(), "unknown setting")
	})
	t.Run("VerifyCARequiresCA", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.SSLMode = mysqltls.ModeVerifyCA
		require.ErrorContains(t, cfg.Validate(), "'ssl_server_ca' is required")
	})
	t.Run("VerifyIdentityWithoutCA", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.SSLMode = mysqltls.ModeVerifyIdentity
		require.NoError(t, cfg.Validate())
	})
	t.Run("CARequiresVerifyingMode", func(t *testing.T) {
		for _, mode := range []string{"", mysqltls.ModeDisabled, mysqltls.ModePreferred, mysqltls.ModeRequired} {
			var name = mode
			if name == "" {
				name = "Unset"
			}
			t.Run(name, func(t *testing.T) {
				var cfg = base()
				cfg.Advanced.SSLMode = mode
				cfg.Advanced.SSLServerCA = testCAPEM(t)
				require.ErrorContains(t, cfg.Validate(), "does not verify the server certificate")
			})
		}
		t.Run("VerifyIdentity", func(t *testing.T) {
			var cfg = base()
			cfg.Advanced.SSLMode = mysqltls.ModeVerifyIdentity
			cfg.Advanced.SSLServerCA = testCAPEM(t)
			require.NoError(t, cfg.Validate())
		})
	})
	// Under bearer-token auth the password is a short-lived credential, so every
	// mode that could put it on the wire unencrypted must be rejected.
	t.Run("BearerTokenRequiresEncryptedMode", func(t *testing.T) {
		var iamConfig = func() Config {
			var cfg = base()
			cfg.Password = ""
			cfg.Credentials = &CredentialsConfig{
				AuthType: AzureIAM,
				IAMConfig: iam.IAMConfig{
					AzureConfig: iam.AzureConfig{
						AzureClientID: "11111111-2222-3333-4444-555555555555",
						AzureTenantID: "66666666-7777-8888-9999-000000000000",
					},
					IAMTokens: iam.IAMTokens{AzureTokens: iam.AzureTokens{AzureAccessToken: "tok"}},
				},
			}
			return cfg
		}
		for _, tc := range []struct {
			mode    string
			allowed bool
		}{
			{"", true}, // Defaults to 'required'.
			{mysqltls.ModeDisabled, false},
			{mysqltls.ModePreferred, false},
			{mysqltls.ModeRequired, true},
			{mysqltls.ModeVerifyIdentity, true},
		} {
			var name = tc.mode
			if name == "" {
				name = "Unset"
			}
			t.Run(name, func(t *testing.T) {
				var cfg = iamConfig()
				cfg.Advanced.SSLMode = tc.mode
				if tc.allowed {
					require.NoError(t, cfg.Validate())
					require.True(t, cfg.sslSettings().GuaranteesEncryption())
				} else {
					require.ErrorContains(t, cfg.Validate(), "must never be sent over an unencrypted connection")
					require.False(t, cfg.sslSettings().GuaranteesEncryption())
				}
			})
		}

		t.Run("VerifyCAWithCA", func(t *testing.T) {
			var cfg = iamConfig()
			cfg.Advanced.SSLMode = mysqltls.ModeVerifyCA
			cfg.Advanced.SSLServerCA = testCAPEM(t)
			require.NoError(t, cfg.Validate())
		})
	})
	t.Run("PreferredAllowedWithPassword", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.SSLMode = mysqltls.ModePreferred
		require.NoError(t, cfg.Validate())
	})
	t.Run("DisabledAllowedWithPassword", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.SSLMode = mysqltls.ModeDisabled
		require.NoError(t, cfg.Validate())
	})
	t.Run("ServerHost", func(t *testing.T) {
		var cfg = base()
		require.Equal(t, "db.example.com", cfg.serverHost())
		require.Equal(t, "db.example.com", (&Config{Address: "db.example.com"}).serverHost())
	})
}

// testCAPEM returns a throwaway self-signed CA certificate in PEM form, for
// cases that need 'ssl_server_ca' to contain something that actually parses.
func testCAPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	var tmpl = &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}
