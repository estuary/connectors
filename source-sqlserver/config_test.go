package main

import (
	"testing"

	"github.com/estuary/connectors/go/auth/iam"
	"github.com/stretchr/testify/require"
)

func TestNormalizeCredentials(t *testing.T) {
	t.Run("LegacyPasswordPromotes", func(t *testing.T) {
		var cfg = Config{Address: "example.com:1433", User: "flow_capture", Password: "secret1234", Database: "flow"}

		cfg.normalizeCredentials()

		require.Empty(t, cfg.Password)
		require.NotNil(t, cfg.Credentials)
		require.Equal(t, UserPassword, cfg.Credentials.AuthType)
		require.Equal(t, "secret1234", cfg.Credentials.Password)
	})

	t.Run("CredentialsWinOverLegacyPassword", func(t *testing.T) {
		var cfg = Config{
			Address:  "example.com:1433",
			User:     "flow_capture",
			Password: "stale-legacy-password",
			Database: "flow",
			Credentials: &CredentialsConfig{
				AuthType:           UserPassword,
				UserPasswordConfig: UserPasswordConfig{Password: "current-password"},
			},
		}

		cfg.normalizeCredentials()

		require.Empty(t, cfg.Password)
		require.Equal(t, "current-password", cfg.Credentials.Password)
	})

	t.Run("NormalizedShapeInURI", func(t *testing.T) {
		var legacy = Config{Address: "example.com:1433", User: "flow_capture", Password: "secret1234", Database: "flow"}
		var union = Config{
			Address:  "example.com:1433",
			User:     "flow_capture",
			Database: "flow",
			Credentials: &CredentialsConfig{
				AuthType:           UserPassword,
				UserPasswordConfig: UserPasswordConfig{Password: "secret1234"},
			},
		}

		legacy.normalizeCredentials()
		union.normalizeCredentials()

		unionURI, err := union.ToURI()
		require.NoError(t, err)
		legacyURI, err := legacy.ToURI()
		require.NoError(t, err)
		require.Equal(t, unionURI, legacyURI)
	})
}

func TestConfigValidate(t *testing.T) {
	var valid = func() Config {
		return Config{Address: "example.com:1433", User: "flow_capture", Database: "flow", Credentials: &CredentialsConfig{
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

	t.Run("UnknownAuthType", func(t *testing.T) {
		var cfg = valid()
		cfg.Credentials = &CredentialsConfig{AuthType: "Bogus"}

		require.ErrorContains(t, cfg.Validate(), "unknown 'auth_type'")
	})
}

func TestAWSIAMConfigURI(t *testing.T) {
	var cfg = Config{
		Address:  "example.rds.amazonaws.com",
		User:     "flow_capture",
		Database: "flow",
		Credentials: &CredentialsConfig{
			AuthType: AWSIAM,
			IAMConfig: iam.IAMConfig{
				AWSConfig: iam.AWSConfig{AWSRegion: "us-east-1", AWSRole: "arn:aws:iam::123456789012:role/flow-capture"},
			},
		},
	}
	cfg.SetDefaults()
	cfg.normalizeCredentials()

	// The DSN carries the user but no password: the RDS auth token is supplied
	// out-of-band by the connector's access token provider.
	uri, err := cfg.ToURI()
	require.NoError(t, err)
	require.Equal(t,
		"sqlserver://flow_capture@example.rds.amazonaws.com:1433?TrustServerCertificate=true&app+name=Flow+CDC+Connector&database=flow&encrypt=true",
		uri)
}

func TestUnsupportedAuthTypeURI(t *testing.T) {
	var cfg = Config{
		Address:     "example.com:1433",
		User:        "flow_capture",
		Database:    "flow",
		Credentials: &CredentialsConfig{AuthType: "Bogus"},
	}

	_, err := cfg.ToURI()
	require.ErrorContains(t, err, `unsupported 'auth_type' "Bogus"`)

	_, err = cfg.buildConnector()
	require.ErrorContains(t, err, `unsupported 'auth_type' "Bogus"`)
}
