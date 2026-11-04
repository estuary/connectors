package connector

import (
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"

	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	"github.com/estuary/connectors/go/writer"
)

func TestConfigURI(t *testing.T) {
	for name, cfg := range map[string]config{
		"User & Password Authentication": {
			Host:     "orgname-accountname.snowflakecomputing.com",
			Database: "mydb",
			Schema:   "myschema",
			Credentials: &snowflake_auth.CredentialConfig{
				AuthType:   snowflake_auth.UserPass,
				User:       "will",
				Password:   "some+complex/password",
				PrivateKey: "non-existant-jwt",
			},
		},
		"Optional Parameters": {
			Host:      "orgname-accountname.snowflakecomputing.com",
			Database:  "mydb",
			Schema:    "myschema",
			Warehouse: "mywarehouse",
			Role:      "myrole",
			Account:   "myaccount",
			Credentials: &snowflake_auth.CredentialConfig{
				AuthType:   snowflake_auth.UserPass,
				User:       "alex",
				Password:   "mysecret",
				PrivateKey: "non-existant-jwt",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, cfg.Validate())
			uri, err := cfg.toURI(true, "")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, uri)
		})
	}
}

func TestTimestampTypeHelpers(t *testing.T) {
	t.Run("toTimestampTypeMapping", func(t *testing.T) {
		require.Equal(t, timestampLTZ, timestampTypeLTZ.toTimestampTypeMapping())
		require.Equal(t, timestampNTZ, timestampTypeNTZDiscard.toTimestampTypeMapping())
		require.Equal(t, timestampNTZ, timestampTypeNTZNormalize.toTimestampTypeMapping())
		require.Equal(t, timestampTZ, timestampTypeTZ.toTimestampTypeMapping())
	})

	t.Run("isCompatibleWith", func(t *testing.T) {
		require.True(t, timestampTypeLTZ.isCompatibleWith(timestampLTZ))
		require.False(t, timestampTypeLTZ.isCompatibleWith(timestampNTZ))
		require.False(t, timestampTypeLTZ.isCompatibleWith(timestampTZ))

		require.True(t, timestampTypeNTZDiscard.isCompatibleWith(timestampNTZ))
		require.True(t, timestampTypeNTZNormalize.isCompatibleWith(timestampNTZ))
		require.False(t, timestampTypeNTZDiscard.isCompatibleWith(timestampLTZ))

		require.True(t, timestampTypeTZ.isCompatibleWith(timestampTZ))
		require.False(t, timestampTypeTZ.isCompatibleWith(timestampLTZ))
		require.False(t, timestampTypeTZ.isCompatibleWith(timestampNTZ))
	})
}

func TestFileSizeLimit(t *testing.T) {
	var base = config{
		Host:     "orgname-accountname.snowflakecomputing.com",
		Database: "mydb",
		Schema:   "myschema",
		Credentials: &snowflake_auth.CredentialConfig{
			AuthType:   snowflake_auth.UserPass,
			User:       "will",
			Password:   "some+complex/password",
			PrivateKey: "non-existant-jwt",
		},
	}

	for _, tc := range []struct {
		name    string
		limit   int
		want    int
		wantErr string
	}{
		{name: "blank uses the default", limit: 0, want: writer.DefaultJsonFileSizeLimit},
		{name: "minimum", limit: minFileSizeLimit, want: minFileSizeLimit},
		{name: "custom", limit: 64 << 20, want: 64 << 20},
		{name: "below the minimum", limit: 250, wantErr: "fileSizeLimit 250 must be at least 1048576 bytes"},
		{name: "negative", limit: -1, wantErr: "fileSizeLimit -1 must be at least 1048576 bytes"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg = base
			cfg.Advanced.FileSizeLimit = tc.limit
			if tc.wantErr != "" {
				require.ErrorContains(t, cfg.Validate(), tc.wantErr)
				return
			}
			require.NoError(t, cfg.Validate())
			require.Equal(t, tc.want, cfg.fileSizeLimit())
		})
	}
}
