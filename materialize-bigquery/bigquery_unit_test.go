package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

func TestSpecification(t *testing.T) {
	var resp, err = newBigQueryDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestConfigValidateCredentialsWithEndpoint(t *testing.T) {
	var validCreds = `{"type": "service_account"}`
	var base = func() config {
		return config{
			ProjectID: "project",
			Region:    "region",
			Dataset:   "dataset",
			Bucket:    "bucket",
		}
	}

	t.Run("endpoint unset requires credentials", func(t *testing.T) {
		var cfg = base()
		require.ErrorContains(t, cfg.Validate(), "credentials must be valid JSON")
	})

	t.Run("endpoint unset with credentials validates as before", func(t *testing.T) {
		var cfg = base()
		cfg.CredentialsJSON = validCreds
		require.NoError(t, cfg.Validate())
	})

	t.Run("endpoint set with credentials absent passes and disables auth", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.Endpoint = "http://localhost:9050/bigquery/v2/"
		require.NoError(t, cfg.Validate())

		opt, err := cfg.CredentialsClientOption()
		require.NoError(t, err)
		require.Equal(t, option.WithoutAuthentication(), opt)
	})

	t.Run("endpoint set rejects the deprecated top-level credentials_json", func(t *testing.T) {
		for name, creds := range map[string]string{"valid": validCreds, "invalid": "not json"} {
			t.Run(name, func(t *testing.T) {
				var cfg = base()
				cfg.Advanced.Endpoint = "http://localhost:9050/bigquery/v2/"
				cfg.CredentialsJSON = creds
				require.ErrorContains(t, cfg.Validate(), "'credentials'")
			})
		}
	})

	t.Run("endpoint set with structured credentials still validates them", func(t *testing.T) {
		var cfg = base()
		cfg.Advanced.Endpoint = "http://localhost:9050/bigquery/v2/"
		cfg.Credentials = &CredentialsConfig{AuthType: CredentialsJSON}
		require.ErrorContains(t, cfg.Validate(), "missing 'credentials_json'")

		cfg.Credentials.CredentialsJSON = validCreds
		require.NoError(t, cfg.Validate())

		opt, err := cfg.CredentialsClientOption()
		require.NoError(t, err)
		require.Equal(t, option.WithCredentialsJSON([]byte(validCreds)), opt)
	})
}
