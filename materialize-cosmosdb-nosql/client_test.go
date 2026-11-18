package connector

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient(t *testing.T) {
	for _, endpoint := range []string{"https://cosmos.test:443/", "http://cosmos.test:8081/"} {
		t.Run(endpoint, func(t *testing.T) {
			cfg := testConfig()
			cfg.Endpoint = endpoint
			client, err := cfg.client(nil)
			require.NoError(t, err)
			defer client.Close()
			require.Equal(t, endpoint, client.Endpoint())
		})
	}

	t.Run("invalid key", func(t *testing.T) {
		cfg := testConfig()
		cfg.Key = "not-a-base64-key"
		client, err := cfg.client(nil)
		require.Nil(t, client)
		require.ErrorContains(t, err, "creating account key credential")
		require.NotContains(t, err.Error(), cfg.Key)
	})
}
