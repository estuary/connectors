package connector

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpecification(t *testing.T) {
	var resp, err = (driver{}).
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestRedactedAddress(t *testing.T) {
	require.Equal(t, "mongodb+srv://user:xxxxx@my-mongo.test/?authSource=admin", redactedAddress("mongodb+srv://user:secret@my-mongo.test/?authSource=admin"))
	require.Equal(t, "mongodb://my-mongo.test:27017", redactedAddress("mongodb://my-mongo.test:27017"))
	require.Equal(t, "<unparseable address>", redactedAddress("mongodb://user:secret@my-mongo.test:bad"))
}

func TestUnmarshalConfig(t *testing.T) {
	for _, tt := range []struct {
		name    string
		address string
		want    string
	}{
		{name: "srv with login", address: "mongodb+srv://fake-user:fake-password@my-mongo.test/?authSource=admin&w=majority", want: "mongodb+srv://my-mongo.test/?authSource=admin&w=majority"},
		{name: "hosts with login", address: "mongodb://fake-user:fake%40password@h1:27017,h2:27017/?replicaSet=rs0", want: "mongodb://h1:27017,h2:27017/?replicaSet=rs0"},
		{name: "no login", address: "mongodb://my-mongo.test:27017/?authSource=admin", want: "mongodb://my-mongo.test:27017/?authSource=admin"},
		{name: "no scheme", address: "fake-user:fake-password@my-mongo.test:27017", want: "my-mongo.test:27017"},
		{name: "unparseable", address: "mongodb://fake-user:fake-password@my-mongo.test:bad", want: "mongodb://my-mongo.test:bad"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(map[string]string{"user": "user", "password": "password", "database": "db", "address": tt.address})
			require.NoError(t, err)

			var cfg config
			require.NoError(t, boilerplate.UnmarshalStrict(raw, &cfg))
			require.Equal(t, tt.want, cfg.Address)
		})
	}

	t.Run("unknown field", func(t *testing.T) {
		var cfg config
		require.ErrorContains(t, boilerplate.UnmarshalStrict([]byte(`{"user": "user", "password": "password", "database": "db", "address": "mongodb://my-mongo.test", "bogus": 1}`), &cfg), `unknown field "bogus"`)
	})
}
