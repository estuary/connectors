package connector

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	t.Parallel()

	response, err := Driver().Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)
	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestEndpointConfig(t *testing.T) {
	t.Parallel()
	cfg, err := resolveEndpointConfig(json.RawMessage(`{"host":"broker.example","username":"test","password":"made-up-password"}`))
	require.NoError(t, err)
	require.Equal(t, config{
		Host: "broker.example", Port: 5672, VirtualHost: "/", Username: "test", Password: "made-up-password", TLS: false,
	}, cfg)

	cfg, err = resolveEndpointConfig(json.RawMessage(`{"host":"broker.example","port":5674,"virtualHost":"factory","username":"test","password":"made-up-password","tls":true}`))
	require.NoError(t, err)
	require.Equal(t, 5674, cfg.Port)
	require.Equal(t, "factory", cfg.VirtualHost)
	require.True(t, cfg.TLS)

	for _, tc := range []struct {
		name  string
		field string
		value any
		err   string
	}{
		{"missing host", "host", "", "missing 'host'"},
		{"missing username", "username", "", "missing 'username'"},
		{"missing password", "password", "", "missing 'password'"},
		{"empty virtual host", "virtualHost", "", "missing 'virtualHost'"},
		{"zero port", "port", 0, "'port' must be between"},
		{"negative port", "port", -1, "'port' must be between"},
		{"large port", "port", 65536, "'port' must be between"},
		{"fractional port", "port", 5672.5, "cannot unmarshal"},
		{"invalid TLS", "tls", "true", "cannot unmarshal"},
		{"unknown field", "hardDelete", true, "unknown field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := map[string]any{"host": "broker.example", "username": "test", "password": "made-up-password"}
			raw[tc.field] = tc.value
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = resolveEndpointConfig(encoded)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestEndpointAddress(t *testing.T) {
	t.Parallel()
	for _, host := range []string{"broker.example", "127.0.0.1", "::1"} {
		for _, vhost := range []string{"/", "factory", "a/b", "a b?c#d%"} {
			for _, tls := range []bool{false, true} {
				cfg := config{Host: host, Port: 5674, VirtualHost: vhost, TLS: tls, Password: "made-up-password"}
				address := cfg.address()
				u, err := amqp.ParseURI(address)
				require.NoError(t, err)
				require.Equal(t, host, u.Host)
				require.Equal(t, 5674, u.Port)
				require.Equal(t, vhost, u.Vhost)
				require.Equal(t, tls, u.Scheme == "amqps")
				require.NotContains(t, address, cfg.Password)
			}
		}
	}
}

func TestResourceConfig(t *testing.T) {
	t.Parallel()
	res, err := resolveResourceConfig(json.RawMessage(`{"exchange":"factory.orders"}`))
	require.NoError(t, err)
	require.Equal(t, resource{Exchange: "factory.orders", ExchangeType: "topic"}, res)

	for _, tc := range []struct {
		name  string
		field string
		value any
		err   string
	}{
		{"default exchange", "exchange", "", "missing 'exchange'"},
		{"long exchange", "exchange", strings.Repeat("x", 256), "at most 255 bytes"},
		{"unsupported type", "exchangeType", "headers", "must be direct, fanout, or topic"},
		{"empty type", "exchangeType", "", "must be direct, fanout, or topic"},
		{"long routing key", "routingKey", strings.Repeat("x", 256), "at most 255 bytes"},
		{"invalid pointer", "routingKeyField", "status", "must start with '/'"},
		{"invalid escape", "routingKeyField", "/a~2b", "invalid '~' escape"},
		{"trailing escape", "routingKeyField", "/a~", "invalid '~' escape"},
		{"unknown field", "hardDelete", true, "unknown field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := map[string]any{"exchange": "factory.orders"}
			raw[tc.field] = tc.value
			encoded, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = resolveResourceConfig(encoded)
			require.ErrorContains(t, err, tc.err)
		})
	}
	for _, exchangeType := range []string{"direct", "fanout", "topic"} {
		res.ExchangeType = exchangeType
		require.NoError(t, res.Validate())
	}
}

func TestBindingConstraints(t *testing.T) {
	t.Parallel()
	collection := pf.CollectionSpec{
		Name: "tests/orders",
		Projections: []pf.Projection{
			{Field: "flow_document", Ptr: ""},
			{Field: "id", Ptr: "/id", IsPrimaryKey: true},
			{Field: "status_alias", Ptr: "/status"},
		},
	}
	res := resource{Exchange: "factory.orders", RoutingKey: "orders", RoutingKeyField: "/status"}
	binding, err := res.validateBinding(collection)
	require.NoError(t, err)
	require.True(t, binding.DeltaUpdates)
	require.False(t, binding.CaseInsensitiveFields)
	require.Equal(t, []string{"factory.orders", "orders"}, binding.ResourcePath)
	require.Len(t, binding.ProjectionConstraints, 3)
	require.Equal(t, pm.Response_Validated_Constraint_LOCATION_REQUIRED, binding.ProjectionConstraints[0].Constraint.Type)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_OPTIONAL, binding.ProjectionConstraints[1].Constraint.Type)
	require.Equal(t, pm.Response_Validated_Constraint_FIELD_FORBIDDEN, binding.ProjectionConstraints[2].Constraint.Type)

	res.RoutingKey = ""
	binding, err = res.validateBinding(collection)
	require.NoError(t, err)
	require.Equal(t, []string{"factory.orders"}, binding.ResourcePath)

	res.RoutingKeyField = "/notProjected"
	_, err = res.validateBinding(collection)
	require.ErrorContains(t, err, "must be a projection")
	res.RoutingKeyField = "/status_alias"
	_, err = res.validateBinding(collection)
	require.ErrorContains(t, err, "must be a projection")
}

func TestExchangeTypeConflict(t *testing.T) {
	t.Parallel()
	seen := make(map[string]string)
	require.NoError(t, checkExchangeType(seen, resource{Exchange: "cdc", ExchangeType: "fanout", RoutingKey: "a"}))
	require.NoError(t, checkExchangeType(seen, resource{Exchange: "cdc", ExchangeType: "fanout", RoutingKey: "b"}))
	require.NoError(t, checkExchangeType(seen, resource{Exchange: "audit", ExchangeType: "direct"}))
	require.ErrorContains(t,
		checkExchangeType(seen, resource{Exchange: "cdc", ExchangeType: "topic", RoutingKey: "c"}),
		`bindings for exchange "cdc" set different exchangeType values (fanout, topic)`)
}
