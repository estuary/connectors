package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const validConfigJSON = `{"host":"rabbitmq.test","username":"test","password":"test"}`

func TestConfigDefaults(t *testing.T) {
	var cfg config
	require.NoError(t, pf.UnmarshalStrict(json.RawMessage(validConfigJSON), &cfg))
	cfg.SetDefaults()
	require.Equal(t, 5672, cfg.Port)
	require.Equal(t, 15672, cfg.ManagementPort)
	require.Equal(t, "/", cfg.VirtualHost)
	require.False(t, cfg.TLS)
	require.Equal(t, "PLAIN", cfg.AuthMechanism)
	require.Equal(t, 10, cfg.PrefetchCount)
	require.Equal(t, tlsConfig{}, cfg.TLSConfig)

	custom := config{
		Host:           "rabbitmq.test",
		Port:           5671,
		ManagementPort: 15671,
		VirtualHost:    "tenant/events",
		Username:       "test",
		Password:       "test",
		TLS:            true,
		AuthMechanism:  "PLAIN",
		PrefetchCount:  500,
	}
	cfg = custom
	cfg.SetDefaults()
	require.Equal(t, custom, cfg)
	require.NoError(t, cfg.Validate())
}

func TestConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*config)
		want   string
	}{
		{"missing host", func(c *config) { c.Host = "" }, "missing 'host'"},
		{"missing username", func(c *config) { c.Username = "" }, "missing 'username'"},
		{"missing password", func(c *config) { c.Password = "" }, "missing 'password'"},
		{"negative port", func(c *config) { c.Port = -1 }, "invalid 'port'"},
		{"large port", func(c *config) { c.Port = 65536 }, "invalid 'port'"},
		{"negative management port", func(c *config) { c.ManagementPort = -1 }, "invalid 'managementPort'"},
		{"large management port", func(c *config) { c.ManagementPort = 65536 }, "invalid 'managementPort'"},
		{"negative prefetch", func(c *config) { c.PrefetchCount = -1 }, "invalid 'prefetchCount'"},
		{"large prefetch", func(c *config) { c.PrefetchCount = 65536 }, "invalid 'prefetchCount'"},
		{"unknown mechanism", func(c *config) { c.AuthMechanism = "LOGIN" }, "invalid 'authMechanism'"},
		{"EXTERNAL without TLS", func(c *config) { c.AuthMechanism = "EXTERNAL" }, "EXTERNAL authentication requires"},
		{"EXTERNAL without certificate", func(c *config) { c.AuthMechanism = "EXTERNAL"; c.TLS = true }, "EXTERNAL authentication requires"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg config
			require.NoError(t, json.Unmarshal([]byte(validConfigJSON), &cfg))
			tc.modify(&cfg)
			require.ErrorContains(t, cfg.Validate(), tc.want)
		})
	}
}

func TestConfigUnmarshal(t *testing.T) {
	for _, input := range []string{
		`{"host":"rabbitmq.test","username":"test","password":"test","port":5672.5}`,
		`{"host":"rabbitmq.test","username":"test","password":"test","tls":"true"}`,
		`{"host":"rabbitmq.test","username":"test","password":"test","virtual_host":"/"}`,
		`{"host":"rabbitmq.test","username":"test","password":"test","prefetchCount":1.5}`,
	} {
		var cfg config
		require.Error(t, pf.UnmarshalStrict(json.RawMessage(input), &cfg))
	}
}

func TestResourceStreamStartOffset(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  any
	}{
		{"", "first"}, {"first", "first"}, {"last", "last"}, {"next", "next"},
		{"7D", "7D"}, {"12h", "12h"}, {"1Y", "1Y"}, {"2M", "2M"}, {"30m", "30m"}, {"5s", "5s"},
		{"2026-11-11T08:30:00-08:00", time.Date(2026, 11, 11, 16, 30, 0, 0, time.UTC)},
		{"2026-11-11T16:30:00.123Z", time.Date(2026, 11, 11, 16, 30, 0, 0, time.UTC)},
		{"1970-01-01T00:00:00Z", time.Unix(0, 0).UTC()},
	} {
		t.Run(tc.value, func(t *testing.T) {
			var res resource
			require.NoError(t, pf.UnmarshalStrict(mustMarshal(t, resource{Queue: "events", QueueType: "stream", StreamStartOffset: tc.value}), &res))
			offset, err := res.initialStreamOffset()
			require.NoError(t, err)
			require.Equal(t, tc.want, offset)
		})
	}
	for _, value := range []string{"0", "42", "latest", "0h", "-1D", "1d", "1w", "1.5h", "1h30m", " 1h", "2026-11-11", "1969-12-31T23:59:59Z", "999999999999999999999999D"} {
		require.ErrorContains(t, (&resource{Queue: "events", StreamStartOffset: value}).Validate(), "invalid 'streamStartOffset'")
	}
	for _, queueType := range []string{"classic", "quorum"} {
		require.NoError(t, (&resource{Queue: "orders", QueueType: queueType, StreamStartOffset: "first"}).Validate())
	}
}

func TestResourceValidate(t *testing.T) {
	for _, name := range []string{"orders", "events", "queue with spaces", "tenant/orders"} {
		require.NoError(t, (&resource{Queue: name}).Validate())
	}
	require.ErrorContains(t, (&resource{}).Validate(), "missing 'queue'")

	var res resource
	for _, queueType := range []string{"classic", "quorum", "stream"} {
		require.NoError(t, pf.UnmarshalStrict(mustMarshal(t, resource{Queue: "orders", QueueType: queueType}), &res))
		require.Equal(t, queueType, res.QueueType)
	}
	require.ErrorContains(t, pf.UnmarshalStrict(json.RawMessage(`{"queue":"orders","queueType":"unknown"}`), &res), "invalid 'queueType'")
	require.Error(t, pf.UnmarshalStrict(json.RawMessage(`{"queue":"orders","queue_type":"stream"}`), &res))
}

func TestValidateInvalidConfig(t *testing.T) {
	_, err := driver{}.Validate(context.Background(), &pc.Request_Validate{
		ConfigJson: json.RawMessage(`{"host":"rabbitmq.test"}`),
	})
	require.ErrorContains(t, err, "parsing endpoint config")

	// Invalid bindings are rejected before opening an AMQP connection.
	_, err = driver{}.Validate(context.Background(), &pc.Request_Validate{
		ConfigJson: json.RawMessage(validConfigJSON),
		Bindings: []*pc.Request_Validate_Binding{
			{ResourceConfigJson: json.RawMessage(`{}`)},
		},
	})
	require.ErrorContains(t, err, "missing 'queue'")
}
