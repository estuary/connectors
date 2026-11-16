package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestDiscover(t *testing.T) {
	d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		require.Equal(t, http.MethodGet, req.Method)
		require.Equal(t, "http://rabbitmq.test:15672/api/queues/%2F", req.URL.String())
		require.Equal(t, "application/json", req.Header.Get("Accept"))
		username, password, ok := req.BasicAuth()
		require.True(t, ok)
		require.Equal(t, "test", username)
		require.Equal(t, "test", password)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`[
				{"name":"orders","type":"classic","arguments":{"x-queue-type":"classic"},"messages":3},
				{"name":"amq.gen-exclusive","exclusive":true},
				{"name":"amq.gen-autodelete","auto_delete":true},
				{"name":"amq.gen-both","exclusive":true,"auto_delete":true},
				{"name":"events","arguments":{"x-queue-type":"stream"}},
				{"name":"quorum","type":"quorum","arguments":{"x-queue-type":"quorum"}},
				{"name":"named-transient","auto_delete":true},
				{"name":"stream-default","type":"stream"},
				{"name":"amq.gen-persistent","exclusive":false,"auto_delete":false}
			]`)),
		}, nil
	})}}

	response, err := d.Discover(context.Background(), &pc.Request_Discover{ConfigJson: json.RawMessage(validConfigJSON)})
	require.NoError(t, err)
	require.NoError(t, response.Validate())
	var names []string
	queueTypes := map[string]string{
		"amq.gen-persistent": "classic", "events": "stream", "named-transient": "classic",
		"orders": "classic", "quorum": "quorum", "stream-default": "stream",
	}
	for _, binding := range response.Bindings {
		names = append(names, binding.RecommendedName)
		var res map[string]string
		require.NoError(t, json.Unmarshal(binding.ResourceConfigJson, &res))
		require.Equal(t, map[string]string{"queue": binding.RecommendedName, "queueType": queueTypes[binding.RecommendedName]}, res)
		require.Equal(t, []string{"/_meta/id"}, binding.Key)
	}
	require.Equal(t, []string{"amq.gen-persistent", "events", "named-transient", "orders", "quorum", "stream-default"}, names)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestDiscoverEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  config
		want string
	}{
		{
			name: "escaped virtual host",
			cfg:  config{Host: "rabbitmq.test", Port: 5673, ManagementPort: 15673, VirtualHost: "team/events %?#", Username: "test", Password: "p@ss:/?%"},
			want: "http://rabbitmq.test:15673/api/queues/team%2Fevents%20%25%3F%23",
		},
		{
			name: "TLS and IPv6",
			cfg:  config{Host: "::1", TLS: true, ManagementPort: 15671, Username: "test", Password: "test"},
			want: "https://[::1]:15671/api/queues/%2F",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, tc.want, req.URL.String())
				username, password, ok := req.BasicAuth()
				require.True(t, ok)
				require.Equal(t, tc.cfg.Username, username)
				require.Equal(t, tc.cfg.Password, password)
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`[]`))}, nil
			})}}
			configJSON, err := json.Marshal(tc.cfg)
			require.NoError(t, err)
			response, err := d.Discover(context.Background(), &pc.Request_Discover{ConfigJson: configJSON})
			require.NoError(t, err)
			require.Empty(t, response.Bindings)
		})
	}
}

func TestDiscoverUnsupportedQueueTypes(t *testing.T) {
	for _, fields := range []string{
		`"type":"rabbit_mqtt_qos0_queue"`,
		`"arguments":{"x-queue-type":"rabbit_mqtt_qos0_queue"}`,
		`"type":"rabbit_mqtt_qos0_queue","arguments":{"x-queue-type":"stream"}`,
	} {
		t.Run(fields, func(t *testing.T) {
			d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
					`[{"name":"unsupported",` + fields + `},{"name":"orders","type":"classic"}]`,
				))}, nil
			})}}
			response, err := d.Discover(context.Background(), &pc.Request_Discover{ConfigJson: json.RawMessage(validConfigJSON)})
			require.NoError(t, err)
			require.Len(t, response.Bindings, 1)
			require.Equal(t, "orders", response.Bindings[0].RecommendedName)
			var res resource
			require.NoError(t, pf.UnmarshalStrict(response.Bindings[0].ResourceConfigJson, &res))
		})
	}
}

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

func TestDiscoverErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"unauthorized", 401, `{"reason":"access denied"}`, "HTTP 401"},
		{"forbidden", 403, `{"reason":"not authorized"}`, "HTTP 403"},
		{"missing virtual host", 404, `{"reason":"not found"}`, "HTTP 404"},
		{"server failure", 500, `server error`, "HTTP 500"},
		{"redirect", 302, ``, "HTTP 302"},
		{"invalid JSON", 200, `[`, "decoding management API queues"},
		{"unexpected shape", 200, `{"items":[]}`, "decoding management API queues"},
		{"missing queue name", 200, `[{"type":"classic"}]`, "queue without a name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &trackedBody{Reader: strings.NewReader(tc.body)}
			d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.status, Body: body}, nil
			})}}
			_, err := d.Discover(context.Background(), &pc.Request_Discover{ConfigJson: json.RawMessage(validConfigJSON)})
			require.ErrorContains(t, err, tc.want)
			require.True(t, body.closed)
		})
	}
}

func TestDiscoverTransportError(t *testing.T) {
	failure := errors.New("connection failed")
	d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, failure
	})}}
	_, err := d.Discover(context.Background(), &pc.Request_Discover{ConfigJson: json.RawMessage(validConfigJSON)})
	require.ErrorIs(t, err, failure)
}

func TestDiscoverCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		require.ErrorIs(t, req.Context().Err(), context.Canceled)
		return nil, req.Context().Err()
	})}}
	_, err := d.Discover(ctx, &pc.Request_Discover{ConfigJson: json.RawMessage(validConfigJSON)})
	require.ErrorIs(t, err, context.Canceled)
}

func TestDiscoverInvalidConfig(t *testing.T) {
	d := driver{httpClient: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("invalid configuration must be rejected before discovery")
		return nil, nil
	})}}
	_, err := d.Discover(context.Background(), &pc.Request_Discover{ConfigJson: json.RawMessage(`{"host":"rabbitmq.test"}`)})
	require.ErrorContains(t, err, "missing 'username'")
}
