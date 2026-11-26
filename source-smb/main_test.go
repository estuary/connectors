package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/filesource"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const testConfig = `{"host":"smb.example","share":"data","username":"test","password":"test","directory":"/inbox","matchFiles":".*\\.csv$"}`

func TestSpec(t *testing.T) {
	parserSpec, err := os.ReadFile("tests/parser_spec.json")
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(configSchema(json.RawMessage(parserSpec)), "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))

	var spec struct {
		Properties map[string]json.RawMessage `json:"properties"`
		Required   []string                   `json:"required"`
	}
	require.NoError(t, json.Unmarshal(formatted, &spec))
	require.ElementsMatch(t, []string{"host", "share", "username", "password"}, spec.Required)
	require.JSONEq(t, string(parserSpec), string(spec.Properties["parser"]))

	var password struct {
		Type   string `json:"type"`
		Secret bool   `json:"secret"`
	}
	require.NoError(t, json.Unmarshal(spec.Properties["password"], &password))
	require.Equal(t, "string", password.Type)
	require.True(t, password.Secret)

	var port struct {
		Type    string `json:"type"`
		Default int    `json:"default"`
	}
	require.NoError(t, json.Unmarshal(spec.Properties["port"], &port))
	require.Equal(t, "integer", port.Type)
	require.Equal(t, 445, port.Default)

	var directory struct {
		Type    string `json:"type"`
		Default string `json:"default"`
	}
	require.NoError(t, json.Unmarshal(spec.Properties["directory"], &directory))
	require.Equal(t, "string", directory.Type)
	require.Equal(t, "/", directory.Default)

	var tunnel struct {
		Properties map[string]struct {
			Required   []string `json:"required"`
			Properties map[string]struct {
				Type      string `json:"type"`
				Secret    bool   `json:"secret"`
				Multiline bool   `json:"multiline"`
			} `json:"properties"`
		} `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(spec.Properties["networkTunnel"], &tunnel))
	var forwarding = tunnel.Properties["sshForwarding"]
	require.ElementsMatch(t, []string{"sshEndpoint", "privateKey"}, forwarding.Required)
	require.Equal(t, "string", forwarding.Properties["sshEndpoint"].Type)
	require.Equal(t, "string", forwarding.Properties["privateKey"].Type)
	require.True(t, forwarding.Properties["privateKey"].Secret)
	require.True(t, forwarding.Properties["privateKey"].Multiline)
}

func TestConfig(t *testing.T) {
	var driver = newDriver()
	cfg, err := driver.NewConfig(json.RawMessage(testConfig))
	require.NoError(t, err)
	require.Equal(t, 445, cfg.(config).Port)
	require.Equal(t, "/inbox", cfg.DiscoverRoot())
	require.Equal(t, "inbox", cfg.RecommendedName())
	require.Equal(t, `.*\.csv$`, cfg.PathRegex())
	require.False(t, cfg.FilesAreMonotonic())
	require.Nil(t, cfg.ParserConfig())
	require.Nil(t, cfg.(config).NetworkTunnel)

	for _, field := range []string{"host", "share", "username", "password"} {
		t.Run("missing "+field, func(t *testing.T) {
			var properties map[string]any
			require.NoError(t, json.Unmarshal([]byte(testConfig), &properties))
			delete(properties, field)
			raw, err := json.Marshal(properties)
			require.NoError(t, err)
			_, err = driver.NewConfig(raw)
			require.ErrorContains(t, err, "missing '"+field+"'")
		})
	}

	for _, tt := range []struct {
		name  string
		field string
		value any
	}{
		{"NUL directory", "directory", "/inbox\x00"},
		{"invalid regex", "matchFiles", "["},
		{"zero port", "port", 0},
		{"negative port", "port", -1},
		{"large port", "port", 65536},
		{"fractional port", "port", 445.5},
		{"string port", "port", "445"},
		{"share path", "share", "data/inbox"},
		{"UNC share", "share", `\\server\data`},
		{"parent share", "share", ".."},
		{"unknown field", "address", "smb.example:445"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var properties map[string]any
			require.NoError(t, json.Unmarshal([]byte(testConfig), &properties))
			properties[tt.field] = tt.value
			raw, err := json.Marshal(properties)
			require.NoError(t, err)
			_, err = driver.NewConfig(raw)
			require.Error(t, err)
		})
	}

	for _, tt := range []struct {
		name      string
		directory any
		root      string
	}{
		{"omitted directory", nil, "/"},
		{"empty directory", "", "/"},
		{"share root", "/", "/"},
		{"backslash root", `\`, "/"},
		{"relative directory", "inbox", "/inbox"},
		{"unclean directory", "/inbox/../other", "/other"},
		{"trailing separator", "/inbox/", "/inbox"},
		{"backslash directory", `/inbox\sub`, "/inbox/sub"},
		{"leading backslash", `\inbox\sub\`, "/inbox/sub"},
		{"relative backslashes", `inbox\sub`, "/inbox/sub"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var properties map[string]any
			require.NoError(t, json.Unmarshal([]byte(testConfig), &properties))
			delete(properties, "directory")
			if tt.directory != nil {
				properties["directory"] = tt.directory
			}
			raw, err := json.Marshal(properties)
			require.NoError(t, err)
			cfg, err := driver.NewConfig(raw)
			require.NoError(t, err)
			require.Equal(t, tt.root, cfg.(config).Directory)
			require.Equal(t, tt.root, cfg.DiscoverRoot())
			require.NoError(t, cfg.Validate())
		})
	}

	t.Run("optional settings", func(t *testing.T) {
		cfg, err := driver.NewConfig(json.RawMessage(`{
			"host":"2001:db8::1", "port":1445, "share":"data", "username":"test", "password":"test",
			"domain":"EXAMPLE", "directory":"/", "parser":{"format":{"type":"csv"}},
			"advanced":{"ascendingKeys":true}
		}`))
		require.NoError(t, err)
		require.Equal(t, 1445, cfg.(config).Port)
		require.Equal(t, "EXAMPLE", cfg.(config).Domain)
		require.Equal(t, "root", cfg.RecommendedName())
		require.True(t, cfg.FilesAreMonotonic())
		require.Equal(t, "csv", cfg.ParserConfig().Format["type"])
	})
}

func TestDiscover(t *testing.T) {
	for _, tt := range []struct {
		label     string
		directory string
		stream    string
		name      string
	}{
		{"/inbox", "/inbox", "/inbox", "inbox"},
		{"/inbox/sub", "/inbox/sub", "/inbox/sub", "inbox/sub"},
		{"/", "/", "/", "root"},
		{"omitted directory", "", "/", "root"},
		{"relative directory", "inbox", "/inbox", "inbox"},
		{"backslash directory", `\inbox\sub\`, "/inbox/sub", "inbox/sub"},
	} {
		t.Run(tt.label, func(t *testing.T) {
			var driver = newDriver()
			var store = &testStore{}
			cfg, err := driver.NewConfig(json.RawMessage(testConfig))
			require.NoError(t, err)
			var endpoint = cfg.(config)
			endpoint.Directory = tt.directory
			raw, err := json.Marshal(endpoint)
			require.NoError(t, err)
			endpoint.Directory = tt.stream
			driver.Connect = func(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
				require.Equal(t, endpoint, cfg)
				return store, nil
			}

			resp, err := driver.Discover(context.Background(), &pc.Request_Discover{ConfigJson: raw})
			require.NoError(t, err)
			require.Len(t, resp.Bindings, 1)
			require.Equal(t, 1, store.closed)
			var binding = resp.Bindings[0]
			require.Equal(t, tt.name, binding.RecommendedName)
			resource, err := json.Marshal(map[string]string{"stream": tt.stream})
			require.NoError(t, err)
			require.JSONEq(t, string(resource), string(binding.ResourceConfigJson))
			require.Equal(t, []string{"/_meta/file", "/_meta/offset"}, binding.Key)

			formatted, err := json.MarshalIndent(resp, "", "  ")
			require.NoError(t, err)
			cupaloy.SnapshotT(t, string(formatted))
		})
	}
}

func TestDiscoverConnectionError(t *testing.T) {
	var driver = newDriver()
	var connectErr = errors.New("share unavailable")
	driver.Connect = func(context.Context, filesource.Config) (filesource.Store, error) {
		return nil, connectErr
	}
	_, err := driver.Discover(context.Background(), &pc.Request_Discover{ConfigJson: json.RawMessage(testConfig)})
	require.ErrorIs(t, err, connectErr)
}

func TestValidateClosesStore(t *testing.T) {
	for _, tt := range []struct {
		resource string
		wantErr  bool
	}{
		{`{"stream":"/inbox"}`, false},
		{`{}`, true},
	} {
		t.Run(tt.resource, func(t *testing.T) {
			var driver = newDriver()
			var store = &testStore{}
			driver.Connect = func(context.Context, filesource.Config) (filesource.Store, error) {
				return store, nil
			}
			resp, err := driver.Validate(context.Background(), &pc.Request_Validate{
				ConfigJson: json.RawMessage(testConfig),
				Bindings: []*pc.Request_Validate_Binding{{
					ResourceConfigJson: json.RawMessage(tt.resource),
				}},
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, resp.Bindings, 1)
				require.Equal(t, []string{"/inbox"}, resp.Bindings[0].ResourcePath)
			}
			require.Equal(t, 1, store.closed)
		})
	}
}

func TestPullClosesStore(t *testing.T) {
	for _, tt := range []struct {
		name     string
		state    json.RawMessage
		bindings []*pf.CaptureSpec_Binding
		wantErr  string
	}{
		{name: "no bindings"},
		{name: "invalid state", state: json.RawMessage(`{"unexpected":true}`), wantErr: "unmarshalling state"},
		{
			name: "listing error",
			bindings: []*pf.CaptureSpec_Binding{{
				ResourceConfigJson: json.RawMessage(`{"stream":"/inbox"}`),
			}},
			wantErr: "unexpected List call",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var driver = newDriver()
			var store = &testStore{}
			driver.Connect = func(context.Context, filesource.Config) (filesource.Store, error) {
				return store, nil
			}
			err := driver.Pull(&pc.Request_Open{
				Capture:   &pf.CaptureSpec{ConfigJson: json.RawMessage(testConfig), Bindings: tt.bindings},
				StateJson: tt.state,
			}, &boilerplate.PullOutput{Connector_CaptureServer: testStream{}})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, store.closed)
		})
	}
}

type testStream struct {
	pc.Connector_CaptureServer
}

func (testStream) Context() context.Context { return context.Background() }
func (testStream) Send(*pc.Response) error  { return nil }

type testStore struct {
	closed int
}

func (*testStore) List(context.Context, filesource.Query) (filesource.Listing, error) {
	return nil, errors.New("unexpected List call")
}

func (*testStore) Read(context.Context, filesource.ObjectInfo) (io.ReadCloser, filesource.ObjectInfo, error) {
	return nil, filesource.ObjectInfo{}, errors.New("unexpected Read call")
}

func (s *testStore) Close() error {
	s.closed++
	return nil
}

func TestIntegration(t *testing.T) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	// These credentials are for the local Samba fixture.
	var cfg = config{Host: "127.0.0.1", Port: 1445, Share: "data", Username: "test", Password: "test", Directory: "/inbox"}
	t.Run("discover", func(t *testing.T) {
		raw, err := json.Marshal(cfg)
		require.NoError(t, err)
		resp, err := newDriver().Discover(context.Background(), &pc.Request_Discover{ConfigJson: raw})
		require.NoError(t, err)
		require.Len(t, resp.Bindings, 1)
		require.JSONEq(t, `{"stream":"/inbox"}`, string(resp.Bindings[0].ResourceConfigJson))
		require.Equal(t, "inbox", resp.Bindings[0].RecommendedName)
		require.Equal(t, []string{"/_meta/file", "/_meta/offset"}, resp.Bindings[0].Key)
	})
	t.Run("share root", func(t *testing.T) {
		for _, directory := range []string{"/", ""} {
			var root = cfg
			root.Directory = directory
			raw, err := json.Marshal(root)
			require.NoError(t, err)
			resp, err := newDriver().Discover(context.Background(), &pc.Request_Discover{ConfigJson: raw})
			require.NoError(t, err)
			require.Len(t, resp.Bindings, 1)
			require.JSONEq(t, `{"stream":"/"}`, string(resp.Bindings[0].ResourceConfigJson))
			require.Equal(t, "root", resp.Bindings[0].RecommendedName)
		}
	})
	t.Run("reconnect and close", func(t *testing.T) {
		var ctx = context.Background()
		var previousDial = smbDialContext
		t.Cleanup(func() { smbDialContext = previousDial })
		var transport net.Conn
		var dials int
		smbDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			dials++
			var err error
			transport, err = (&net.Dialer{}).DialContext(ctx, network, address)
			return transport, err
		}
		source, err := newSmbSource(ctx, cfg, "127.0.0.1:1445")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, source.Close()) })

		var original = transport
		require.NoError(t, original.Close())
		info, err := (smbDirectory{source: source, ctx: ctx}).Stat(cfg.Directory)
		require.NoError(t, err)
		require.True(t, info.IsDir())
		require.NotSame(t, original, transport)
		require.Equal(t, 2, dials)
		require.NoError(t, source.Close())
		require.NoError(t, source.Close())
	})
}
