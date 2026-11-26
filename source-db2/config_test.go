package main

import (
	"encoding/json"
	"testing"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, pf.UnmarshalStrict(json.RawMessage(`{"address":"db2.example","password":"test-password","database":"TESTDB"}`), &cfg))
	cfg.SetDefaults()
	require.Equal(t, "db2.example:50000", cfg.Address)
	require.Equal(t, "flow_capture", cfg.User)
	require.False(t, cfg.HistoryMode)
	require.Equal(t, "ASN", cfg.Advanced.CaptureSchema)
	require.Equal(t, "FLOW_CAPTURE.FLOW_WATERMARKS", cfg.Advanced.WatermarksTable)
	require.Equal(t, 50000, cfg.Advanced.BackfillChunkSize)
	require.Equal(t, "1s", cfg.Advanced.PollingInterval)
	require.Equal(t, "UTC", cfg.Advanced.Timezone)
	require.NoError(t, cfg.Validate())
	var first = cfg
	cfg.SetDefaults()
	require.Equal(t, first, cfg)
}

func TestConfig(t *testing.T) {
	var cfg Config
	require.NoError(t, pf.UnmarshalStrict(json.RawMessage(`{
		"address":"db2-test:50000", "user":"db2inst1", "password":"flowtest1", "database":"TESTDB",
		"historyMode":true,
		"advanced":{
			"capture_schema":"REPL", "watermarksTable":"DB2INST1.FLOW_WATERMARKS", "polling_interval":"2s",
			"backfill_chunk_size":100, "discover_schemas":["DB2INST1"], "timezone":"America/Los_Angeles",
			"source_tag":"fixture", "feature_flags":"no_use_schema_inference"
		}
	}`), &cfg))
	cfg.SetDefaults()
	require.Equal(t, "db2-test:50000", cfg.Address)
	require.True(t, cfg.HistoryMode)
	require.Equal(t, "REPL", cfg.Advanced.CaptureSchema)
	require.Equal(t, "DB2INST1.FLOW_WATERMARKS", cfg.Advanced.WatermarksTable)
	require.Equal(t, "2s", cfg.Advanced.PollingInterval)
	require.Equal(t, 100, cfg.Advanced.BackfillChunkSize)
	require.Equal(t, []string{"DB2INST1"}, cfg.Advanced.DiscoverSchemas)
	require.Equal(t, "America/Los_Angeles", cfg.Advanced.Timezone)
	require.Equal(t, "fixture", cfg.Advanced.SourceTag)
}

func TestConfigValidation(t *testing.T) {
	var cases = []struct {
		name   string
		change func(*Config)
		error  string
	}{
		{"MissingAddress", func(c *Config) { c.Address = "" }, "missing 'address'"},
		{"MissingPassword", func(c *Config) { c.Password = "" }, "missing 'password'"},
		{"MissingDatabase", func(c *Config) { c.Database = "" }, "missing 'database'"},
		{"SemicolonUser", func(c *Config) { c.User = "user;UID=other" }, "Db2 connection strings cannot carry ';' in the user"},
		{"SemicolonPassword", func(c *Config) { c.Password = "p;w1" }, "Db2 connection strings cannot carry ';' in the password; change the database user's password"},
		{"SemicolonDatabase", func(c *Config) { c.Database = "TESTDB;DATABASE=other" }, "Db2 connection strings cannot carry ';' in the database"},
		{"LeadingPasswordSpace", func(c *Config) { c.Password = " password" }, "Db2 connection strings cannot carry leading or trailing whitespace in the password; change the database user's password"},
		{"TrailingPasswordSpace", func(c *Config) { c.Password = "password " }, "Db2 connection strings cannot carry leading or trailing whitespace in the password; change the database user's password"},
		{"PasswordWhitespace", func(c *Config) { c.Password = "\tpassword\n" }, "leading or trailing whitespace in the password"},
		{"EmptyHost", func(c *Config) { c.Address = ":50000" }, "invalid 'address'"},
		{"BadPort", func(c *Config) { c.Address = "db2:abc" }, "invalid 'address'"},
		{"LargePort", func(c *Config) { c.Address = "db2:65536" }, "invalid 'address'"},
		{"BadPollingInterval", func(c *Config) { c.Advanced.PollingInterval = "tomorrow" }, "invalid 'polling_interval'"},
		{"ZeroPollingInterval", func(c *Config) { c.Advanced.PollingInterval = "0s" }, "must be greater than zero"},
		{"NegativePollingInterval", func(c *Config) { c.Advanced.PollingInterval = "-1s" }, "must be greater than zero"},
		{"NegativeChunkSize", func(c *Config) { c.Advanced.BackfillChunkSize = -1 }, "invalid 'backfill_chunk_size'"},
		{"BadTimezone", func(c *Config) { c.Advanced.Timezone = "Not/A_Zone" }, "invalid 'timezone'"},
		{"UnqualifiedWatermarks", func(c *Config) { c.Advanced.WatermarksTable = "FLOW_WATERMARKS" }, "must be fully-qualified"},
		{"EmptySchema", func(c *Config) { c.Advanced.WatermarksTable = ".FLOW_WATERMARKS" }, "must be fully-qualified"},
		{"EmptyTable", func(c *Config) { c.Advanced.WatermarksTable = "DB2INST1." }, "must be fully-qualified"},
		{"ThreePartTable", func(c *Config) { c.Advanced.WatermarksTable = "TESTDB.DB2INST1.FLOW_WATERMARKS" }, "must be fully-qualified"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg = Config{Address: "db2", User: "flow_capture", Password: "test-password", Database: "TESTDB"}
			tc.change(&cfg)
			require.ErrorContains(t, cfg.Validate(), tc.error)
		})
	}
	t.Run("UnknownOption", func(t *testing.T) {
		var cfg Config
		require.ErrorContains(t, pf.UnmarshalStrict(json.RawMessage(`{"address":"db2","password":"test-password","database":"TESTDB","advanced":{"poll":"1s"}}`), &cfg), `unknown field "poll"`)
	})
}

func TestConfigURI(t *testing.T) {
	for _, tc := range []struct {
		address string
		host    string
		port    string
	}{
		{"db2.example", "db2.example", "50000"},
		{"db2.example:50100", "db2.example", "50100"},
		{"[::1]", "::1", "50000"},
		{"[::1]:50100", "::1", "50100"},
	} {
		t.Run(tc.address, func(t *testing.T) {
			var cfg = Config{Address: tc.address, User: "db2inst1", Password: "test-password", Database: "TESTDB"}
			require.NoError(t, cfg.Validate())
			cfg.SetDefaults()
			require.Equal(t, "HOSTNAME="+tc.host+";PORT="+tc.port+";DATABASE=TESTDB;UID=db2inst1;PWD=test-password", cfg.ToURI())
		})
	}
	t.Run("Tunnel", func(t *testing.T) {
		var cfg = Config{Address: "private-db2:50100", User: "flow_capture", Password: "test-password", Database: "TESTDB"}
		require.NoError(t, json.Unmarshal([]byte(`{"sshForwarding":{"sshEndpoint":"ssh://test@bastion.example","privateKey":"test-key"}}`), &cfg.NetworkTunnel))
		require.IsType(t, &networkTunnel.TunnelConfig{}, cfg.NetworkTunnel)
		require.Equal(t, "HOSTNAME=localhost;PORT=50000;DATABASE=TESTDB;UID=flow_capture;PWD=test-password", cfg.ToURI())
	})
}

func TestQuoteIdentifier(t *testing.T) {
	require.Equal(t, `"Mixed Schema"."a""b"`, quoteTableName("Mixed Schema", `a"b`))
}

func TestConfigCredentialsUnchanged(t *testing.T) {
	for _, value := range []string{"p}w1", "p{w1", "p=w1", "{abc}", "a b", "plain"} {
		t.Run(value, func(t *testing.T) {
			var cfg = Config{Address: "db2.example", Database: value, User: value, Password: value}
			require.NoError(t, cfg.Validate())
			require.Equal(t, "HOSTNAME=db2.example;PORT=50000;DATABASE="+value+";UID="+value+";PWD="+value, cfg.ToURI())
		})
	}
}
