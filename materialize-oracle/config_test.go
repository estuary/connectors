package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

func TestSpecification(t *testing.T) {
	var resp, err = NewDriver().Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestTLSConfig(t *testing.T) {
	var server = httptest.NewTLSServer(nil)
	defer server.Close()
	var state = tls.ConnectionState{PeerCertificates: []*x509.Certificate{server.Certificate()}}

	require.Nil(t, config{SSLMode: sslDisable}.tlsConfig())
	require.True(t, config{SSLMode: sslRequire}.tlsConfig().InsecureSkipVerify)
	require.Nil(t, config{SSLMode: sslRequire}.tlsConfig().VerifyConnection)

	// An unset SSL mode verifies like verify-full, with the built-in check.
	for _, mode := range []sslMode{"", sslVerifyFull} {
		var direct = config{SSLMode: mode, Address: "example.com:2484"}.tlsConfig()
		require.False(t, direct.InsecureSkipVerify)
		require.Nil(t, direct.VerifyConnection)
	}

	// The driver applies that check: the test server's certificate isn't
	// trusted.
	var cfg = exampleConfig()
	cfg.Address = server.Listener.Addr().String()
	var db = cfg.openDB()
	defer db.Close()
	require.ErrorContains(t, db.PingContext(context.Background()), "certificate signed by unknown authority")

	// Through a tunnel the check is our own. The test server's certificate is
	// self-signed for example.com, so it's rejected for its authority.
	var tunneled = config{
		Address:       "example.com:2484",
		NetworkTunnel: &tunnelConfig{SshForwarding: &sshForwarding{SshEndpoint: "ssh://user@bastion"}},
	}.tlsConfig()
	require.True(t, tunneled.InsecureSkipVerify)
	var verify = tunneled.VerifyConnection
	require.ErrorContains(t, verify(state), "certificate signed by unknown authority")
	require.Error(t, verify(tls.ConnectionState{}))
}

func exampleConfig() config {
	return config{
		Address:     "example.com:2484",
		User:        "will",
		Credentials: credentialsConfig{AuthType: UserPassword, userPasswordConfig: userPasswordConfig{Password: "secret1234"}},
		Database:    "ORCLPDB1",
	}
}

func TestConfigValidate(t *testing.T) {
	// SSL mode is optional.
	require.NoError(t, exampleConfig().Validate())

	var cfg = exampleConfig()
	cfg.Credentials.Password = ""
	require.ErrorContains(t, cfg.Validate(), "missing 'password'")

	cfg = exampleConfig()
	cfg.SSLMode = "prefer"
	require.ErrorContains(t, cfg.Validate(), "invalid 'sslmode'")

	// The driver fails to parse these addresses with an error that quotes the
	// whole connection string, so they must not reach it.
	for _, address := range []string{"example.com:prod", "will@example.com", "example.com/db", "example .com:1521"} {
		cfg = exampleConfig()
		cfg.Address = address
		_, driverErr := go_ora.ParseConfig(cfg.toURI())
		require.ErrorContains(t, driverErr, "secret1234", address)

		var err = cfg.Validate()
		require.ErrorContains(t, err, "invalid 'address'", address)
		require.NotContains(t, err.Error(), "secret1234", address)
	}
}

func TestConfigURI(t *testing.T) {
	var uri = func(edit func(*config)) string {
		var cfg = exampleConfig()
		edit(&cfg)
		return cfg.toURI()
	}

	// What a TLS connection verifies is up to tlsConfig, so every mode but
	// disable builds the same URI, an unset mode included.
	for _, mode := range []sslMode{"", sslVerifyFull, sslRequire} {
		require.Equal(t,
			"oracle://will:secret1234@example.com:2484/ORCLPDB1?PROGRAM=Estuary+Flow+Materialization&SSL=true",
			uri(func(c *config) { c.SSLMode = mode }), "mode %q", mode)
	}
	require.Equal(t,
		"oracle://will:secret1234@example.com:2484/ORCLPDB1?PROGRAM=Estuary+Flow+Materialization",
		uri(func(c *config) { c.SSLMode = sslDisable }))

	// An address without a port gets 1521, whatever the SSL mode.
	require.Equal(t,
		"oracle://will:secret1234@example.com:1521/ORCLPDB1?PROGRAM=Estuary+Flow+Materialization&SSL=true",
		uri(func(c *config) { c.Address = "example.com" }))

	// Through an SSH tunnel the connection goes to the tunnel's local end.
	require.Equal(t,
		"oracle://will:secret1234@localhost:1521/ORCLPDB1?PROGRAM=Estuary+Flow+Materialization&SSL=true",
		uri(func(c *config) {
			c.NetworkTunnel = &tunnelConfig{SshForwarding: &sshForwarding{SshEndpoint: "ssh://user@bastion"}}
		}))

	// Reserved characters in the credentials reach the driver intact.
	parsed, err := go_ora.ParseConfig(uri(func(c *config) { c.User, c.Credentials.Password = "wi:ll", "p@ss/w:rd?#" }))
	require.NoError(t, err)
	require.Equal(t, "wi:ll", parsed.UserID)
	require.Equal(t, "p@ss/w:rd?#", parsed.Password)
}
