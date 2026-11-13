package connector

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
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

	// The test server's certificate is self-signed for example.com, so it's
	// rejected for its authority, and for a host name it doesn't cover.
	var verify = config{SSLMode: sslVerifyFull, Address: "example.com:2484"}.tlsConfig().VerifyConnection
	require.ErrorContains(t, verify(state), "certificate signed by unknown authority")
	require.Error(t, verify(tls.ConnectionState{}))
}

func TestConfigURI(t *testing.T) {
	var base = config{
		Address:     "example.com",
		User:        "will",
		Credentials: credentialsConfig{AuthType: UserPassword, userPasswordConfig: userPasswordConfig{Password: "secret1234"}},
		Database:    "ORCLPDB1",
		SSLMode:     sslVerifyFull,
	}
	for name, edit := range map[string]func(*config){
		"VerifyFull": func(*config) {},
		"Require":    func(c *config) { c.SSLMode = sslRequire; c.Address = "example.com:2484" },
		"Disable":    func(c *config) { c.SSLMode = sslDisable },
		"NoSSLMode":  func(c *config) { c.SSLMode = "" },
		"NoPassword": func(c *config) { c.Credentials.Password = "" },
		"Tunnel": func(c *config) {
			c.NetworkTunnel = &tunnelConfig{SshForwarding: &sshForwarding{SshEndpoint: "ssh://user@bastion"}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			var cfg = base
			edit(&cfg)
			var valid = "config valid"
			if err := cfg.Validate(); err != nil {
				valid = err.Error()
			}
			cupaloy.SnapshotT(t, fmt.Sprintf("%s\n%s", cfg.toURI(), valid))
		})
	}
}
