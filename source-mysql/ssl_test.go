package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"strings"
	"testing"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	mysqltls "github.com/estuary/connectors/go/mysql/tls"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/stretchr/testify/require"
)

// presentedChain returns the certificates the test database presents during
// the TLS handshake, leaf first, skipping the test if it doesn't offer TLS.
func presentedChain(t *testing.T) []*x509.Certificate {
	t.Helper()
	var chain []*x509.Certificate
	var conn, err = client.Connect(*dbCaptureAddress, *dbCaptureUser, *dbCapturePass, *dbName, func(c *client.Conn) error {
		c.SetTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
			VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
				for _, raw := range rawCerts {
					cert, err := x509.ParseCertificate(raw)
					if err != nil {
						return err
					}
					chain = append(chain, cert)
				}
				return nil
			},
		})
		return nil
	})
	if err != nil {
		t.Skipf("test database does not accept TLS connections: %v", err)
	}
	conn.Close()
	require.NotEmpty(t, chain)
	return chain
}

// TestSSLModes tests the 'sslmode' ladder against the test database. It
// targets MySQL's auto-generated certificates, which are signed by a private
// CA the server also presents, and which name no host.
func TestSSLModes(t *testing.T) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var chain = presentedChain(t)
	var leaf, root = chain[0], chain[len(chain)-1]
	if len(leaf.DNSNames) > 0 || len(leaf.IPAddresses) > 0 {
		t.Skipf("server certificate names %v %v, but this test expects one that names no host", leaf.DNSNames, leaf.IPAddresses)
	}
	if len(chain) < 2 || !root.IsCA || root.CheckSignatureFrom(root) != nil {
		t.Skip("server does not present the self-signed CA that issued its certificate")
	}
	var serverCA = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: root.Raw}))

	var dial = func(mode, ca string) error {
		var cfg = Config{
			Address:  *dbCaptureAddress,
			User:     *dbCaptureUser,
			Password: *dbCapturePass,
			Advanced: advancedConfig{DBName: *dbName, SSLMode: mode, SSLServerCA: ca},
		}
		require.NoError(t, cfg.Validate())
		var conn, err = (&mysqlDatabase{config: &cfg}).dial(cfg.Address, cfg.Password)
		if err == nil {
			conn.Close()
		}
		return err
	}
	var requireUserError = func(t *testing.T, err error, contains ...string) {
		t.Helper()
		require.Error(t, err)
		var userErr *cerrors.UserError
		require.ErrorAs(t, err, &userErr, "certificate failures must be reported to the user")
		for _, s := range contains {
			require.ErrorContains(t, err, s)
		}
	}

	t.Run("Unset", func(t *testing.T) {
		requireUserError(t, dial("", ""),
			`'sslmode' is unset and defaults to "verify_identity"`,
			"does not name any host",
			`'sslmode' to "verify_ca"`,
			strings.TrimSpace(serverCA))
	})
	t.Run("UnsetWithPresentedCA", func(t *testing.T) {
		// Pinning the CA doesn't help: the certificate still names no host.
		requireUserError(t, dial("", serverCA), "does not name any host")
	})
	t.Run("VerifyCAWithPresentedCA", func(t *testing.T) {
		require.NoError(t, dial(mysqltls.ModeVerifyCA, serverCA))
	})
	t.Run("VerifyCAWithOtherCA", func(t *testing.T) {
		requireUserError(t, dial(mysqltls.ModeVerifyCA, testCAPEM(t)), "not signed by the configured 'ssl_server_ca'")
	})
	t.Run("Required", func(t *testing.T) {
		require.NoError(t, dial(mysqltls.ModeRequired, ""))
	})
}
