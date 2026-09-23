package mysqltls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

func newTestCA(t *testing.T, name string) *testCA {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	var tmpl = &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return &testCA{cert: cert, key: key, pem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

// issueWith produces a server certificate for the named host, signed by the CA,
// with the certificate template adjusted by edit before signing.
func (ca *testCA) issueWith(t *testing.T, host string, edit func(*x509.Certificate)) tls.Certificate {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	var tmpl = &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: host},
		DNSNames:     []string{host},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	edit(tmpl)
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// issue is issueWith, leaving the certificate template unchanged.
func (ca *testCA) issue(t *testing.T, host string) tls.Certificate {
	return ca.issueWith(t, host, func(*x509.Certificate) {})
}

// withChain appends the CA's certificate to the chain cert presents, as MySQL
// does with its auto-generated CA.
func (ca *testCA) withChain(cert tls.Certificate) tls.Certificate {
	cert.Certificate = append(cert.Certificate, ca.cert.Raw)
	return cert
}

// serve runs a TLS listener presenting cert and returns its address.
func serve(t *testing.T, cert tls.Certificate) string {
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				conn.(*tls.Conn).Handshake()
				conn.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

func dial(t *testing.T, settings Settings, serverHost, addr string) error {
	cfg, err := settings.Config(serverHost)
	require.NoError(t, err)
	require.NotNil(t, cfg)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
	if err == nil {
		conn.Close()
	}
	return err
}

func TestVerification(t *testing.T) {
	var realCA = newTestCA(t, "real-ca")
	var attackerCA = newTestCA(t, "attacker-ca")
	const host = "db.example.com"

	var genuine = serve(t, realCA.issue(t, host))
	var impostor = serve(t, attackerCA.issue(t, host))
	var wrongName = serve(t, realCA.issue(t, "other.example.com"))

	t.Run("required accepts anything", func(t *testing.T) {
		var s = Settings{Mode: ModeRequired}
		require.NoError(t, dial(t, s, host, genuine))
		require.NoError(t, dial(t, s, host, impostor))
	})
	t.Run("verify_ca checks the chain but not the name", func(t *testing.T) {
		var s = Settings{Mode: ModeVerifyCA, ServerCA: realCA.pem}
		require.NoError(t, dial(t, s, host, genuine))
		require.NoError(t, dial(t, s, host, wrongName))
		require.Error(t, dial(t, s, host, impostor))
	})
	t.Run("verify_identity checks chain and name", func(t *testing.T) {
		var s = Settings{Mode: ModeVerifyIdentity, ServerCA: realCA.pem}
		require.NoError(t, dial(t, s, host, genuine))
		require.Error(t, dial(t, s, host, wrongName))
		require.Error(t, dial(t, s, host, impostor))
	})
	t.Run("verify_identity without a CA uses the default roots", func(t *testing.T) {
		var s = Settings{Mode: ModeVerifyIdentity}
		require.Error(t, dial(t, s, host, genuine))
	})
}

// defaultRoots skips any bundled certificate that fails to parse, so this is
// what guarantees that every one of them is trusted.
func TestDefaultRootsTrustCloudCAs(t *testing.T) {
	var pool = defaultRoots()
	names, err := fs.Glob(cloudCAs, "cloudcas/*.pem")
	require.NoError(t, err)
	require.NotEmpty(t, names)

	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			var rest, err = cloudCAs.ReadFile(name)
			require.NoError(t, err)
			var count int
			for {
				var block *pem.Block
				if block, rest = pem.Decode(rest); block == nil {
					break
				}
				cert, err := x509.ParseCertificate(block.Bytes)
				require.NoError(t, err)
				// Every bundled certificate is itself a trust anchor in the pool.
				_, err = cert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}})
				require.NoError(t, err, "%s: %s", name, cert.Subject)
				count++
			}
			require.NotZero(t, count)
		})
	}
}

func TestVerificationRemediation(t *testing.T) {
	var realCA = newTestCA(t, "real-ca")
	var attackerCA = newTestCA(t, "attacker-ca")
	const host = "db.example.com"

	var genuine = serve(t, realCA.issue(t, host))
	var impostor = serve(t, attackerCA.issue(t, host))
	var wrongName = serve(t, realCA.issue(t, "other.example.com"))
	// Like MySQL's auto-generated certificates: a Common Name but no SANs.
	var noNames = serve(t, realCA.issueWith(t, host, func(c *x509.Certificate) { c.DNSNames = nil }))
	var expired = serve(t, realCA.issueWith(t, host, func(c *x509.Certificate) {
		c.NotBefore, c.NotAfter = time.Now().Add(-2*time.Hour), time.Now().Add(-time.Hour)
	}))

	for _, tc := range []struct {
		name     string
		settings Settings
		addr     string
		expect   string // Substring of the remediation, or "" when there should be none.
	}{
		{"public roots reject a private CA", Settings{Mode: ModeVerifyIdentity}, genuine, "not signed by a public certificate authority, nor by Amazon RDS"},
		{"configured CA rejects an impostor", Settings{Mode: ModeVerifyIdentity, ServerCA: realCA.pem}, impostor, "not signed by the configured 'ssl_server_ca'"},
		{"verify_ca rejects an impostor", Settings{Mode: ModeVerifyCA, ServerCA: realCA.pem}, impostor, "not signed by the configured 'ssl_server_ca'"},
		{"wrong name suggests verify_ca", Settings{Mode: ModeVerifyIdentity, ServerCA: realCA.pem}, wrongName, `is issued for ["other.example.com"]`},
		{"no names suggests verify_ca", Settings{Mode: ModeVerifyIdentity, ServerCA: realCA.pem}, noNames, "does not name any host"},
		{"names are checked before the chain", Settings{Mode: ModeVerifyIdentity}, noNames, "'sslmode' to \"verify_ca\""},
		{"expired", Settings{Mode: ModeVerifyIdentity, ServerCA: realCA.pem}, expired, "certificate has expired"},
		{"success needs no remediation", Settings{Mode: ModeVerifyIdentity, ServerCA: realCA.pem}, genuine, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err = dial(t, tc.settings, host, tc.addr)
			var remediation = tc.settings.verificationRemediation(err, host)
			if tc.expect == "" {
				require.NoError(t, err)
				require.Empty(t, remediation)
			} else {
				require.Error(t, err)
				require.Contains(t, remediation, tc.expect)
			}
		})
	}

	t.Run("offers the presented CA", func(t *testing.T) {
		var autoGenerated = serve(t, realCA.withChain(realCA.issueWith(t, host, func(c *x509.Certificate) { c.DNSNames = nil })))
		var presentsGenuine = serve(t, realCA.withChain(realCA.issue(t, host)))
		var presentsUnrelated = serve(t, attackerCA.withChain(realCA.issue(t, host)))

		for _, tc := range []struct {
			name     string
			settings Settings
			addr     string
			offered  string // PEM expected in the remediation, or "" for none.
		}{
			{"auto-generated", Settings{}, autoGenerated, realCA.pem},
			{"auto-generated with its CA configured", Settings{ServerCA: realCA.pem}, autoGenerated, realCA.pem},
			{"private CA", Settings{Mode: ModeVerifyIdentity}, presentsGenuine, realCA.pem},
			{"CA not presented", Settings{}, noNames, ""},
			{"presented CA did not sign", Settings{Mode: ModeVerifyIdentity}, presentsUnrelated, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var remediation = tc.settings.verificationRemediation(dial(t, tc.settings, host, tc.addr), host)
				require.NotEmpty(t, remediation)
				if tc.offered == "" {
					require.NotContains(t, remediation, "BEGIN CERTIFICATE")
				} else {
					require.Contains(t, remediation, strings.TrimSpace(tc.offered))
					require.Contains(t, remediation, "confirm that it matches your server's CA certificate")
				}
			})
		}
	})

	t.Run("other failures need no remediation", func(t *testing.T) {
		require.Empty(t, Settings{Mode: ModeVerifyIdentity}.verificationRemediation(errors.New("connection refused"), host))
	})
}

func TestFailureMessage(t *testing.T) {
	const host = "10.0.0.5"
	var hostErr = fmt.Errorf("writeAuthHandshake: %w", &tls.CertificateVerificationError{
		Err: x509.HostnameError{Certificate: &x509.Certificate{}, Host: host},
	})
	// As go-mysql reports it, which the connectors' tests pin.
	var noTLSErr = errors.New("readInitialHandshake: the MySQL Server does not support TLS required by the client")

	t.Run("CertificateRejected", func(t *testing.T) {
		var msg = Settings{}.FailureMessage(hostErr, host, true)
		require.Contains(t, msg, hostErr.Error())
		require.Contains(t, msg, `'sslmode' is unset and defaults to "verify_identity", which verifies the server certificate.`)
		require.Contains(t, msg, "does not name any host")
		require.Contains(t, msg, `set 'sslmode' to "verify_ca"`)
		require.Contains(t, msg, `set 'sslmode' to "required"`)
		require.NotContains(t, msg, "unencrypted")
	})
	t.Run("ExplicitMode", func(t *testing.T) {
		var msg = Settings{Mode: ModeRequired}.FailureMessage(noTLSErr, host, true)
		require.Contains(t, msg, `'sslmode' is "required", which does not permit falling back`)
	})
	t.Run("TLSUnavailable", func(t *testing.T) {
		var msg = Settings{}.FailureMessage(noTLSErr, host, true)
		require.Contains(t, msg, noTLSErr.Error())
		require.Contains(t, msg, "does not permit falling back to an unencrypted connection")
		require.Contains(t, msg, `set 'sslmode' to "preferred"`)
	})
	t.Run("TLSUnavailableWithoutPlaintext", func(t *testing.T) {
		var msg = Settings{}.FailureMessage(noTLSErr, host, false)
		require.Contains(t, msg, "The server must be configured to accept TLS.")
		require.NotContains(t, msg, `"preferred"`)
	})
	// No 'sslmode' fixes these, so they mustn't be blamed on TLS.
	t.Run("UnrelatedToTLS", func(t *testing.T) {
		for _, err := range []error{
			errors.New("dial tcp 10.0.0.5:3306: i/o timeout"),
			errors.New("readInitialHandshake: io.ReadFull(header) failed. err EOF"),
			errors.New("ERROR 1049 (42000): Unknown database 'flow'"),
		} {
			require.Empty(t, Settings{}.FailureMessage(err, host, true), err.Error())
		}
	})
}

func TestValidate(t *testing.T) {
	var ca = newTestCA(t, "ca")
	require.NoError(t, Settings{Mode: ModePreferred}.Validate())
	require.NoError(t, Settings{Mode: ModeVerifyIdentity}.Validate())
	require.NoError(t, Settings{Mode: ModeVerifyCA, ServerCA: ca.pem}.Validate())
	require.NoError(t, Settings{}.Validate()) // Unset selects DefaultMode.
	require.Error(t, Settings{Mode: "VERIFY_CA"}.Validate())
	require.Error(t, Settings{Mode: ModeVerifyCA}.Validate())
	require.Error(t, Settings{Mode: ModeVerifyCA, ServerCA: "not a pem"}.Validate())
	require.Error(t, Settings{Mode: ModeRequired, ClientCert: "x"}.Validate())

	// A CA is meaningless outside the verifying modes, and is rejected rather than
	// ignored so that setting one without changing 'sslmode' can't quietly leave
	// the connection unverified.
	for _, mode := range []string{ModeDisabled, ModePreferred, ModeRequired} {
		require.ErrorContains(t, Settings{Mode: mode, ServerCA: ca.pem}.Validate(), "does not verify the server certificate")
	}
	require.NoError(t, Settings{Mode: ModeVerifyIdentity, ServerCA: ca.pem}.Validate())
	// Client certificates are usable in every mode which negotiates TLS at all.
	require.ErrorContains(t, Settings{Mode: ModeDisabled, ClientCert: "x", ClientKey: "y"}.Validate(), "never uses TLS")

	cfg, err := Settings{Mode: ModeDisabled}.Config("host")
	require.NoError(t, err)
	require.Nil(t, cfg)
	require.True(t, Settings{Mode: ModePreferred}.AllowsPlaintextFallback())
	require.False(t, Settings{Mode: ModeRequired}.AllowsPlaintextFallback())
	require.False(t, Settings{Mode: ModeDisabled}.AllowsPlaintextFallback())
}

// Pinned per mode because callers rely on this to decide whether a bearer token
// may be presented: 'disabled' never encrypts and 'preferred' may silently stop
// encrypting, so only the three strict modes qualify.
func TestGuaranteesEncryption(t *testing.T) {
	for _, tc := range []struct {
		mode   string
		expect bool
	}{
		{ModeDisabled, false},
		{ModePreferred, false},
		{ModeRequired, true},
		{ModeVerifyCA, true},
		{ModeVerifyIdentity, true},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			require.Equal(t, tc.expect, Settings{Mode: tc.mode}.GuaranteesEncryption())
		})
	}
	// An unset mode is judged by DefaultMode, and a bogus one must not read as a guarantee.
	require.True(t, Settings{}.GuaranteesEncryption())
	require.False(t, Settings{Mode: "REQUIRED"}.GuaranteesEncryption())
}
