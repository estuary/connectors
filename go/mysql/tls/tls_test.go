package mysqltls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
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

// issue produces a server certificate for the named host, signed by the CA.
func (ca *testCA) issue(t *testing.T, host string) tls.Certificate {
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
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
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
	t.Run("verify_identity without a CA uses system roots", func(t *testing.T) {
		var s = Settings{Mode: ModeVerifyIdentity}
		require.Error(t, dial(t, s, host, genuine))
	})
}

func TestValidate(t *testing.T) {
	var ca = newTestCA(t, "ca")
	require.NoError(t, Settings{Mode: ModePreferred}.Validate())
	require.NoError(t, Settings{Mode: ModeVerifyIdentity}.Validate())
	require.NoError(t, Settings{Mode: ModeVerifyCA, ServerCA: ca.pem}.Validate())
	require.Error(t, Settings{Mode: ""}.Validate())
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
	// An unset or bogus mode must not read as a guarantee.
	require.False(t, Settings{}.GuaranteesEncryption())
	require.False(t, Settings{Mode: "REQUIRED"}.GuaranteesEncryption())
}
