package pgtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
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
	return &testCA{cert: cert, key: key}
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

// trustOnly makes the verifying modes trust only ca for the rest of the test.
func trustOnly(t *testing.T, ca *testCA) {
	var pool = x509.NewCertPool()
	pool.AddCert(ca.cert)
	var prev = defaultRoots
	defaultRoots = func() *x509.CertPool { return pool }
	t.Cleanup(func() { defaultRoots = prev })
}

// parse builds the pgx config for a server at host, as the connectors do.
func parse(t *testing.T, host, sslmode string) *pgconn.Config {
	cfg, err := pgconn.ParseConfig("postgres://user@" + host + ":5432/db?sslmode=" + sslmode)
	require.NoError(t, err)
	return cfg
}

// handshake runs a TLS handshake with the server at addr, using the TLS
// config of a copy of cfg. pgxpool and pgx.ConnectConfig copy the config
// before every connection, so this is the config a real connection uses.
func handshake(t *testing.T, cfg *pgconn.Config, addr string) error {
	var tlsConfig = cfg.Copy().TLSConfig
	require.NotNil(t, tlsConfig)
	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, tlsConfig)
	if err == nil {
		conn.Close()
	}
	return err
}

func TestVerification(t *testing.T) {
	var realCA = newTestCA(t, "real-ca")
	var attackerCA = newTestCA(t, "attacker-ca")
	trustOnly(t, realCA)
	const host = "db.example.com"

	var genuine = serve(t, realCA.issue(t, host))
	var impostor = serve(t, attackerCA.issue(t, host))
	var wrongName = serve(t, realCA.issue(t, "other.example.com"))

	for _, mode := range []string{"verify-ca", "verify-full"} {
		t.Run(mode+" needs the default roots", func(t *testing.T) {
			// Without Configure, pgx trusts only the system root store.
			require.Error(t, handshake(t, parse(t, host, mode), genuine))
		})
	}
	t.Run("verify-ca checks the chain but not the name", func(t *testing.T) {
		var cfg = parse(t, host, "verify-ca")
		Configure(cfg, "verify-ca")
		require.NoError(t, handshake(t, cfg, genuine))
		require.NoError(t, handshake(t, cfg, wrongName))
		require.Error(t, handshake(t, cfg, impostor))
	})
	t.Run("verify-full checks the chain and the name", func(t *testing.T) {
		var cfg = parse(t, host, "verify-full")
		Configure(cfg, "verify-full")
		require.NoError(t, handshake(t, cfg, genuine))
		require.Error(t, handshake(t, cfg, wrongName))
		require.Error(t, handshake(t, cfg, impostor))
	})
}

func TestConfigure(t *testing.T) {
	var ca = newTestCA(t, "ca")
	trustOnly(t, ca)

	for _, mode := range []string{"verify-ca", "verify-full"} {
		t.Run(mode, func(t *testing.T) {
			var cfg = parse(t, "db.example.com", mode)
			Configure(cfg, mode)
			require.Same(t, defaultRoots(), cfg.TLSConfig.RootCAs)
		})
	}
	t.Run("multiple hosts", func(t *testing.T) {
		// Each host has its own fallback config, and each must be changed.
		var cfg = parse(t, "a.example.com:5432,b.example.com", "verify-full")
		require.Len(t, cfg.Fallbacks, 1)
		Configure(cfg, "verify-full")
		require.Same(t, defaultRoots(), cfg.TLSConfig.RootCAs)
		require.Same(t, defaultRoots(), cfg.Fallbacks[0].TLSConfig.RootCAs)
	})
	for _, mode := range []string{"disable", "allow", "prefer", "require"} {
		t.Run(mode+" is unchanged", func(t *testing.T) {
			var cfg = parse(t, "db.example.com", mode)
			var want = parse(t, "db.example.com", mode)
			Configure(cfg, mode)
			for i, fb := range append([]*pgconn.FallbackConfig{{TLSConfig: cfg.TLSConfig}}, cfg.Fallbacks...) {
				if fb.TLSConfig == nil {
					continue
				}
				require.Nil(t, fb.TLSConfig.RootCAs, "config %d", i)
				var wantTLS = want.TLSConfig
				if i > 0 {
					wantTLS = want.Fallbacks[i-1].TLSConfig
				}
				require.Equal(t, wantTLS.ServerName, fb.TLSConfig.ServerName, "config %d", i)
				require.Equal(t, wantTLS.InsecureSkipVerify, fb.TLSConfig.InsecureSkipVerify, "config %d", i)
			}
		})
	}
}
