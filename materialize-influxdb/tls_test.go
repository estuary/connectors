package connector

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClientTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"influxdb.test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	ca := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	serverTLS := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}

	for _, tc := range []struct {
		name     string
		advanced advancedConfig
		trusted  bool
	}{
		{"public CAs", advancedConfig{}, false},
		{"custom CA", advancedConfig{SSLCACertificate: ca}, true},
		{"skip verification", advancedConfig{SSLSkipVerify: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config{
				URL: "https://influxdb.test", Org: "estuary", Bucket: "sensors",
				Credentials: credentialsConfig{AccessToken: "made-up-token"}, Advanced: tc.advanced,
			}
			require.NoError(t, cfg.Validate())
			c, err := cfg.client(nil)
			require.NoError(t, err)
			defer c.httpClient.CloseIdleConnections()
			c.retry.MaxAttempts = 1
			transport := c.httpClient.Transport.(*http.Transport)
			transport.Proxy = nil
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				clientConn, serverConn := net.Pipe()
				go func() {
					conn := tls.Server(serverConn, serverTLS)
					defer conn.Close()
					conn.SetDeadline(time.Now().Add(5 * time.Second))
					req, err := http.ReadRequest(bufio.NewReader(conn))
					if err != nil {
						return
					}
					defer req.Body.Close()
					io.Copy(io.Discard, req.Body)
					response := &http.Response{StatusCode: http.StatusNoContent, ProtoMajor: 1, ProtoMinor: 1, Close: true}
					if req.Header.Get("Authorization") != "Token made-up-token" {
						response.StatusCode = http.StatusUnauthorized
					} else if req.URL.Path == "/api/v2/buckets" {
						body := `{"buckets":[{"name":"sensors"}]}`
						response.StatusCode = http.StatusOK
						response.Body = io.NopCloser(strings.NewReader(body))
						response.ContentLength = int64(len(body))
					}
					response.Write(conn)
				}()
				return clientConn, nil
			}
			for _, err := range []error{
				c.checkBucket(t.Context()),
				c.post(t.Context(), "write", "text/plain", "readings value=1i 0", nil),
				c.deletePoints(t.Context(), deleteRequest{}),
			} {
				if tc.trusted {
					require.NoError(t, err)
				} else {
					var verification *tls.CertificateVerificationError
					require.ErrorAs(t, err, &verification)
				}
			}
			require.Equal(t, tc.advanced.SSLSkipVerify, transport.TLSClientConfig.InsecureSkipVerify)
		})
	}
}

func TestClientTransportIsolation(t *testing.T) {
	cfg, err := resolveEndpointConfig(testEndpoint)
	require.NoError(t, err)
	verified, err := cfg.client(nil)
	require.NoError(t, err)
	defer verified.httpClient.CloseIdleConnections()
	cfg.Advanced.SSLSkipVerify = true
	unverified, err := cfg.client(nil)
	require.NoError(t, err)
	defer unverified.httpClient.CloseIdleConnections()
	first := verified.httpClient.Transport.(*http.Transport)
	second := unverified.httpClient.Transport.(*http.Transport)
	require.NotSame(t, first, second)
	require.NotSame(t, first, http.DefaultTransport)
	require.False(t, first.TLSClientConfig.InsecureSkipVerify)
	require.True(t, second.TLSClientConfig.InsecureSkipVerify)
}
