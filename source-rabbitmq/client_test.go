package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"testing"
	"time"

	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestConnectionError(t *testing.T) {
	plainHint := "the server did not speak plain AMQP on [::1]:5671; it may require TLS (enable TLS, or use port 5672 for non-TLS): "
	tlsHint := "the server did not speak TLS on [::1]:5672; it may require plain AMQP (disable TLS, or use port 5671 for TLS): "
	for _, tc := range []struct {
		name string
		tls  bool
		err  error
		hint string
	}{
		{"frame too large", false, amqp.ErrFrameTooLarge, plainHint},
		{"nested frame error", false, &amqp.Error{Code: amqp.FrameError, Reason: amqp.ErrFrameTooLarge.Error()}, plainHint},
		{"wrapped frame error", false, fmt.Errorf("handshake: %w", amqp.ErrFrame), plainHint},
		{"syntax error", false, amqp.ErrSyntax, plainHint},
		{"unexpected frame", false, amqp.ErrUnexpectedFrame, plainHint},
		{"unexpected command", false, amqp.ErrCommandInvalid, plainHint},
		{"EOF", false, io.EOF, plainHint},
		{"unexpected EOF", false, fmt.Errorf("handshake: %w", io.ErrUnexpectedEOF), plainHint},
		{"AMQP EOF", false, &amqp.Error{Code: amqp.FrameError, Reason: "EOF"}, plainHint},
		{"AMQP unexpected EOF", false, &amqp.Error{Code: amqp.FrameError, Reason: "unexpected EOF"}, plainHint},
		{"unexpected bytes", false, &amqp.Error{Code: amqp.FrameError, Reason: "unexpected bytes at connect"}, plainHint},
		{"TLS against plain AMQP", true, tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}, tlsHint},
		{"wrapped TLS mismatch", true, fmt.Errorf("handshake: %w", tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}), tlsHint},
		{"authentication", false, amqp.ErrCredentials, ""},
		{"vhost", false, amqp.ErrVhost, ""},
		{"DNS", false, &net.DNSError{Err: "no such host", Name: "rabbitmq.invalid"}, ""},
		{"refused", false, errors.New("dial tcp: connection refused"), ""},
		{"timeout", false, context.DeadlineExceeded, ""},
		{"AMQP timeout", false, &amqp.Error{Code: amqp.FrameError, Reason: "read tcp: i/o timeout"}, ""},
		{"AMQP reset", false, &amqp.Error{Code: amqp.FrameError, Reason: "read tcp: connection reset by peer"}, ""},
		{"canceled", false, context.Canceled, ""},
		{"TLS certificate", true, errors.New("x509: certificate signed by unknown authority"), ""},
		{"TLS frame error", true, amqp.ErrFrameTooLarge, ""},
		{"TLS EOF", true, io.EOF, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config{Host: "::1", Port: 5671, TLS: tc.tls, Username: "test-user", Password: "test-password"}
			if tc.tls {
				cfg.Port = 5672
			}
			err := connectionError(&cfg, tc.err)
			require.EqualError(t, err, "connecting to RabbitMQ: "+tc.hint+tc.err.Error())
			require.ErrorIs(t, err, tc.err)
			require.NotContains(t, err.Error(), cfg.Password)
			require.NotContains(t, err.Error(), cfg.Username)
		})
	}
}

type testCertificate struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM string
	keyPEM  string
	pair    tls.Certificate
}

func newTestCertificate(t *testing.T, parent *testCertificate, name string, usage x509.ExtKeyUsage) testCertificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	require.NoError(t, err)
	cert := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: name}, DNSNames: []string{name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
		BasicConstraintsValid: true,
	}
	signer, issuer := key, cert
	if parent == nil {
		cert.IsCA = true
		cert.KeyUsage |= x509.KeyUsageCertSign
		cert.ExtKeyUsage = nil
	} else {
		signer, issuer = parent.key, parent.cert
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, issuer, &key.PublicKey, signer)
	require.NoError(t, err)
	cert, err = x509.ParseCertificate(der)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	pair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
	require.NoError(t, err)
	return testCertificate{cert: cert, key: key, certPEM: certPEM, keyPEM: keyPEM, pair: pair}
}

func testTLSConfig(t *testing.T) (config, *tls.Config) {
	t.Helper()
	ca := newTestCertificate(t, nil, "test-ca", 0)
	server := newTestCertificate(t, &ca, "rabbitmq.test", x509.ExtKeyUsageServerAuth)
	client := newTestCertificate(t, &ca, "estuary", x509.ExtKeyUsageClientAuth)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM([]byte(ca.certPEM)))
	return config{
		Host: "rabbitmq.test", TLS: true, Username: "test", Password: "test",
		TLSConfig: tlsConfig{SSLServerCA: ca.certPEM, SSLClientCert: client.certPEM, SSLClientKey: client.keyPEM},
	}, &tls.Config{Certificates: []tls.Certificate{server.pair}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}
}

func TestTLSConfigValidation(t *testing.T) {
	valid, _ := testTLSConfig(t)
	for _, tc := range []struct {
		name   string
		modify func(*config)
		want   string
	}{
		{"TLS disabled", func(c *config) { c.TLS = false }, "requires 'tls'"},
		{"invalid CA", func(c *config) { c.TLSConfig.SSLServerCA = "invalid PEM" }, "no PEM-encoded certificates"},
		{"missing certificate", func(c *config) { c.TLSConfig.SSLClientCert = "" }, "must be provided together"},
		{"missing key", func(c *config) { c.TLSConfig.SSLClientKey = "" }, "must be provided together"},
		{"invalid certificate", func(c *config) { c.TLSConfig.SSLClientCert = "invalid PEM" }, "invalid 'ssl_client_cert' / 'ssl_client_key'"},
		{"invalid key", func(c *config) { c.TLSConfig.SSLClientKey = "invalid PEM" }, "invalid 'ssl_client_cert' / 'ssl_client_key'"},
		{"mismatched key", func(c *config) { c.TLSConfig.SSLClientKey = newTestCertificate(t, nil, "other", 0).keyPEM }, "private key does not match public key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.modify(&cfg)
			var parsed config
			require.ErrorContains(t, pf.UnmarshalStrict(mustMarshal(t, cfg), &parsed), tc.want)
		})
	}
	for _, certificates := range []tlsConfig{{}, {SSLClientCert: valid.TLSConfig.SSLClientCert, SSLClientKey: valid.TLSConfig.SSLClientKey}} {
		cfg := valid
		cfg.TLSConfig = certificates
		options, err := cfg.buildTLSConfig()
		require.NoError(t, err)
		require.Nil(t, options.RootCAs, "unspecified CA must retain system roots")
		require.False(t, options.InsecureSkipVerify)
		require.Equal(t, cfg.Host, options.ServerName)
	}
	var plaintext config
	require.NoError(t, pf.UnmarshalStrict([]byte(validConfigJSON), &plaintext))
	options, err := plaintext.buildTLSConfig()
	require.NoError(t, err)
	require.Nil(t, options)
}

func TestTLSVerification(t *testing.T) {
	valid, serverConfig := testTLSConfig(t)
	otherCA := newTestCertificate(t, nil, "other-ca", 0)
	for _, tc := range []struct {
		name   string
		modify func(*config, *tls.Config)
		want   string
	}{
		{"private CA and mTLS", func(*config, *tls.Config) {}, ""},
		{"CA bundle", func(c *config, _ *tls.Config) { c.TLSConfig.SSLServerCA = otherCA.certPEM + c.TLSConfig.SSLServerCA }, ""},
		{"self-signed server", func(c *config, s *tls.Config) {
			c.Host, c.TLSConfig.SSLServerCA = "other-ca", otherCA.certPEM
			s.Certificates = []tls.Certificate{otherCA.pair}
		}, ""},
		{"untrusted CA", func(c *config, _ *tls.Config) { c.TLSConfig.SSLServerCA = otherCA.certPEM }, "unknown authority"},
		{"wrong hostname", func(c *config, _ *tls.Config) { c.Host = "wrong.test" }, "wrong.test"},
		{"missing client certificate", func(c *config, _ *tls.Config) { c.TLSConfig.SSLClientCert = ""; c.TLSConfig.SSLClientKey = "" }, "client didn't provide a certificate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			serverOptions := serverConfig.Clone()
			tc.modify(&cfg, serverOptions)
			options, err := cfg.buildTLSConfig()
			require.NoError(t, err)
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			require.NoError(t, clientConn.SetDeadline(time.Now().Add(5*time.Second)))
			require.NoError(t, serverConn.SetDeadline(time.Now().Add(5*time.Second)))
			server := tls.Server(serverConn, serverOptions)
			client := tls.Client(clientConn, options)
			done := make(chan error, 1)
			go func() {
				err := server.Handshake()
				serverConn.Close()
				done <- err
			}()
			clientErr := client.Handshake()
			clientConn.Close()
			serverErr := receive(t, done)
			if tc.want != "" {
				require.ErrorContains(t, errors.Join(clientErr, serverErr), tc.want)
				return
			}
			require.NoError(t, clientErr)
			require.NoError(t, serverErr)
			require.Equal(t, "estuary", server.ConnectionState().PeerCertificates[0].Subject.CommonName)
			require.NotEmpty(t, server.ConnectionState().VerifiedChains)
		})
	}
}

func TestAMQPTLSAuthentication(t *testing.T) {
	for _, mechanism := range []string{"PLAIN", "EXTERNAL"} {
		t.Run(mechanism, func(t *testing.T) {
			cfg, serverConfig := testTLSConfig(t)
			cfg.AuthMechanism = mechanism
			response := "\x00test\x00test"
			if mechanism == "EXTERNAL" {
				cfg.Username, cfg.Password = "", ""
				response = "\x00*\x00*"
			}
			require.NoError(t, cfg.Validate())
			cfg.SetDefaults()
			options, err := clientConfig(context.Background(), &cfg)
			require.NoError(t, err)
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			require.NoError(t, serverConn.SetDeadline(time.Now().Add(5*time.Second)))
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer serverConn.Close()
				server := tls.Server(serverConn, serverConfig)
				peer := amqpPeer{t: t, conn: server}
				peer.handshakeAuth(mechanism, response)
				require.Equal(t, "estuary", server.ConnectionState().PeerCertificates[0].Subject.CommonName)
				peer.readMethod(10, 50)
				peer.sendMethod(0, 10, 51, nil)
			}()
			client, err := amqp.Open(tls.Client(clientConn, options.TLSClientConfig), options)
			require.NoError(t, err)
			require.NoError(t, client.CloseDeadline(time.Now().Add(time.Second)))
			receive(t, done)
		})
	}
}

func TestDiscoverTLS(t *testing.T) {
	for _, mechanism := range []string{"PLAIN", "EXTERNAL"} {
		t.Run(mechanism, func(t *testing.T) {
			cfg, serverConfig := testTLSConfig(t)
			cfg.AuthMechanism = mechanism
			cfg.SetDefaults()
			client, err := buildHTTPClient(&cfg)
			require.NoError(t, err)
			defer client.CloseIdleConnections()
			transport := client.Transport.(*http.Transport)
			transport.Proxy = nil
			done := make(chan struct{})
			transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
				clientConn, serverConn := net.Pipe()
				require.NoError(t, serverConn.SetDeadline(time.Now().Add(5*time.Second)))
				go func() {
					defer close(done)
					defer serverConn.Close()
					server := tls.Server(serverConn, serverConfig)
					req, err := http.ReadRequest(bufio.NewReader(server))
					require.NoError(t, err)
					require.Equal(t, "/api/queues/%2F", req.URL.EscapedPath())
					username, password, ok := req.BasicAuth()
					require.True(t, ok)
					require.Equal(t, "test", username)
					require.Equal(t, "test", password)
					require.Equal(t, "estuary", server.ConnectionState().PeerCertificates[0].Subject.CommonName)
					_, err = io.WriteString(server, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n[]")
					require.NoError(t, err)
				}()
				return clientConn, nil
			}
			response, err := (driver{httpClient: client}).Discover(context.Background(), &pc.Request_Discover{ConfigJson: mustMarshal(t, cfg)})
			require.NoError(t, err)
			require.Empty(t, response.Bindings)
			receive(t, done)
		})
	}
}

func TestDiscoverExternalRequiresManagementCredentials(t *testing.T) {
	cfg, _ := testTLSConfig(t)
	cfg.AuthMechanism, cfg.Username, cfg.Password = "EXTERNAL", "", ""
	_, err := driver{}.Discover(context.Background(), &pc.Request_Discover{ConfigJson: mustMarshal(t, cfg)})
	require.ErrorContains(t, err, "management API discovery requires 'username' and 'password'")
}
