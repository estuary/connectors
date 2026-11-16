package connector

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestConnectionConfig(t *testing.T) {
	for _, tt := range []struct {
		config  string
		wantErr string
	}{
		{`"sentinel":{"masterName":"primary","addresses":["sentinel:26379"],"username":"user","password":"secret"}`, ""},
		{`"sentinel":{}`, "sentinel.masterName"},
		{`"sentinel":{"masterName":"primary","addresses":["sentinel"]}`, "host:port"},
		{`"sentinel":{"masterName":"primary","addresses":["sentinel:0"]}`, "port must be"},
		{`"networkTunnel":{}`, ""},
		{`"networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion:22","privateKey":"key"}}`, ""},
		{`"networkTunnel":{"sshForwarding":{"privateKey":"key"}}`, "sshEndpoint"},
		{`"networkTunnel":{"sshForwarding":{"sshEndpoint":"https://user@bastion","privateKey":"key"}}`, "sshEndpoint"},
		{`"networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://bastion","privateKey":"key"}}`, "sshEndpoint"},
		{`"networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion:0","privateKey":"key"}}`, "port must be"},
		{`"networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion"}}`, "privateKey"},
		{`"advanced":{"ssl_server_name":"redis.example"}`, "require tls"},
		{`"tls":true,"advanced":{"ssl_server_ca":"invalid"}`, "ssl_server_ca"},
		{`"tls":true,"advanced":{"ssl_client_cert":"invalid"}`, "provided together"},
		{`"tls":true,"advanced":{"ssl_client_key":"invalid"}`, "provided together"},
		{`"tls":true,"advanced":{"ssl_client_cert":"invalid","ssl_client_key":"invalid"}`, "ssl_client_cert"},
		{`"tls":true,"advanced":{"ssl_server_name":"redis.example","ssl_skip_verify":true}`, ""},
	} {
		t.Run(tt.config, func(t *testing.T) {
			var cfg config
			err := pf.UnmarshalStrict(json.RawMessage(`{"address":"redis:6379",`+tt.config+`}`), &cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestTLSHandshake(t *testing.T) {
	ca, cert, key := testCertificates(t)
	serverCert, err := tls.X509KeyPair([]byte(cert), []byte(key))
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM([]byte(ca)))
	for _, tt := range []struct {
		name     string
		advanced advancedConfig
		mtls     bool
		wantErr  bool
	}{
		{"custom CA", advancedConfig{SSLServerCA: ca, SSLServerName: "redis.internal"}, false, false},
		{"untrusted CA", advancedConfig{SSLServerName: "redis.internal"}, false, true},
		{"wrong hostname", advancedConfig{SSLServerCA: ca, SSLServerName: "wrong.internal"}, false, true},
		{"skip verification", advancedConfig{SSLServerName: "wrong.internal", SSLSkipVerify: true}, false, false},
		{"mutual TLS", advancedConfig{SSLServerCA: ca, SSLServerName: "redis.internal", SSLClientCert: cert, SSLClientKey: key}, true, false},
		{"missing client certificate", advancedConfig{SSLServerCA: ca, SSLServerName: "redis.internal"}, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := (config{TLS: true, Advanced: tt.advanced}).tlsConfig()
			require.NoError(t, err)
			require.Equal(t, uint16(tls.VersionTLS12), cfg.MinVersion)
			serverConfig := &tls.Config{Certificates: []tls.Certificate{serverCert}, ClientCAs: roots}
			if tt.mtls {
				serverConfig.ClientAuth = tls.RequireAndVerifyClientCert
			}
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tls.Server(serverConn, serverConfig).HandshakeContext(ctx) }()
			clientErr := tls.Client(clientConn, cfg).HandshakeContext(ctx)
			clientConn.Close()
			serverErr := <-done
			if tt.wantErr {
				require.True(t, clientErr != nil || serverErr != nil)
			} else {
				require.NoError(t, clientErr)
				require.NoError(t, serverErr)
			}
		})
	}
}

func testCertificates(t *testing.T) (string, string, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	require.NoError(t, err)
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), DNSNames: []string{"redis.internal"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})),
		string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})),
		string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
}

func TestTunnelForwarding(t *testing.T) {
	var forwarded []string
	f := &tunnelForwarder{start: func(ctx context.Context, address string) (string, error) {
		forwarded = append(forwarded, address)
		return "127.0.0.1:12345", nil
	}}
	for _, address := range []string{"sentinel:26379", "primary:6379", "primary:6379", "new-primary:6379"} {
		local, err := f.address(context.Background(), address)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1:12345", local)
	}
	require.Equal(t, []string{"sentinel:26379", "primary:6379", "new-primary:6379"}, forwarded)
	f = &tunnelForwarder{start: func(context.Context, string) (string, error) { return "", errors.New("bastion unavailable") }}
	_, err := f.dialer(nil)(context.Background(), "tcp", "redis:6379")
	require.ErrorContains(t, err, "bastion unavailable")
	require.Empty(t, f.addresses)

	client, err := (config{Address: "redis:6379", NetworkTunnel: &networkTunnel.TunnelConfig{
		SSHForwarding: &networkTunnel.SSHForwardingConfig{SSHEndpoint: "ssh://user@bastion", PrivateKey: "key"},
	}}).client()
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })
	require.NotNil(t, client.Options().Dialer)
}

func TestSentinelFailover(t *testing.T) {
	var mu sync.Mutex
	primary := "primary-one"
	var commands []string
	dial := func(ctx context.Context, network, address string) (net.Conn, error) {
		if address == "unavailable:26379" {
			return nil, errors.New("Sentinel unavailable")
		}
		conn, server := net.Pipe()
		t.Cleanup(func() { conn.Close(); server.Close() })
		go serveRESP(server, func(args []string) any {
			mu.Lock()
			defer mu.Unlock()
			commands = append(commands, address+" "+strings.Join(args, " "))
			switch strings.ToLower(args[0]) {
			case "hello":
				return errors.New("ERR unknown command 'hello'")
			case "sentinel":
				if args[1] == "get-master-addr-by-name" {
					if args[2] != "mymaster" {
						return errors.New("ERR unknown primary")
					}
					return []any{primary, "6379"}
				}
				return []any{}
			case "subscribe":
				return []any{"subscribe", args[1], int64(1)}
			case "ping":
				return "PONG"
			case "set":
				if address != primary+":6379" {
					return errors.New("READONLY You can't write against a read only replica")
				}
				return "OK"
			default:
				return "OK"
			}
		})
		return conn, nil
	}
	cfg := config{
		Address: "unavailable:26379", Username: "redis-user", Password: "redis-password", Database: 2,
		Sentinel: &sentinelConfig{MasterName: "mymaster", Addresses: []string{"sentinel:26379"}, Username: "sentinel-user", Password: "sentinel-password"},
	}
	opts, err := cfg.clientOptions()
	require.NoError(t, err)
	opts.Dialer = dial
	client := cfg.newClient(opts)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err())
	require.NoError(t, client.Set(ctx, "orders:1", `{"id":1}`, 0).Err())
	mu.Lock()
	primary = "primary-two"
	mu.Unlock()
	require.NoError(t, client.Set(ctx, "orders:2", `{"id":2}`, 0).Err())
	mu.Lock()
	defer mu.Unlock()
	for _, want := range []string{
		"sentinel:26379 auth sentinel-user sentinel-password",
		"sentinel:26379 sentinel get-master-addr-by-name mymaster",
		"primary-one:6379 auth redis-user redis-password",
		"primary-one:6379 select 2", "primary-one:6379 ping",
		"primary-two:6379 auth redis-user redis-password",
		"primary-two:6379 select 2", `primary-two:6379 set orders:2 {"id":2}`,
		`primary-one:6379 set orders:2 {"id":2}`,
	} {
		require.True(t, slices.Contains(commands, want), "missing %s in %v", want, commands)
	}
}

func TestTunnelDialer(t *testing.T) {
	ca, cert, key := testCertificates(t)
	serverCert, err := tls.X509KeyPair([]byte(cert), []byte(key))
	require.NoError(t, err)
	for _, tt := range []struct {
		name       string
		secure     bool
		remote     string
		serverName string
	}{
		{"plaintext", false, "redis.internal:6379", ""},
		{"TLS", true, "redis.internal:6379", ""},
		{"TLS server name override", true, "alias.internal:6379", "redis.internal"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(func() { listener.Close() })
			cfg := config{TLS: tt.secure}
			if tt.secure {
				listener = tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{serverCert}})
				cfg.Advanced = advancedConfig{SSLServerCA: ca, SSLServerName: tt.serverName}
			}
			tlsConfig, err := cfg.tlsConfig()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type result struct {
				serverName string
				err        error
			}
			done := make(chan result, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					done <- result{err: err}
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				var state result
				if secure, ok := conn.(*tls.Conn); ok {
					if state.err = secure.HandshakeContext(ctx); state.err != nil {
						done <- state
						return
					}
					state.serverName = secure.ConnectionState().ServerName
				}
				_, state.err = io.WriteString(conn, "tunnel")
				done <- state
			}()
			f := &tunnelForwarder{start: func(ctx context.Context, remote string) (string, error) {
				require.Equal(t, tt.remote, remote)
				return listener.Addr().String(), nil
			}}
			conn, err := f.dialer(tlsConfig)(ctx, "tcp", tt.remote)
			require.NoError(t, err)
			defer conn.Close()
			require.Equal(t, listener.Addr().String(), conn.RemoteAddr().String())
			require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
			data := make([]byte, len("tunnel"))
			_, err = io.ReadFull(conn, data)
			require.NoError(t, err)
			require.Equal(t, "tunnel", string(data))
			state := <-done
			require.NoError(t, state.err)
			if tt.secure {
				require.Equal(t, "redis.internal", conn.(*tls.Conn).ConnectionState().ServerName)
				require.Equal(t, "redis.internal", state.serverName)
				require.Equal(t, tt.serverName, tlsConfig.ServerName, "dialing must not mutate the shared TLS config")
			}
		})
	}
}

func TestSentinelTunnel(t *testing.T) {
	ca, cert, key := testCertificates(t)
	serverCert, err := tls.X509KeyPair([]byte(cert), []byte(key))
	require.NoError(t, err)
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprintf("TLS=%t", secure), func(t *testing.T) {
			var mu sync.Mutex
			var forwarded, commands []string
			addresses := make(map[string]string)
			for _, address := range []string{"redis.internal:26379", "redis.internal:6379"} {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				if secure {
					listener = tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{serverCert}})
				}
				t.Cleanup(func() { listener.Close() })
				addresses[address] = listener.Addr().String()
				go func() {
					for {
						conn, err := listener.Accept()
						if err != nil {
							return
						}
						t.Cleanup(func() { conn.Close() })
						go serveRESP(conn, func(args []string) any {
							mu.Lock()
							defer mu.Unlock()
							commands = append(commands, address+" "+strings.Join(args, " "))
							switch strings.ToLower(args[0]) {
							case "hello":
								return errors.New("ERR unknown command 'hello'")
							case "sentinel":
								if args[1] == "get-master-addr-by-name" {
									return []any{"redis.internal", "6379"}
								}
								return []any{}
							case "subscribe":
								return []any{"subscribe", args[1], int64(1)}
							case "ping":
								return "PONG"
							default:
								return "OK"
							}
						})
					}
				}()
			}
			cfg := config{
				Address: "redis.internal:26379", TLS: secure, Sentinel: &sentinelConfig{MasterName: "mymaster"},
				NetworkTunnel: &networkTunnel.TunnelConfig{
					SSHForwarding: &networkTunnel.SSHForwardingConfig{SSHEndpoint: "ssh://user@bastion", PrivateKey: "key"},
				},
				startTunnel: func(ctx context.Context, address string) (string, error) {
					mu.Lock()
					defer mu.Unlock()
					forwarded = append(forwarded, address)
					if local, ok := addresses[address]; ok {
						return local, nil
					}
					return "", fmt.Errorf("unexpected tunnel address %q", address)
				},
			}
			if secure {
				cfg.Advanced.SSLServerCA = ca
			}
			client, err := cfg.client()
			require.NoError(t, err)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, client.Ping(ctx).Err())
			require.NoError(t, client.Set(ctx, "orders:1", `{"id":1}`, 0).Err())
			mu.Lock()
			defer mu.Unlock()
			require.ElementsMatch(t, []string{"redis.internal:26379", "redis.internal:6379"}, forwarded)
			require.Contains(t, commands, "redis.internal:26379 sentinel get-master-addr-by-name mymaster")
			require.Contains(t, commands, "redis.internal:6379 ping")
			require.Contains(t, commands, `redis.internal:6379 set orders:1 {"id":1}`)
		})
	}
}

func serveRESP(conn net.Conn, handle func([]string) any) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
		if err != nil {
			return
		}
		args := make([]string, n)
		for i := range args {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
			if err != nil || size < 0 {
				return
			}
			data := make([]byte, size+2)
			if _, err := io.ReadFull(r, data); err != nil {
				return
			}
			args[i] = string(data[:size])
		}
		var reply strings.Builder
		writeRESP(&reply, handle(args))
		if _, err := io.WriteString(conn, reply.String()); err != nil {
			return
		}
	}
}

func writeRESP(w io.Writer, value any) {
	switch v := value.(type) {
	case nil:
		fmt.Fprint(w, "$-1\r\n")
	case error:
		fmt.Fprintf(w, "-%s\r\n", v)
	case string:
		fmt.Fprintf(w, "$%d\r\n%s\r\n", len(v), v)
	case int64:
		fmt.Fprintf(w, ":%d\r\n", v)
	case []any:
		fmt.Fprintf(w, "*%d\r\n", len(v))
		for _, item := range v {
			writeRESP(w, item)
		}
	default:
		panic(fmt.Sprintf("unsupported RESP value %T", v))
	}
}
