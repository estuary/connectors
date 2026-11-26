package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func testSSHKey(t *testing.T) string {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	block, err := ssh.MarshalPrivateKey(privateKey, "source-smb test")
	require.NoError(t, err)
	return string(pem.EncodeToMemory(block))
}

func testTunnelConfig(t *testing.T) config {
	t.Helper()
	cfg, err := newDriver().NewConfig(json.RawMessage(testConfig))
	require.NoError(t, err)
	var endpoint = cfg.(config)
	endpoint.NetworkTunnel = &networkTunnel.TunnelConfig{SSHForwarding: &networkTunnel.SSHForwardingConfig{
		SSHEndpoint: "ssh://flow@bastion.example:2222",
		PrivateKey:  testSSHKey(t),
	}}
	return endpoint
}

func TestNetworkTunnelConfig(t *testing.T) {
	var key = testSSHKey(t)
	for _, tt := range []struct {
		name       string
		forwarding any
		wantErr    string
	}{
		{"no forwarding", nil, ""},
		{"default SSH port", map[string]string{"sshEndpoint": "ssh://flow@bastion.example", "privateKey": key}, ""},
		{"custom SSH port", map[string]string{"sshEndpoint": "ssh://flow@bastion.example:2222", "privateKey": key}, ""},
		{"IPv6 bastion", map[string]string{"sshEndpoint": "ssh://flow@[2001:db8::1]:2222", "privateKey": key}, ""},
		{"missing endpoint", map[string]string{"privateKey": key}, "missing 'networkTunnel.sshForwarding.sshEndpoint'"},
		{"missing key", map[string]string{"sshEndpoint": "ssh://flow@bastion.example"}, "missing 'networkTunnel.sshForwarding.privateKey'"},
		{"empty forwarding", map[string]string{}, "missing 'networkTunnel.sshForwarding.sshEndpoint'"},
		{"invalid key", map[string]string{"sshEndpoint": "ssh://flow@bastion.example", "privateKey": "not a private key"}, "invalid 'networkTunnel.sshForwarding.privateKey'"},
		{"unknown field", map[string]string{"sshEndpoint": "ssh://flow@bastion.example", "privateKey": key, "password": "secret"}, "unknown field"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var properties map[string]any
			require.NoError(t, json.Unmarshal([]byte(testConfig), &properties))
			properties["networkTunnel"] = map[string]any{"sshForwarding": tt.forwarding}
			raw, err := json.Marshal(properties)
			require.NoError(t, err)
			cfg, err := newDriver().NewConfig(raw)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.NotContains(t, err.Error(), key)
			} else {
				require.NoError(t, err)
				require.Equal(t, 445, cfg.(config).Port)
				require.Equal(t, tt.forwarding != nil, cfg.(config).NetworkTunnel.InUse())
			}
		})
	}

	for _, endpoint := range []string{
		"bastion.example", "https://flow@bastion.example", "ssh://bastion.example", "ssh://@bastion.example",
		"ssh://flow@", "ssh://flow@bastion.example:0", "ssh://flow@bastion.example:65536", "ssh://flow@bastion.example:bad",
		"ssh://flow:secret@bastion.example", "ssh://flow@bastion.example/path", "ssh://flow@bastion.example?query", "ssh://flow@bastion.example#fragment",
	} {
		t.Run(endpoint, func(t *testing.T) {
			var cfg = testTunnelConfig(t)
			cfg.NetworkTunnel.SSHForwarding.SSHEndpoint = endpoint
			raw, err := json.Marshal(cfg)
			require.NoError(t, err)
			_, err = newDriver().NewConfig(raw)
			require.ErrorContains(t, err, "invalid 'networkTunnel.sshForwarding.sshEndpoint'")
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestNetworkTunnelConnections(t *testing.T) {
	for _, tt := range []struct {
		name   string
		host   string
		port   int
		tunnel bool
	}{
		{"direct", "smb.example", 445, false},
		{"tunneled", "smb.internal", 445, true},
		{"custom port", "smb.internal", 1445, true},
		{"IPv6 SMB", "2001:db8::2", 445, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var cfg = testTunnelConfig(t)
			cfg.Host, cfg.Port = tt.host, tt.port
			if !tt.tunnel {
				cfg.NetworkTunnel = nil
			}
			var starts, dials int
			var previousStart, previousDial = smbStartTunnel, smbDialContext
			t.Cleanup(func() { smbStartTunnel, smbDialContext = previousStart, previousDial })
			smbStartTunnel = func(tunnel *networkTunnel.TunnelConfig, ctx context.Context, address, port string) (*networkTunnel.SshTunnel, error) {
				starts++
				require.Equal(t, cfg.NetworkTunnel, tunnel)
				require.NoError(t, ctx.Err())
				require.Equal(t, net.JoinHostPort(tt.host, strconv.Itoa(tt.port)), address)
				require.Equal(t, "9445", port)
				return nil, nil
			}
			var dialErr = errors.New("test SMB connection failed")
			smbDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				dials++
				require.Equal(t, "tcp", network)
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.WithinDuration(t, time.Now().Add(30*time.Second), deadline, time.Second)
				if tt.tunnel {
					require.Equal(t, 1, starts, "wait for the tunnel before connecting to SMB")
					require.Equal(t, "127.0.0.1:9445", address)
				} else {
					require.Equal(t, 0, starts)
					require.Equal(t, net.JoinHostPort(tt.host, strconv.Itoa(tt.port)), address)
				}
				return nil, dialErr
			}

			var driver = newDriver()
			raw, err := json.Marshal(cfg)
			require.NoError(t, err)
			_, err = driver.Discover(context.Background(), &pc.Request_Discover{ConfigJson: raw})
			require.ErrorIs(t, err, dialErr)
			_, err = driver.Validate(context.Background(), &pc.Request_Validate{ConfigJson: raw})
			require.ErrorIs(t, err, dialErr)
			err = driver.Pull(&pc.Request_Open{Capture: &pf.CaptureSpec{ConfigJson: raw}}, &boilerplate.PullOutput{Connector_CaptureServer: testStream{}})
			require.ErrorIs(t, err, dialErr)
			require.Equal(t, 3, dials)
			require.Equal(t, tt.host, cfg.Host)
			require.Equal(t, tt.port, cfg.Port)
		})
	}
}

func TestNetworkTunnelTargets(t *testing.T) {
	var cfg = testTunnelConfig(t)
	var driver = newDriver()
	var previousStart, previousDial = smbStartTunnel, smbDialContext
	t.Cleanup(func() { smbStartTunnel, smbDialContext = previousStart, previousDial })
	var starts int
	smbStartTunnel = func(tunnel *networkTunnel.TunnelConfig, _ context.Context, address, port string) (*networkTunnel.SshTunnel, error) {
		starts++
		require.Equal(t, net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)), address)
		require.Equal(t, cfg.NetworkTunnel, tunnel)
		require.Equal(t, strconv.Itoa(9444+starts), port)
		return nil, nil
	}
	var dialErr = errors.New("test SMB connection failed")
	smbDialContext = func(_ context.Context, _, address string) (net.Conn, error) {
		require.Equal(t, net.JoinHostPort("127.0.0.1", strconv.Itoa(9444+starts)), address)
		return nil, dialErr
	}

	for i, change := range []func(){
		func() {},
		func() { cfg.Host = "other.internal" },
		func() { cfg.Port = 1445 },
		func() { cfg.NetworkTunnel.SSHForwarding.SSHEndpoint = "ssh://flow@other-bastion.example" },
		func() { cfg.NetworkTunnel.SSHForwarding.PrivateKey = testSSHKey(t) },
	} {
		change()
		_, err := driver.Connect(context.Background(), cfg)
		require.ErrorIs(t, err, dialErr)
		require.Equal(t, i+1, starts)
		_, err = driver.Connect(context.Background(), cfg)
		require.ErrorIs(t, err, dialErr)
		require.Equal(t, i+1, starts, "reuse the same tunnel after an SMB connection failure")
	}
}

func TestNetworkTunnelFailure(t *testing.T) {
	var cfg = testTunnelConfig(t)
	var previousStart, previousDial = smbStartTunnel, smbDialContext
	t.Cleanup(func() { smbStartTunnel, smbDialContext = previousStart, previousDial })
	var tunnelErr = errors.New("test SSH authentication failed")
	var starts int
	smbStartTunnel = func(*networkTunnel.TunnelConfig, context.Context, string, string) (*networkTunnel.SshTunnel, error) {
		starts++
		return nil, tunnelErr
	}
	smbDialContext = func(context.Context, string, string) (net.Conn, error) {
		t.Fatal("must not dial SMB when the tunnel fails")
		return nil, nil
	}
	var driver = newDriver()
	for range 2 {
		_, err := driver.Connect(context.Background(), cfg)
		require.ErrorIs(t, err, tunnelErr)
		require.ErrorContains(t, err, "starting SSH tunnel")
	}
	require.Equal(t, 2, starts, "do not cache a failed tunnel")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := driver.Connect(ctx, cfg)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 2, starts)
}

func TestNetworkTunnelSMBNegotiation(t *testing.T) {
	var cfg = testTunnelConfig(t)
	var previousStart, previousDial = smbStartTunnel, smbDialContext
	t.Cleanup(func() { smbStartTunnel, smbDialContext = previousStart, previousDial })
	smbStartTunnel = func(*networkTunnel.TunnelConfig, context.Context, string, string) (*networkTunnel.SshTunnel, error) {
		return nil, nil
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	smbDialContext = func(_ context.Context, _, address string) (net.Conn, error) {
		if address != "127.0.0.1:9445" {
			return nil, errors.New("SMB must connect through the tunnel")
		}
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, server.SetDeadline(time.Now().Add(5*time.Second)))
	var result = make(chan error, 1)
	go func() {
		_, err := newDriver().Connect(ctx, cfg)
		result <- err
	}()
	request, err := readSmbPacket(server)
	require.NoError(t, err)
	require.Equal(t, "\xfeSMB", string(request[:4]))
	require.Equal(t, uint16(0), binary.LittleEndian.Uint16(request[12:]))
	require.NotZero(t, binary.LittleEndian.Uint16(request[68:])&0x2, "SMB signing remains required through SSH")
	require.NoError(t, server.Close())
	select {
	case err := <-result:
		require.ErrorContains(t, err, "creating SMB session")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func readSmbPacket(conn net.Conn) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, err
	}
	var packet = make([]byte, binary.BigEndian.Uint32(header[:]))
	_, err := io.ReadFull(conn, packet)
	return packet, err
}
