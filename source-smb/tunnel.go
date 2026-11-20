package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/estuary/connectors/filesource"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"golang.org/x/crypto/ssh"
)

func validateNetworkTunnel(cfg *networkTunnel.TunnelConfig) error {
	if cfg == nil || cfg.SSHForwarding == nil {
		return nil
	}
	var forwarding = cfg.SSHForwarding
	if forwarding.SSHEndpoint == "" {
		return fmt.Errorf("missing 'networkTunnel.sshForwarding.sshEndpoint'")
	}
	if forwarding.PrivateKey == "" {
		return fmt.Errorf("missing 'networkTunnel.sshForwarding.privateKey'")
	}
	u, err := url.Parse(forwarding.SSHEndpoint)
	if err != nil || u.Scheme != "ssh" || u.Hostname() == "" || u.User == nil || u.User.Username() == "" {
		return fmt.Errorf("invalid 'networkTunnel.sshForwarding.sshEndpoint': expected ssh://user@hostname[:port]")
	}
	if _, hasPassword := u.User.Password(); hasPassword || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid 'networkTunnel.sshForwarding.sshEndpoint': expected ssh://user@hostname[:port]")
	}
	if u.Port() != "" {
		if port, err := strconv.Atoi(u.Port()); err != nil || port < 1 || port > 65535 {
			return fmt.Errorf("invalid 'networkTunnel.sshForwarding.sshEndpoint': port must be between 1 and 65535")
		}
	}
	if _, err := ssh.ParsePrivateKey([]byte(forwarding.PrivateKey)); err != nil {
		return fmt.Errorf("invalid 'networkTunnel.sshForwarding.privateKey': %w", err)
	}
	return nil
}

type smbTunnelKey struct {
	address string
	networkTunnel.SSHForwardingConfig
}

var smbStartTunnel = (*networkTunnel.TunnelConfig).Start

func (d *smbDriver) connect(ctx context.Context, cfg filesource.Config) (filesource.Store, error) {
	var endpoint = cfg.(config)
	var address = net.JoinHostPort(endpoint.Host, strconv.Itoa(endpoint.Port))
	if endpoint.NetworkTunnel.InUse() {
		port, err := d.tunnelPort(ctx, endpoint)
		if err != nil {
			return nil, fmt.Errorf("starting SSH tunnel: %w", err)
		}
		address = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	}
	return newSmbSource(ctx, endpoint, address)
}

func (d *smbDriver) tunnelPort(ctx context.Context, cfg config) (int, error) {
	d.tunnelMu.Lock()
	defer d.tunnelMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	var key = smbTunnelKey{
		address:             net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		SSHForwardingConfig: *cfg.NetworkTunnel.SSHForwarding,
	}
	if port, ok := d.tunnels[key]; ok {
		return port, nil
	}

	// Shared tunnels live for the connector process, across RPCs and SMB reconnects.
	// Use unprivileged ports because the connector runs as nonroot.
	var port = 9445 + len(d.tunnels)
	if _, err := smbStartTunnel(cfg.NetworkTunnel, ctx, key.address, strconv.Itoa(port)); err != nil {
		return 0, err
	}
	if d.tunnels == nil {
		d.tunnels = make(map[smbTunnelKey]int)
	}
	d.tunnels[key] = port
	return port, nil
}
