package networkTunnel

import (
	"context"
	"fmt"
	"net"
)

type SSHForwardingConfig struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type TunnelConfig struct {
	SSHForwarding *SSHForwardingConfig `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

func (cfg *TunnelConfig) InUse() bool {
	return cfg != nil && cfg.SSHForwarding != nil && cfg.SSHForwarding.SSHEndpoint != ""
}

func (cfg *TunnelConfig) Start(ctx context.Context, address string, localPort string) (*SshTunnel, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("error splitting address %q into host and port: %w", address, err)
	}

	var sshConfig = &SshConfig{
		SshEndpoint: cfg.SSHForwarding.SSHEndpoint,
		PrivateKey:  []byte(cfg.SSHForwarding.PrivateKey),
		ForwardHost: host,
		ForwardPort: port,
		LocalPort:   localPort,
	}
	var tunnel = sshConfig.CreateTunnel()

	if err := tunnel.Start(); err != nil {
		return nil, err
	}
	return tunnel, nil
}
