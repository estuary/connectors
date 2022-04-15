package networktunnel

import (
	"os"

	sf "github.com/estuary/connectors/network-tunnel-service/sshforwarding"
)

// Configuration set based on sshforwarding/test_sshd_configs/docker-compose.yaml.
func CreateSshForwardingTestConfig(keyFilePath string, remotePort uint16) (*NetworkTunnelConfig, error) {
	var b, err = os.ReadFile(keyFilePath)
	if err != nil {
		return nil, err
	}
	return &NetworkTunnelConfig{
		TunnelType: "sshForwarding",
		SshForwardingConfig: sf.SshForwardingConfig{
			SshEndpoint: "ssh://127.0.0.1:2222",
			PrivateKey:  string(b),
			User:        "flowssh",
			ForwardHost: "127.0.0.1",
			ForwardPort: remotePort,
			LocalPort:   12345,
		},
	}, nil
}
