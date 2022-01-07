package sshforwarding

import (
	"encoding/base64"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSshForwardConfig_Validate(t *testing.T) {
	var validConfig = SshForwardingConfig{
		SshEndpoint:         "test_endpoint",
		SshPrivateKeyBase64: "test_private_key",
		SshUser:             "test_ssh_user",
		RemoteHost:          "remote_host",
		RemotePort:          1234,
	}

	require.NoError(t, validConfig.Validate())

	var MissingSshEndpoint = validConfig
	MissingSshEndpoint.SshEndpoint = ""
	require.Error(t, MissingSshEndpoint.Validate(), "expected validation error if ssh_endpoint is missing")

	var MissingRemoteHost = validConfig
	MissingRemoteHost.RemoteHost = ""
	require.Error(t, MissingRemoteHost.Validate(), "expected validation error if remote_host is missing")

	var MissingSshPrivateKey = validConfig
	MissingSshPrivateKey.SshPrivateKeyBase64 = ""
	require.Error(t, MissingSshPrivateKey.Validate(), "expected validation error if ssh_private_key_base64 is missing")
}

// Configuration set based on docker-compose.yaml.
func createForwardingTestConfig() (*SshForwardingConfig, error) {
	var b, err = os.ReadFile("test_sshd_configs/keys/id_rsa")
	if err != nil {
		return nil, err
	}
	return &SshForwardingConfig{
		SshEndpoint:         "127.0.0.1:2222",
		SshPrivateKeyBase64: base64.RawStdEncoding.EncodeToString(b),
		SshUser:             "flowssh",
		RemoteHost:          "127.0.0.1",
		RemotePort:          2222, // Tunnelling to SSH service itself to support unit testing.
	}, nil
}

func TestSshForwardConfig_startWithDefault(t *testing.T) {
	var config, err = createForwardingTestConfig()
	require.NoError(t, err)

	deployed_local_port, err := config.Start(15433)
	require.NoError(t, err)
	require.Equal(t, uint16(15433), deployed_local_port)

	deployed_local_port, err = config.Start(0)
	require.NoError(t, err)
	require.NotEqual(t, uint16(15433), deployed_local_port)
	require.GreaterOrEqual(t, deployed_local_port, uint16(10000))
}

func TestSshForwardConfig_startWithConflictingPorts(t *testing.T) {
	var config, err = createForwardingTestConfig()
	require.NoError(t, err)

	deployed_local_port, err := config.Start(15430)
	require.NoError(t, err)
	require.Equal(t, uint16(15433), deployed_local_port)

	// The port is no longer available when trying to start a second tunnel with it.
	deployed_local_port, err = config.startWithTimeout(15430, 1)
	require.Contains(t, err.Error(), "Local port 15430 is unavailable.")
}

func TestSshForwardConfig_startWithDefaultWithBadSshEndpoint(t *testing.T) {
	var config, err = createForwardingTestConfig()
	require.NoError(t, err)
	config.SshEndpoint = "bad_endpoint"
	_, err = config.startWithTimeout(0, 1)
	require.Contains(t, err.Error(), "Failed to create to SSH tunnel.")
}
