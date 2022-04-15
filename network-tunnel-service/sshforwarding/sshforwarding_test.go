package sshforwarding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSshForwardConfig_Validate(t *testing.T) {
	var validConfig = SshForwardingConfig{
		SshEndpoint: "test_endpoint",
		PrivateKey:  "test_private_key",
		User:        "test_ssh_user",
		ForwardHost: "forward_host",
		ForwardPort: 1234,
	}

	require.NoError(t, validConfig.Validate())

	var MissingSshEndpoint = validConfig
	MissingSshEndpoint.SshEndpoint = ""
	require.Error(t, MissingSshEndpoint.Validate(), "expected validation error if ssh_endpoint is missing")

	var MissingForwardHost = validConfig
	MissingForwardHost.ForwardHost = ""
	require.Error(t, MissingForwardHost.Validate(), "expected validation error if forward_host is missing")

	var MissingPrivateKey = validConfig
	MissingPrivateKey.PrivateKey = ""
	require.Error(t, MissingPrivateKey.Validate(), "expected validation error if private_key is missing")
}
