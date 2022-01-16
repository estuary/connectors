package sshforwarding

import "errors"

type SshForwardingConfig struct {
	SshEndpoint         string `json:"ssh_endpoint" jsonschema:"description=Endpoint of the remote SSH server that supports tunneling, in the form of ssh://hostname[:port]"`
	SshPrivateKeyBase64 string `json:"ssh_private_key_base64" jsonschema:"description=Base64-encoded private Key to connect to the remote SSH server."`
	SshUser             string `json:"ssh_user,omitempty" jsonschema:"description=User name to connect to the remote SSH server."`
	RemoteHost          string `json:"remote_host" jsonschema:"description=Host name to connect from the remote SSH server to the remote destination (e.g. DB) via internal network."`
	RemotePort          uint16 `json:"remote_port,omitempty" jsonschema:"description=Port of the remote destination."`
	LocalPort           uint16 `json:"local_port" jsonschema:"description=Local port to start the SSH tunnel. The connector should fetch data from localhost:<local_port> after SSH tunnel is enabled."`
}

func (sfc SshForwardingConfig) Validate() error {
	if sfc.SshEndpoint == "" {
		return errors.New("missing ssh_endpoint")
	}

	if sfc.RemoteHost == "" {
		return errors.New("missing remote_host")
	}

	if sfc.SshPrivateKeyBase64 == "" {
		return errors.New("missing ssh_private_key_base64")
	}

	return nil
}
