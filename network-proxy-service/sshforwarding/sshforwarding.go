package sshforwarding

import "errors"

type SshForwardingConfig struct {
	SshEndpoint string `json:"sshEndpoint" jsonschema:"description=Endpoint of the remote SSH server that supports tunneling, in the form of ssh://hostname[:port]"`
	PrivateKey  string `json:"privateKey" jsonschema:"description=private Key to connect to the remote SSH server."`
	User        string `json:"user,omitempty" jsonschema:"description=User name to connect to the remote SSH server."`
	ForwardHost string `json:"forwardHost" jsonschema:"description=Host name to connect from the remote SSH server to the remote destination (e.g. DB) via internal network."`
	ForwardPort uint16 `json:"forwardPort,omitempty" jsonschema:"description=Port of the remote destination."`
	LocalPort   uint16 `json:"localPort" jsonschema:"description=Local port to start the SSH tunnel. The connector should fetch data from localhost:<local_port> after SSH tunnel is enabled."`
}

func (sfc SshForwardingConfig) Validate() error {
	if sfc.SshEndpoint == "" {
		return errors.New("missing sshEndpoint")
	}

	if sfc.ForwardHost == "" {
		return errors.New("missing forwardHost")
	}

	if sfc.PrivateKey == "" {
		return errors.New("missing PrivateKey")
	}

	return nil
}
