package network_tunnel

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

type SshConfig struct {
	SshEndpoint string
	PrivateKey  []byte
	ForwardHost string
	ForwardPort string
	LocalPort   string
}

type SshTunnel struct {
	Config SshConfig
	ctx    context.Context
	cmd    *exec.Cmd
	cancel context.CancelFunc
}

func (c SshConfig) CreateTunnel() SshTunnel {
	var ctx, cancel = context.WithCancel(context.Background())
	return SshTunnel{
		Config: c,
		ctx:    ctx,
		cancel: cancel,
		cmd:    nil,
	}
}

func (t *SshTunnel) Start() ([]byte, error) {
	if t.cmd != nil {
		return nil, errors.New("This tunnel has already been started.")
	}

	var tmpKeyFile, err = os.CreateTemp("flow-network-tunnel", "")
	if err != nil {
		return nil, fmt.Errorf("creating temporary file for private key: %w", err)
	}

	if _, err := tmpKeyFile.Write(t.Config.PrivateKey); err != nil {
		return nil, fmt.Errorf("writing key to temporary file: %w", err)
	}

	t.cmd = exec.CommandContext(
		t.ctx,
		"flow-network-tunnel",
		"--ssh-endpoint", t.Config.SshEndpoint,
		"--private-key", tmpKeyFile.Name(),
		"--forward-host", t.Config.ForwardHost,
		"--forward-port", t.Config.ForwardPort,
		"--local-port", t.Config.LocalPort,
	)

	return t.cmd.CombinedOutput()
}

func (t *SshTunnel) Stop() {
	t.cancel()
}
