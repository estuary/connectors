package network_tunnel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

type SshConfig struct {
	SshEndpoint string
	PrivateKey  []byte
	ForwardHost string
	ForwardPort string
	LocalPort   string
}

type SshTunnel struct {
	Config *SshConfig
	ctx    context.Context
	Cmd    *exec.Cmd
	cancel context.CancelFunc
}

func (c *SshConfig) CreateTunnel() SshTunnel {
	var ctx, cancel = context.WithCancel(context.Background())
	return SshTunnel{
		Config: c,
		ctx:    ctx,
		cancel: cancel,
		Cmd:    nil,
	}
}

// Start tunnel and wait until READY signal
func (t SshTunnel) Start() error {
	if t.Cmd != nil {
		return errors.New("This tunnel has already been started.")
	}

	var tmpKeyFile, err = os.CreateTemp("flow-network-tunnel", "")
	if err != nil {
		return fmt.Errorf("creating temporary file for private key: %w", err)
	}

	if _, err := tmpKeyFile.Write(t.Config.PrivateKey); err != nil {
		return fmt.Errorf("writing key to temporary file: %w", err)
	}

	log.WithFields(log.Fields{
		"ssh-endpoint": t.Config.SshEndpoint,
		"forward-host": t.Config.ForwardHost,
		"forward-port": t.Config.ForwardPort,
		"local-port":   t.Config.LocalPort,
	}).Info("starting network-tunnel")

	t.Cmd = exec.CommandContext(
		t.ctx,
		"flow-network-tunnel",
		"--ssh-endpoint", t.Config.SshEndpoint,
		"--private-key", tmpKeyFile.Name(),
		"--forward-host", t.Config.ForwardHost,
		"--forward-port", t.Config.ForwardPort,
		"--local-port", t.Config.LocalPort,
	)

	stdout, err := t.Cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("could not get stdout pipe for network-tunnel: %w", err)
	}

	t.Cmd.Start()
	// we want to read "READY" from stdout of tunnel
	var readyBuf = make([]byte, 5)
	var ready = []byte("READY")
	if _, err = io.ReadFull(stdout, readyBuf); err != nil {
		return fmt.Errorf("reading READY signal from network-tunnel: %w", err)
	} else if bytes.Compare(readyBuf, ready) != 0 {
		return fmt.Errorf("network-tunnel returned %v instead of READY", readyBuf)
	}

	log.Info("network-tunnel ready")

	return nil
}

func (t SshTunnel) Stop() {
	t.cancel()
}
