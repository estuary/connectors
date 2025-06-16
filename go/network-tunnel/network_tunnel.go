package networkTunnel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"

	cerrors "github.com/estuary/connectors/go/connector-errors"
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
	Config      *SshConfig
	ctx         context.Context
	Cmd         *exec.Cmd
	tmpFileName string
	cancel      context.CancelFunc
}

func (c *SshConfig) CreateTunnel() *SshTunnel {
	var ctx, cancel = context.WithCancel(context.Background())
	return &SshTunnel{
		Config:      c,
		ctx:         ctx,
		cancel:      cancel,
		tmpFileName: "",
		Cmd:         nil,
	}
}

// Start tunnel and wait until READY signal
func (t *SshTunnel) Start() error {
	if t.Cmd != nil {
		return errors.New("This tunnel has already been started.")
	}

	var tmpKeyFile, err = os.CreateTemp("", "flow-network-tunnel")
	if err != nil {
		return fmt.Errorf("creating temporary file for private key: %w", err)
	}
	t.tmpFileName = tmpKeyFile.Name()

	if _, err := tmpKeyFile.Write(t.Config.PrivateKey); err != nil {
		return fmt.Errorf("writing key to temporary file: %w", err)
	}

	log.WithFields(log.Fields{
		"ssh-endpoint": t.Config.SshEndpoint,
		"forward-host": t.Config.ForwardHost,
		"forward-port": t.Config.ForwardPort,
		"local-port":   t.Config.LocalPort,
	}).Info("starting network-tunnel")

	logLevel := log.GetLevel().String()
	if logLevel == "warning" {
		// Adapt the logrus level to a compatible `env_filter` level of the
		// network-tunnel `tracing_subscriber`.
		logLevel = "warn"
	}

	t.Cmd = exec.CommandContext(
		t.ctx,
		"flow-network-tunnel",
		"ssh",
		"--ssh-endpoint", t.Config.SshEndpoint,
		"--private-key", tmpKeyFile.Name(),
		"--forward-host", t.Config.ForwardHost,
		"--forward-port", t.Config.ForwardPort,
		"--local-port", t.Config.LocalPort,
		"--log.level", logLevel,
	)
	// Assign process gid to this process and its children
	// so we can kill them together in the end
	t.Cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdout, err := t.Cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("could not get stdout pipe for network-tunnel: %w", err)
	}

	stderr, err := t.Cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("could not get stderr pipe for network-tunnel: %w", err)
	}
	go func() {
		// Copy log lines output from the network tunnel process to the
		// connector's stderr.
		_, err = io.Copy(os.Stderr, stderr)
		if err != nil {
			log.WithField("error", err).Info("error copying stderr of tunnel")
		}
	}()

	if err := t.Cmd.Start(); err != nil {
		return fmt.Errorf("starting network-tunnel: %w", err)
	}
	// we want to read "READY" from stdout of tunnel
	var readyBuf = make([]byte, 5)
	var ready = []byte("READY")
	if _, err = io.ReadFull(stdout, readyBuf); err != nil {
		// EOF means the underlying process exited without writing READY, this is most likely
		// an error that we would like to surface to the user without logging anything further
		// ourselves
		if err == io.EOF {
			return cerrors.NewTransparentError(err)
		}
		return fmt.Errorf("reading READY signal from network-tunnel: %w", err)
	} else if !bytes.Equal(readyBuf, ready) {
		return fmt.Errorf("network-tunnel returned %v instead of READY", readyBuf)
	}

	log.Info("network-tunnel ready")

	go func() {
		// The network tunnel should never stop once started. If it does, the
		// connector should crash and attempt to log the final error message.
		// It's not unlikely that some other error will occur within the
		// connector's processing that will race this log, but we do the best we
		// can.
		if err = t.Cmd.Wait(); err != nil {
			log.WithError(err).Fatal("network-tunnel failed")
		} else {
			log.Fatal("network-tunnel exited")
		}
	}()

	return nil
}

func (t *SshTunnel) Stop() {
	if t == nil {
		return
	}
	if t.Cmd != nil {
		// Using the negative pid signals kill to a process group.
		// This ensures the children of the process are also killed
		syscall.Kill(-t.Cmd.Process.Pid, syscall.SIGKILL)
	}
	t.cancel()
	// cleanup key file
	if t.tmpFileName != "" {
		os.Remove(t.tmpFileName)
	}
}
