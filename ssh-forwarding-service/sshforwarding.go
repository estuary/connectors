package sshforwarding

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"syscall"
	"time"
)

type SshForwardingConfig struct {
	SshEndpoint         string `json:"ssh_endpoint" jsonschema:"description=Endpoint of the remote SSH server that supports tunneling, in the form of ssh://hostname[:port]"`
	SshPrivateKeyBase64 string `json:"ssh_private_key_base64" jsonschema:"description=Base64-encoded private Key to connect to the remote SSH server."`
	SshUser             string `json:"ssh_user,omitempty" jsonschema:"description=User name to connect to the remote SSH server."`
	RemoteHost          string `json:"remote_host" jsonschema:"description=Host name to connect from the remote SSH server to the remote destination (e.g. DB) via internal network."`
	RemotePort          uint16 `json:"remote_port,omitempty" jsonschema:"description=Port of the remote destination."`
}

const ProgramName = "ssh-forwarding-service"

type forwardingServiceInput struct {
	SshForwardingConfig  SshForwardingConfig `json:"ssh_forwarding_config"`
	LocalPort            uint16              `json:"local_port"`
	MaxPollingRetryTimes uint16              `json:"max_polling_retry_times"`
}

type forwardingServideOutput struct {
	DeployedLocalPort uint16 `json:"deployed_local_port"`
}

func (sfc *SshForwardingConfig) Validate() error {
	if sfc == nil {
		return nil
	}

	if sfc.SshEndpoint == "" {
		return fmt.Errorf("missing ssh_endpoint")
	}

	if sfc.RemoteHost == "" {
		return fmt.Errorf("missing remote_host")
	}

	if sfc.SshPrivateKeyBase64 == "" {
		return fmt.Errorf("missing ssh_private_key_base64")
	}

	return nil
}

const defaultTimeoutSecs = 5

func (sfc *SshForwardingConfig) Start(suggestedLocalPort uint16) (uint16, error) {
	return sfc.startWithTimeout(suggestedLocalPort, defaultTimeoutSecs)
}

func (sfc *SshForwardingConfig) startWithTimeout(suggestedLocalPort uint16, timeoutSecs uint16) (uint16, error) {
	if sfc == nil {
		// SshForwardingConfig is not set.
		return suggestedLocalPort, nil
	}

	var cmd = exec.Command(ProgramName)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	var readyCh = make(chan *forwardingServideOutput)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdoutWriter{delegate: stdout, ch: readyCh}
	cmd.Stderr = &stderr

	if err := sfc.sendInput(cmd, suggestedLocalPort); err != nil {
		return 0, fmt.Errorf("sending input to service: %w", err)
	} else if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("starting ssh forwarding service: %w", err)
	}

	select {
	case output := <-readyCh:
		if output == nil {
			return 0, fmt.Errorf(
				"ssh forwarding service returns invalid outputs. stderr: %s",
				stderr.String(),
			)
		}

		return output.DeployedLocalPort, nil
	case <-time.After(time.Duration(timeoutSecs) * time.Second):
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
		}
		return 0, fmt.Errorf(
			"ssh forwarding service failed to be ready after waiting for long enough. stderr: %s",
			stderr.String(),
		)
	}
}

func (sfc *SshForwardingConfig) sendInput(cmd *exec.Cmd, localPort uint16) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("getting stdin pipe: %w", err)
	}

	input, err := json.Marshal(forwardingServiceInput{
		SshForwardingConfig: *sfc,
		LocalPort:           localPort,
	})

	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	go func() {
		stdin.Write(input)
		stdin.Close()
	}()

	return nil
}

type stdoutWriter struct {
	delegate bytes.Buffer
	ch       chan *forwardingServideOutput
}

func (w *stdoutWriter) Write(p []byte) (int, error) {
	if w.ch == nil {
		panic("write to stdout after receiving ending marker")
	}

	if !bytes.HasSuffix(p, []byte{0}) {
		return w.delegate.Write(p)
	}

	var n, err = w.delegate.Write(p[:len(p)-1])
	if n < len(p)-1 || err != nil {
		return n, err
	}

	var output forwardingServideOutput
	if unmarshalErr := json.Unmarshal(w.delegate.Bytes(), &output); unmarshalErr == nil {
		w.ch <- &output
	}
	close(w.ch)
	w.ch = nil

	return len(p), nil
}
