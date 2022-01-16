package networkproxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	sf "github.com/estuary/connectors/network-proxy-service/sshforwarding"
)

const ProgramName = "network-proxy-service"

func SupportedNetworkProxyTypes() []string {
	return []string{"ssh_forwarding"}
}

type NetworkProxyConfig struct {
	ProxyType           string                 `json:"proxy_type"`
	SshForwardingConfig sf.SshForwardingConfig `json:"ssh_forwarding"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (NetworkProxyConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "ProxyType":
		return fmt.Sprintf("The type of the network proxy. Supported types are: ( %s )", strings.Join(SupportedNetworkProxyTypes(), ", "))
	case "SshForwardingConfig":
		return "Config for proxy of type ssh_forwarding"
	default:
		return ""
	}
}

func (npc *NetworkProxyConfig) Validate() error {
	if npc == nil {
		return nil
	}

	var supported = false
	for _, t := range SupportedNetworkProxyTypes() {
		if t == npc.ProxyType {
			supported = true
			break
		}
	}

	if !supported {
		return fmt.Errorf("Unsupported proxy type: %s. Valid values are: ( %s ).", npc.ProxyType, strings.Join(SupportedNetworkProxyTypes(), ", "))
	}

	switch npc.ProxyType {
	case "ssh_forwarding":
		return npc.SshForwardingConfig.Validate()
	default:
		panic(fmt.Sprintf("Implementation of validating %s is not ready.", npc.ProxyType))
	}
}

func (npc *NetworkProxyConfig) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	switch npc.ProxyType {
	case "ssh_forwarding":
		m[npc.ProxyType] = npc.SshForwardingConfig
	default:
		panic(fmt.Sprintf("Implementation of MarshalJSON for %s is missing.", npc.ProxyType))
	}

	return json.Marshal(m)
}

const defaultTimeoutSecs = 5

func (npc *NetworkProxyConfig) Start() error {
	return npc.startWithTimeout(defaultTimeoutSecs)
}

func (npc *NetworkProxyConfig) startWithTimeout(timeoutSecs uint16) error {
	if npc == nil {
		// NetworkProxyConfig is not set.
		return nil
	}

	var cmd = exec.Command(ProgramName)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	var readyCh = make(chan error)
	cmd.Stdout = &readyWriter{delegate: os.Stdout, ch: readyCh}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := npc.sendInput(cmd); err != nil {
		return fmt.Errorf("sending input to service: %w", err)
	} else if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting ssh forwarding service: %w", err)
	}

	select {
	case err := <-readyCh:
		if err != nil {
			return fmt.Errorf(
				"network proxy service error: %w. stderr: %s",
				err, stderr.String(),
			)
		}
		return nil

	case <-time.After(time.Duration(timeoutSecs) * time.Second):
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
		}
		return fmt.Errorf(
			"network proxy service failed to be ready after waiting for long enough. stderr: %s",
			stderr.String(),
		)
	}
}

func (npc *NetworkProxyConfig) sendInput(cmd *exec.Cmd) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("getting stdin pipe: %w", err)
	}

	input, err := json.Marshal(npc)

	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	go func() {
		stdin.Write(input)
		stdin.Close()
	}()

	return nil
}

type readyWriter struct {
	delegate io.Writer
	ch       chan error
}

func (w *readyWriter) Write(p []byte) (int, error) {
	if w.ch == nil {
		return w.delegate.Write(p) // Common case.
	}

	defer func() {
		close(w.ch)
		w.ch = nil
	}()

	if bytes.HasPrefix(p, []byte("READY\n")) {
		var n, err = w.delegate.Write(p[6:])
		n += 6
		return n, err
	} else {
		w.ch <- fmt.Errorf("did not read READY from subprocess")
		return w.delegate.Write(p)
	}
}
