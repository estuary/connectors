package networktunnel

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

	sf "github.com/estuary/connectors/network-tunnel-service/sshforwarding"
)

const ProgramName = "flow-network-tunnel"

func SupportedNetworkTunnelTypes() []string {
	return []string{"sshForwarding"}
}

type NetworkTunnelConfig struct {
	SshForwardingConfig *sf.SshForwardingConfig `json:"sshForwarding"`
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (NetworkTunnelConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "SshForwardingConfig":
		return "Config for tunnel of type sshForwarding"
	default:
		return ""
	}
}

func (npc *NetworkTunnelConfig) Validate() error {
	if npc == nil {
		return nil
	}

	if npc.SshForwardingConfig != nil {
		return npc.SshForwardingConfig.Validate()
	} else {
		panic(fmt.Sprintf("You must provide at least one tunnel configuration. Supported tunnel configurations: %s", strings.Join(SupportedNetworkTunnelTypes(), ", ")))
	}
}

func (npc *NetworkTunnelConfig) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	if npc.SshForwardingConfig != nil {
		m["sshForwarding"] = npc.SshForwardingConfig
	} else {
		panic(fmt.Sprintf("Could not find any tunnel configuration to marshal. Supported tunnel configurations: %s", strings.Join(SupportedNetworkTunnelTypes(), ", ")))
	}

	return json.Marshal(m)
}

const defaultTimeoutSecs = 5

func (npc *NetworkTunnelConfig) Start() error {
	return npc.startInternal(defaultTimeoutSecs, os.Stderr)
}

func (npc *NetworkTunnelConfig) startInternal(timeoutSecs uint16, stderr io.Writer) error {
	if npc == nil {
		// NetworkTunnelConfig is not set.
		return nil
	}

	var cmd = exec.Command(ProgramName)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGTERM}

	var readyCh = make(chan error)
	cmd.Stdout = &readyWriter{delegate: os.Stdout, ch: readyCh}
	cmd.Stderr = stderr

	if err := npc.sendInput(cmd); err != nil {
		return fmt.Errorf("sending input to service: %w", err)
	} else if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting ssh forwarding service: %w", err)
	}

	select {
	case err := <-readyCh:
		if err != nil {
			return fmt.Errorf(
				"network tunnel service error: %w",
				err,
			)
		}
		return nil

	case <-time.After(time.Duration(timeoutSecs) * time.Second):
		if cmd.Process != nil {
			cmd.Process.Signal(syscall.SIGTERM)
		}
		return fmt.Errorf("network tunnel service failed to be ready after waiting for long enough")
	}
}

func (npc *NetworkTunnelConfig) sendInput(cmd *exec.Cmd) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("getting stdin pipe: %w", err)
	}

	input, err := json.Marshal(npc)

	if err != nil {
		return fmt.Errorf("marshal input: %w", err)
	}

	go func() {
		if n, err := stdin.Write(input); err != nil {
			panic(fmt.Errorf("Failed to send input to network-tunnel-service binary. %d, %w", n, err))
		}
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
