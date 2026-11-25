package networkTunnel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
)

type SSHForwardingConfig struct {
	SSHEndpoint             string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey              string `json:"privateKey" jsonschema:"title=SSH Private Key" jsonschema_extras:"secret=true,multiline=true"`
	KnownHosts              string `json:"knownHosts,omitempty" jsonschema:"title=SSH Known Hosts" jsonschema_extras:"multiline=true"`
	SkipHostKeyVerification bool   `json:"skipHostKeyVerification,omitempty" jsonschema:"title=Skip Host Key Verification" jsonschema_extras:"nonsensitive=true"`
}

func (SSHForwardingConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "SSHEndpoint":
		return "Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])"
	case "PrivateKey":
		return "Private key to connect to the remote SSH server."
	case "KnownHosts":
		return "Host keys of the SSH server in OpenSSH known_hosts format, one per line: the output of `ssh-keyscan -p 2222 bastion.example.com`, for example `[bastion.example.com]:2222 ssh-ed25519 AAAA...`."
	case "SkipHostKeyVerification":
		return "Connect without verifying the SSH server's host key. This leaves the task open to man-in-the-middle attacks."
	default:
		return ""
	}
}

var errMissingKnownHosts = errors.New("missing SSH Known Hosts: paste the host keys of the SSH server, as printed by `ssh-keyscan -p <port> <host>`, or set Skip Host Key Verification to connect without verifying the server")

func (cfg *SSHForwardingConfig) validate() error {
	if err := validateKnownHosts(cfg.KnownHosts); err != nil {
		return err
	}
	var hasKnownHosts = strings.TrimSpace(cfg.KnownHosts) != ""
	if hasKnownHosts && cfg.SkipHostKeyVerification {
		return errors.New("both Skip Host Key Verification and SSH Known Hosts are set. Keep SSH Known Hosts to verify the SSH server, or clear it and keep Skip Host Key Verification to connect without verification, but not both")
	} else if !hasKnownHosts && !cfg.SkipHostKeyVerification {
		return errMissingKnownHosts
	}
	return nil
}

// validateKnownHosts checks that every line of the SSH Known Hosts field is a
// known_hosts entry, a comment, or blank. Empty content is valid and means
// there are no pinned keys.
func validateKnownHosts(content string) error {
	if strings.TrimSpace(content) == "" {
		return nil
	}

	var entries int
	for i, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// ssh.ParseKnownHosts allows a single trailing comment word where
		// OpenSSH allows any number, so only the entry's own columns are handed
		// to it.
		fields := strings.Fields(line)
		columns := 3
		if strings.HasPrefix(fields[0], "@") {
			columns = 4
		}
		if len(fields) > columns {
			fields = fields[:columns]
		}

		if _, _, _, _, _, err := ssh.ParseKnownHosts([]byte(strings.Join(fields, " "))); err != nil {
			return fmt.Errorf("SSH Known Hosts line %d: not a known_hosts entry ('[host]:port key-type base64-key', as printed by ssh-keyscan): %w", i+1, err)
		}
		entries++
	}

	if entries == 0 {
		return errors.New("SSH Known Hosts has no host key entries, only comments or blank lines. Paste the output of ssh-keyscan")
	}
	return nil
}

type TunnelConfig struct {
	SSHForwarding *SSHForwardingConfig `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

func (cfg *TunnelConfig) InUse() bool {
	return cfg != nil && cfg.SSHForwarding != nil && cfg.SSHForwarding.SSHEndpoint != ""
}

func (cfg *TunnelConfig) Start(ctx context.Context, address string, localPort string) (*SshTunnel, error) {
	// Connectors start the tunnel while validating a publication, so checking
	// the fields here makes an invalid tunnel config fail the publication.
	if err := cfg.SSHForwarding.validate(); err != nil {
		return nil, err
	}
	var knownHosts = cfg.SSHForwarding.KnownHosts
	if strings.TrimSpace(knownHosts) == "" {
		knownHosts = ""
		log.WithField("ssh-endpoint", cfg.SSHForwarding.SSHEndpoint).Warn("Skip Host Key Verification is enabled, so the SSH server's host key isn't verified.")
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("error splitting address %q into host and port: %w", address, err)
	}

	var sshConfig = &SshConfig{
		SshEndpoint: cfg.SSHForwarding.SSHEndpoint,
		PrivateKey:  []byte(cfg.SSHForwarding.PrivateKey),
		ForwardHost: host,
		ForwardPort: port,
		LocalPort:   localPort,
		KnownHosts:  knownHosts,
	}
	var tunnel = sshConfig.CreateTunnel()

	if err := tunnel.Start(); err != nil {
		return nil, err
	}
	return tunnel, nil
}
