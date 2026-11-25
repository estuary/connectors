package networkTunnel

import (
	"encoding/json"
	"strings"

	"github.com/estuary/connectors/go/common"
	log "github.com/sirupsen/logrus"
)

// skipHostKeyVerificationPath is the path of `skipHostKeyVerification` in the
// endpoint config.
var skipHostKeyVerificationPath = []string{"networkTunnel", "sshForwarding", "skipHostKeyVerification"}

// RecordSkipHostKeyVerification emits a configUpdate event that sets
// `skipHostKeyVerification: true` in the task's endpoint config. It does so
// only when the decrypted config has an SSH tunnel with a blank `knownHosts`
// and no `skipHostKeyVerification` key, so it leaves an explicit `false`
// alone.
//
// It returns the sealed config the event carries, or sealedConfig unchanged if
// it emitted nothing. Each configUpdate replaces the whole endpoint config, so
// any later configUpdate during the same Open must start from the returned
// config. It never returns an error, and logs a warning when it can't build
// the update.
//
// This function is temporary and will be removed once estuary/connectors#5916
// requires every tunnel to set `knownHosts` or `skipHostKeyVerification`.
func RecordSkipHostKeyVerification(config, sealedConfig json.RawMessage) json.RawMessage {
	var parsed struct {
		NetworkTunnel *struct {
			SSHForwarding *struct {
				SSHEndpoint string `json:"sshEndpoint"`
				KnownHosts  string `json:"knownHosts"`
				// A pointer, so that an explicit `false` counts as set.
				SkipHostKeyVerification *bool `json:"skipHostKeyVerification"`
			} `json:"sshForwarding"`
		} `json:"networkTunnel"`
	}
	if err := json.Unmarshal(config, &parsed); err != nil {
		// A config that doesn't parse into this shape belongs to a connector
		// without a tunnel. Connectors with a tunnel reject such a config when
		// it's published.
		return sealedConfig
	}

	// A tunnel without an endpoint isn't in use, as in TunnelConfig.InUse.
	if parsed.NetworkTunnel == nil || parsed.NetworkTunnel.SSHForwarding == nil {
		return sealedConfig
	}
	var fwd = parsed.NetworkTunnel.SSHForwarding
	if fwd.SSHEndpoint == "" ||
		strings.TrimSpace(fwd.KnownHosts) != "" ||
		fwd.SkipHostKeyVerification != nil {
		return sealedConfig
	}

	var logEntry = log.WithField("ssh-endpoint", fwd.SSHEndpoint)
	if len(sealedConfig) == 0 {
		// Runtimes older than `sealed_config_json` don't send the sealed config,
		// and an encrypted config can't be updated without it.
		logEntry.Warn("not recording skipHostKeyVerification in the endpoint config: runtime did not provide the sealed endpoint config")
		return sealedConfig
	}

	updated, err := common.SetSealedConfigProperty(sealedConfig, skipHostKeyVerificationPath, true)
	if err != nil {
		logEntry.WithError(err).Warn("not recording skipHostKeyVerification in the endpoint config: failed to set it in the sealed endpoint config")
		return sealedConfig
	}

	common.EmitConfigUpdate("Recording that the network tunnel connects without verifying the SSH server's host key.", updated)
	return updated
}
