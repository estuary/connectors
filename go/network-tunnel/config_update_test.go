package networkTunnel

import (
	"bytes"
	"encoding/json"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	tunnelConfig          = `{"address":"db:5432","networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey":"key"}}}`
	sealedTunnelConfig    = `{"address":"db:5432","networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey_sops":"ENC[AES256_GCM,data:yy]"}},"sops":{"encrypted_suffix":"_sops","mac":"ENC[AES256_GCM,data:zz]"}}`
	sealedOverlayedConfig = `{"address":"db:5432","networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey_sops":"ENC[AES256_GCM,data:yy]"}},"sops":{"encrypted_suffix":"_sops","mac":"ENC[AES256_GCM,data:zz]","overlay":{"advanced":{"feature_flags":"no_gated"}}}}`
)

func TestRecordSkipHostKeyVerification(t *testing.T) {
	// The test reads configUpdate events from JSON log lines.
	var logs bytes.Buffer
	var logger = log.StandardLogger()
	var priorFormatter, priorOut = logger.Formatter, logger.Out
	logger.SetFormatter(&log.JSONFormatter{})
	logger.SetOutput(&logs)
	t.Cleanup(func() { logger.SetFormatter(priorFormatter); logger.SetOutput(priorOut) })

	// configUpdates returns the config of each configUpdate event in the logs.
	var configUpdates = func() []string {
		var out []string
		for _, line := range bytes.Split(logs.Bytes(), []byte("\n")) {
			var entry struct {
				EventType string          `json:"eventType"`
				Config    json.RawMessage `json:"config"`
			}
			if json.Unmarshal(line, &entry) == nil && entry.EventType == "configUpdate" {
				out = append(out, string(entry.Config))
			}
		}
		return out
	}

	var withTunnel = func(sshForwarding string) string {
		return `{"address":"db:5432","networkTunnel":{"sshForwarding":` + sshForwarding + `}}`
	}

	for _, tc := range []struct {
		name   string
		config string
	}{
		{name: "no tunnel", config: `{"address":"db:5432"}`},
		{name: "null tunnel", config: `{"address":"db:5432","networkTunnel":null}`},
		{name: "tunnel without ssh forwarding", config: `{"address":"db:5432","networkTunnel":{}}`},
		{name: "tunnel without an endpoint", config: withTunnel(`{"sshEndpoint":"","privateKey":""}`)},
		{name: "known hosts set", config: withTunnel(`{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey":"key","knownHosts":"` + testKnownHostsEntry + `"}`)},
		{name: "already skipping", config: withTunnel(`{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey":"key","skipHostKeyVerification":true}`)},
		{name: "explicitly not skipping", config: withTunnel(`{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey":"key","skipHostKeyVerification":false}`)},
		{name: "not an object", config: `[]`},
		{name: "unrelated networkTunnel shape", config: `{"networkTunnel":"none"}`},
	} {
		t.Run(tc.name+" emits no config update", func(t *testing.T) {
			logs.Reset()
			var got = RecordSkipHostKeyVerification(json.RawMessage(tc.config), json.RawMessage(sealedTunnelConfig))
			require.JSONEq(t, sealedTunnelConfig, string(got))
			require.Empty(t, logs.String())
		})
	}

	t.Run("whitespace known hosts gets the setting", func(t *testing.T) {
		logs.Reset()
		var config = withTunnel(`{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey":"key","knownHosts":" \n"}`)
		RecordSkipHostKeyVerification(json.RawMessage(config), json.RawMessage(sealedTunnelConfig))
		require.Len(t, configUpdates(), 1)
	})

	for _, tc := range []struct {
		name   string
		sealed string
		want   string
	}{
		{
			name:   "encrypted config gets the setting in the overlay",
			sealed: sealedTunnelConfig,
			want:   `{"address":"db:5432","networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey_sops":"ENC[AES256_GCM,data:yy]"}},"sops":{"encrypted_suffix":"_sops","mac":"ENC[AES256_GCM,data:zz]","overlay":{"networkTunnel":{"sshForwarding":{"skipHostKeyVerification":true}}}}}`,
		},
		{
			name:   "existing overlay is kept alongside the setting",
			sealed: sealedOverlayedConfig,
			want:   `{"address":"db:5432","networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey_sops":"ENC[AES256_GCM,data:yy]"}},"sops":{"encrypted_suffix":"_sops","mac":"ENC[AES256_GCM,data:zz]","overlay":{"advanced":{"feature_flags":"no_gated"},"networkTunnel":{"sshForwarding":{"skipHostKeyVerification":true}}}}}`,
		},
		{
			name:   "unencrypted config gets the setting directly",
			sealed: tunnelConfig,
			want:   `{"address":"db:5432","networkTunnel":{"sshForwarding":{"sshEndpoint":"ssh://user@bastion.example.com:2222","privateKey":"key","skipHostKeyVerification":true}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs.Reset()
			var got = RecordSkipHostKeyVerification(json.RawMessage(tunnelConfig), json.RawMessage(tc.sealed))
			require.JSONEq(t, tc.want, string(got), "returned config")
			var updates = configUpdates()
			require.Len(t, updates, 1)
			require.JSONEq(t, tc.want, updates[0], "emitted config")
		})
	}

	t.Run("missing sealed config logs a warning", func(t *testing.T) {
		logs.Reset()
		var got = RecordSkipHostKeyVerification(json.RawMessage(tunnelConfig), nil)
		require.Nil(t, got)
		require.Empty(t, configUpdates())
		require.Contains(t, logs.String(), "runtime did not provide the sealed endpoint config")
	})

	t.Run("unusable sealed config logs a warning", func(t *testing.T) {
		logs.Reset()
		var sealed = `{"networkTunnel":"not an object"}`
		var got = RecordSkipHostKeyVerification(json.RawMessage(tunnelConfig), json.RawMessage(sealed))
		require.JSONEq(t, sealed, string(got))
		require.Empty(t, configUpdates())
		require.Contains(t, logs.String(), "failed to set it in the sealed endpoint config")
	})
}
