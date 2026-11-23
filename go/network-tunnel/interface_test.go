package networkTunnel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testHostKey         = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHZY1HmgcqLdq74a5R6H/B/LzuFL0GXP1KbX9kH4O8wa"
	testKnownHostsEntry = "[bastion.example.com]:2222 " + testHostKey
)

func TestValidateKnownHosts(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		wantErr string
	}{
		{name: "empty", content: ""},
		{name: "whitespace", content: " \n\t\n"},
		{name: "entry", content: testKnownHostsEntry + "\n"},
		{name: "entry with comments and blank lines", content: "# bastion\n\n" + testKnownHostsEntry + "\n"},
		{name: "trailing comment words", content: testKnownHostsEntry + " added by ops"},
		{name: "hashed host", content: "|1|c+y45sb583dDknzckooiyPxlxAw=|ek+m18jk2m0fliwdpMBWQpOK0nY= " + testHostKey},
		{name: "cert authority", content: "@cert-authority *.example.com " + testHostKey},
		{name: "only comments", content: "# bastion\n# nothing else\n", wantErr: "has no host key entries"},
		{name: "not an entry", content: testKnownHostsEntry + "\nbastion.example.com\n", wantErr: "SSH Known Hosts line 2: not a known_hosts entry"},
		{name: "bad key", content: "[bastion.example.com]:2222 ssh-ed25519 notbase64", wantErr: "SSH Known Hosts line 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err = validateKnownHosts(tc.content)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestValidateHostKeyVerification(t *testing.T) {
	for _, tc := range []struct {
		name       string
		knownHosts string
		skip       bool
		wantErr    string
	}{
		{name: "known hosts", knownHosts: testKnownHostsEntry},
		{name: "skip", skip: true},
		{name: "skip with whitespace known hosts", knownHosts: " \n", skip: true},
		{name: "neither", wantErr: "missing SSH Known Hosts"},
		{name: "whitespace known hosts", knownHosts: " \n", wantErr: "missing SSH Known Hosts"},
		{name: "both", knownHosts: testKnownHostsEntry, skip: true, wantErr: "both Skip Host Key Verification and SSH Known Hosts are set"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cfg = SSHForwardingConfig{KnownHosts: tc.knownHosts, SkipHostKeyVerification: tc.skip}
			var err = cfg.validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestTunnelConfigStartKnownHosts(t *testing.T) {
	var tunnelConfig = func(knownHosts string, skip bool) *TunnelConfig {
		return &TunnelConfig{SSHForwarding: &SSHForwardingConfig{
			SSHEndpoint:             "ssh://user@bastion.example.com:2222",
			PrivateKey:              "private key",
			KnownHosts:              knownHosts,
			SkipHostKeyVerification: skip,
		}}
	}

	t.Run("passed to the tunnel", func(t *testing.T) {
		var dir = fakeTunnelBinary(t)
		tunnel, err := tunnelConfig(testKnownHostsEntry, false).Start(context.Background(), "db.internal:5432", "5432")
		require.NoError(t, err)
		t.Cleanup(tunnel.Stop)

		require.Contains(t, readArgs(t, dir), "--known-hosts")
	})

	t.Run("whitespace isn't passed", func(t *testing.T) {
		var dir = fakeTunnelBinary(t)
		tunnel, err := tunnelConfig(" \n", true).Start(context.Background(), "db.internal:5432", "5432")
		require.NoError(t, err)
		t.Cleanup(tunnel.Stop)

		require.NotContains(t, readArgs(t, dir), "--known-hosts")
	})

	t.Run("invalid fails before the tunnel starts", func(t *testing.T) {
		var dir = fakeTunnelBinary(t)
		_, err := tunnelConfig("bastion.example.com", false).Start(context.Background(), "db.internal:5432", "5432")
		require.ErrorContains(t, err, "not a known_hosts entry")

		_, statErr := os.Stat(filepath.Join(dir, "args"))
		require.True(t, os.IsNotExist(statErr), "the tunnel shouldn't have started")
	})
}
