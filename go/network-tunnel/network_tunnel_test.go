package networkTunnel

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Stopping a tunnel makes Start's watcher goroutine log a fatal error,
	// sometimes after the test that stopped it has finished, so a fatal log
	// mustn't exit the test binary.
	log.StandardLogger().ExitFunc = func(int) {}
	os.Exit(m.Run())
}

// fakeTunnelBinary puts a flow-network-tunnel on PATH that records its
// arguments, and the contents of any --known-hosts file, in the returned dir.
func fakeTunnelBinary(t *testing.T) string {
	t.Helper()
	var dir = t.TempDir()
	var script = `#!/bin/sh
printf '%s\n' "$@" > "` + dir + `/args"
while [ $# -gt 0 ]; do
	if [ "$1" = "--known-hosts" ]; then cat "$2" > "` + dir + `/known_hosts"; fi
	shift
done
printf READY
exec sleep 60
`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "flow-network-tunnel"), []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

func readArgs(t *testing.T, dir string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "args"))
	require.NoError(t, err)
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

func TestStartPassesKnownHosts(t *testing.T) {
	var sshConfig = func(knownHosts string) *SshConfig {
		return &SshConfig{
			SshEndpoint: "ssh://user@bastion.example.com:2222",
			PrivateKey:  []byte("private key"),
			ForwardHost: "db.internal",
			ForwardPort: "5432",
			LocalPort:   "5432",
			KnownHosts:  knownHosts,
		}
	}

	t.Run("set", func(t *testing.T) {
		var dir = fakeTunnelBinary(t)
		var knownHosts = "[bastion.example.com]:2222 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBKE6oC7wV2dS6Lk3dYbpG4q5TnxoKn4WbGWgM2k3vZ1\n"
		var tunnel = sshConfig(knownHosts).CreateTunnel()
		require.NoError(t, tunnel.Start())
		t.Cleanup(tunnel.Stop)

		var args = readArgs(t, dir)
		var i = slices.Index(args, "--known-hosts")
		require.NotEqual(t, -1, i, "args: %q", args)
		passed, err := os.ReadFile(filepath.Join(dir, "known_hosts"))
		require.NoError(t, err)
		require.Equal(t, knownHosts, string(passed))

		tunnel.Stop()
		_, err = os.Stat(args[i+1])
		require.True(t, os.IsNotExist(err), "Stop should remove the known hosts file")
	})

	t.Run("unset", func(t *testing.T) {
		var dir = fakeTunnelBinary(t)
		var tunnel = sshConfig("").CreateTunnel()
		require.NoError(t, tunnel.Start())
		t.Cleanup(tunnel.Stop)

		require.NotContains(t, readArgs(t, dir), "--known-hosts")
	})
}
