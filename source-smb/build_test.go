package main

import (
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIntegrationEnabledInBuild(t *testing.T) {
	// Use CI's builder-stage extraction to check the environment passed to go test.
	var script = `
set -euo pipefail
unset TEST_DATABASE
go() {
  if [ "$*" = 'test -v ./source-smb/...' ]; then
    printf '%s' "${TEST_DATABASE:-}"
  fi
}
eval "$(sed -n '/ AS builder$/,/^FROM /p' Dockerfile |
  sed -nE -e 's/^(ARG|ENV) /export /p' \
          -e 's/^(RUN)? +(go (test|build) )/\2/p')"
`
	output, err := exec.Command("bash", "-c", script).CombinedOutput()
	require.NoError(t, err, string(output))
	require.Equal(t, "yes", string(output), "CI must enable TestIntegration")
}
