package main

import (
	"os/exec"
	"testing"

	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/assert"
)

// How to update the snapshots:
// UPDATE_SNAPS=true go test ./...

func TestCapture(t *testing.T) {
	t.Skip("Skipping test because flowctl is not supporting go connectors yet.")
	command := "flowctl"

	args := []string{
		"preview",
		"--source",
		"test.flow.yaml",
		"--sessions",
		"1",
		"--delay",
		"10s",
	}

	cmd := exec.Command(command, args...)
	stdout, err := cmd.Output()
	assert.NoError(t, err)

	snaps.MatchSnapshot(t, string(stdout))
}

func TestDiscover(t *testing.T) {
	t.Skip("Skipping test because flowctl is not supporting go connectors yet.")
	command := "flowctl"

	args := []string{
		"raw",
		"discover",
		"--source",
		"test.flow.yaml",
		"-o",
		"json",
		"--emit-raw",
	}

	cmd := exec.Command(command, args...)
	stdout, err := cmd.Output()
	assert.NoError(t, err)

	snaps.MatchJSON(t, string(stdout))
}

func TestSpec(t *testing.T) {
	t.Skip("Skipping test because flowctl is not supporting go connectors yet.")
	command := "flowctl"

	args := []string{
		"raw",
		"spec",
		"--source",
		"test.flow.yaml",
	}

	cmd := exec.Command(command, args...)
	stdout, err := cmd.Output()
	assert.NoError(t, err)

	snaps.MatchJSON(t, string(stdout))
}
