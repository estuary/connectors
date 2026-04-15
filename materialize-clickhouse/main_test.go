package main

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestMain(m *testing.M) {
	// Start ClickHouse via docker compose for non-short test runs.
	if !isShortMode() {
		if err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run(); err != nil {
			panic("starting docker compose: " + err.Error())
		}
	}

	exitCode := m.Run()
	if !isShortMode() {
		_ = exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	}
	os.Exit(exitCode)
}

// isShortMode checks os.Args for the -test.short flag since flag.Parse
// hasn't been called yet inside TestMain.
func isShortMode() bool {
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "-test.short") || arg == "-short" {
			return true
		}
	}
	return false
}
