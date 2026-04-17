package main

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

var (
	dockerOnce sync.Once
	dockerUp   bool
)

// ensureDockerUp starts the ClickHouse docker-compose services and waits until
// the server accepts Go client connections. Tests that need a running database
// must call this after their testing.Short() check. The docker-compose
// healthcheck uses clickhouse-client, which can succeed before the native TCP
// endpoint is ready to handshake with ch-go, so we poll with the real client
// before returning.
func ensureDockerUp(t *testing.T) {
	t.Helper()
	dockerOnce.Do(func() {
		out, err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").CombinedOutput()
		if err != nil {
			t.Fatalf("docker compose up failed: %s\n%s", err, out)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var lastErr error
		for ctx.Err() == nil {
			conn, err := clickhouse.Open(testConfig().newClickhouseOptions())
			if err == nil {
				if err = conn.Ping(ctx); err == nil {
					conn.Close()
					dockerUp = true
					return
				}
				conn.Close()
			}
			lastErr = err
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("clickhouse not accepting connections: %s", lastErr)
	})
	if !dockerUp {
		t.Fatal("docker compose setup previously failed")
	}
}

func TestMain(m *testing.M) {
	code := m.Run()
	if dockerUp {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	}
	os.Exit(code)
}
