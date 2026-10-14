package connector

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// composeStack is one docker-compose file and the lazily-started state of its
// services, so a test file brings the stack up at most once per `go test` run.
type composeStack struct {
	file string
	once sync.Once
	up   bool
}

var (
	// singleNode is a single ClickHouse server.
	singleNode = &composeStack{file: "docker-compose.yaml"}
	// cluster is two ClickHouse shards of two replicas each plus one Keeper.
	cluster = &composeStack{file: "docker-compose.cluster.yaml"}
)

// ensureUp starts the stack and waits until the given address accepts Go
// client connections. Tests that need a running database must call this after
// their testing.Short() check. The docker-compose healthcheck uses
// clickhouse-client, which can succeed before the native TCP endpoint is ready
// to handshake with ch-go, so we poll with the real client before returning.
func (s *composeStack) ensureUp(t *testing.T, cfg config) {
	t.Helper()
	s.once.Do(func() {
		out, err := exec.Command("docker", "compose", "-f", s.file, "up", "--wait").CombinedOutput()
		if err != nil {
			t.Fatalf("docker compose up failed: %s\n%s", err, out)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var lastErr error
		for ctx.Err() == nil {
			conn, err := clickhouse.Open(cfg.newClickhouseOptions())
			if err == nil {
				if err = conn.Ping(ctx); err == nil {
					conn.Close()
					s.up = true
					return
				}
				conn.Close()
			}
			lastErr = err
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("clickhouse not accepting connections: %s", lastErr)
	})
	if !s.up {
		t.Fatal("docker compose setup previously failed")
	}
}

func (s *composeStack) down() {
	if s.up {
		exec.Command("docker", "compose", "-f", s.file, "down", "-v").Run()
	}
}

func ensureDockerUp(t *testing.T) {
	t.Helper()
	singleNode.ensureUp(t, testConfig())
}

func ensureClusterUp(t *testing.T) {
	t.Helper()
	cluster.ensureUp(t, testClusterConfig())
}

func TestMain(m *testing.M) {
	code := m.Run()
	singleNode.down()
	cluster.down()
	os.Exit(code)
}
