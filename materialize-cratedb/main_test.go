package main

import (
	"context"
	stdsql "database/sql"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var (
	dockerOnce sync.Once
	dockerUp   bool
)

// ensureDockerUp starts the CrateDB docker-compose services and waits until
// the server accepts pgx client connections. Tests that need a running
// database must call this after their testing.Short() check. The
// docker-compose healthcheck hits CrateDB's HTTP endpoint, which becomes
// ready slightly before the psql wire endpoint, so we also poll with a real
// pgx query before returning.
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
			db, err := stdsql.Open("pgx", testConfig().ToURI())
			if err == nil {
				if _, err = db.ExecContext(ctx, "SELECT 1"); err == nil {
					db.Close()
					dockerUp = true
					return
				}
				db.Close()
			}
			lastErr = err
			time.Sleep(250 * time.Millisecond)
		}
		t.Fatalf("cratedb not accepting connections: %s", lastErr)
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
