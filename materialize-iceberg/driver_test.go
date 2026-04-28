package connector

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/segmentio/encoding/json"
)

// testAll controls whether integration tests run against every catalog
// configuration (rest-local plus the cloud-based REST/Polaris-managed, Glue,
// and S3 Tables variants) or only against the local docker-compose stack.
// Running the full matrix is slow and requires SOPS decryption keys plus
// live AWS credentials, so by default we only exercise the local stack.
// Opt in to the full matrix by setting the `ICEBERG_TEST_ALL` environment
// variable or passing `-iceberg.test-all` to `go test`.
var testAll = flag.Bool("iceberg.test-all", false, "run integration tests against all catalog configurations (rest-local plus REST/Glue/S3 Tables) instead of the local stack only")

const (
	composeFile    = "docker-compose.yaml"
	composeProject = "materialize-iceberg"
	credsPath      = "testdata/.local/polaris-creds.json"
	configTemplate = "testdata/config.rest-local.yaml"
	localConfig    = "testdata/.local/config.rest-local.yaml"
)

var (
	dockerOnce sync.Once
	dockerUp   bool
)

func ensureDockerUp(t *testing.T) {
	t.Helper()
	dockerOnce.Do(func() {
		if err := composeUpAndBootstrap(); err != nil {
			t.Logf("docker compose setup failed: %v", err)
			return
		}
	})
	if !dockerUp {
		t.Fatal("docker compose setup previously failed")
	}
}

func composeUpAndBootstrap() error {
	if err := os.MkdirAll(filepath.Dir(localConfig), 0o755); err != nil {
		return fmt.Errorf("creating .local dir: %w", err)
	}

	// Ensure a clean slate: any leftover compose state plus a stale credentials
	// file from a prior run would silently produce credentials that don't match
	// the current Polaris instance. Tearing down explicitly is cheap relative
	// to debugging an "unauthorized_client" failure later.
	exec.Command("docker", "compose", "-f", composeFile, "-p", composeProject, "down", "-v").Run()
	_ = os.Remove(credsPath)

	upCmd := exec.Command("docker", "compose", "-f", composeFile, "-p", composeProject, "up", "--wait", "--wait-timeout", "180")
	dockerUp = true
	if out, err := upCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose up failed: %s\n%s", err, out)
	}

	creds, err := readPolarisCreds()
	if err != nil {
		return fmt.Errorf("reading polaris bootstrap credentials: %w", err)
	}

	tmpl, err := os.ReadFile(configTemplate)
	if err != nil {
		return fmt.Errorf("reading %s: %w", configTemplate, err)
	}

	credential := creds.ClientID + ":" + creds.ClientSecret
	rendered := strings.ReplaceAll(string(tmpl), "CREDENTIAL_PLACEHOLDER", credential)
	if err := os.WriteFile(localConfig, []byte(rendered), 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", localConfig, err)
	}

	return nil
}

type polarisCreds struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
}

// readPolarisCreds polls the bootstrap credentials volume until the
// `polaris-bootstrap` service has finished writing the file. The compose
// `--wait` flag returns when the bootstrap container exits successfully, but
// the bind-mounted file may briefly lag the container's exit on some hosts.
func readPolarisCreds() (*polarisCreds, error) {
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		bs, err := os.ReadFile(credsPath)
		if err == nil && len(bs) > 0 {
			var c polarisCreds
			if err := json.Unmarshal(bs, &c); err != nil {
				return nil, fmt.Errorf("parsing %s: %w", credsPath, err)
			}
			if c.ClientID != "" && c.ClientSecret != "" {
				return &c, nil
			}
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("timed out reading %s: %v", credsPath, lastErr)
}

func TestMain(m *testing.M) {
	code := m.Run()
	if dockerUp {
		exec.Command("docker", "compose", "-f", composeFile, "-p", composeProject, "down", "-v").Run()
	}
	os.Exit(code)
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ensureDockerUp(t)

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	// Normalize S3 file paths that contain UUIDs which change on every run.
	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`"s3://[^"]+\.csv\.gz"`).ReplaceAllString(s, `"s3://<bucket>/<uuid>.csv.gz"`)
		},
	}

	all := *testAll || os.Getenv("ICEBERG_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-rest-local.flow.yaml"
	applySpec := "testdata/apply-rest-local.flow.yaml"
	migrateSpec := "testdata/migrate-rest-local.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
		migrateSpec = "testdata/migrate.flow.yaml"
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTestParallel(t, newMaterialization, materializeSpec, makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTestParallel(t, &Driver{}, newMaterialization, applySpec, makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTestParallel(t, newMaterialization, migrateSpec, makeResourceFn, nil)
	})
}
