package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/hirochachacha/go-smb2"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(runTests(m))
}

// composeFixture is set when the tests started the Compose server themselves.
// TEST_DATABASE=yes runs them against an existing server instead, chosen with
// SMB_HOST, SMB_PORT, SMB_SHARE, SMB_USERNAME, SMB_PASSWORD and SMB_DOMAIN.
var composeFixture bool

func runTests(m *testing.M) (code int) {
	if testing.Short() || os.Getenv("TEST_DATABASE") == "yes" {
		return m.Run()
	}

	docker := func(args ...string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		if err != nil {
			return fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, out)
		}
		return nil
	}
	if docker("network", "inspect", "flow-test") != nil {
		if err := docker("network", "create", "flow-test"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	defer func() {
		if err := docker("compose", "-f", "docker-compose.yaml", "down", "-v"); err != nil {
			fmt.Fprintln(os.Stderr, err)
			code = 1
		}
	}()
	if err := docker("compose", "-f", "docker-compose.yaml", "up", "--wait", "--wait-timeout", "120"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := os.Setenv("TEST_DATABASE", "yes"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	composeFixture = true
	return m.Run()
}

func integrationConfig(t *testing.T) config {
	t.Helper()
	if testing.Short() || os.Getenv("TEST_DATABASE") != "yes" {
		t.Skip("skipping integration test: run without -short or set TEST_DATABASE=yes")
	}

	raw := testConfig()
	raw["host"] = "localhost"
	raw["port"] = 1446
	raw["directory"] = ".estuary-test-" + uuid.NewString() + "/exports/flow"
	for key, env := range map[string]string{
		"host": "SMB_HOST", "share": "SMB_SHARE", "username": "SMB_USERNAME", "password": "SMB_PASSWORD", "domain": "SMB_DOMAIN",
	} {
		if value := os.Getenv(env); value != "" {
			raw[key] = value
		}
	}
	if value := os.Getenv("SMB_PORT"); value != "" {
		port, err := strconv.Atoi(value)
		require.NoError(t, err)
		raw["port"] = port
	}
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	c, err := driver.NewConfig(encoded)
	require.NoError(t, err)
	return c.(config)
}

func integrationStore(t *testing.T, cfg config) (*smbStore, *smb2.Share) {
	t.Helper()
	s, err := newStore(t.Context(), cfg)
	require.NoError(t, err)
	share := s.share.(shareAdapter).Share
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		// Each test owns the random parent of exports/flow as well.
		require.NoError(t, share.WithContext(ctx).RemoveAll(smbPath(strings.Split(cfg.Directory, "/")[0])))
		require.NoError(t, s.Close())
	})
	return s, share.WithContext(t.Context())
}

func TestIntegrationStore(t *testing.T) {
	cfg := integrationConfig(t)
	s, share := integrationStore(t, cfg)
	ctx := t.Context()
	require.NoError(t, s.checkWritable(ctx, cfg.Directory))
	key := path.Join(cfg.Directory, "events/raw/v0000000000/00000000000000000000.csv.gz")
	upload, err := s.StageObject(ctx, strings.NewReader("contents"), key)
	require.NoError(t, err)
	_, err = share.Stat(smbPath(key))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, s.CompleteObject(ctx, upload))
	require.NoError(t, s.CompleteObject(ctx, upload))
	contents, err := share.ReadFile(smbPath(key))
	require.NoError(t, err)
	require.Equal(t, "contents", string(contents))
	upload, err = s.StageObject(ctx, strings.NewReader("replacement"), key)
	require.NoError(t, err)
	require.NoError(t, s.CompleteObject(ctx, upload))
	contents, err = share.ReadFile(smbPath(key))
	require.NoError(t, err)
	require.Equal(t, "replacement", string(contents))
}

func TestIntegrationValidate(t *testing.T) {
	cfg := integrationConfig(t)
	_, share := integrationStore(t, cfg)

	for _, tt := range []struct {
		name   string
		change func(*config)
		err    string
	}{
		{"writable", func(*config) {}, ""},
		{"bad credentials", func(c *config) { c.Password = "incorrect-test-password" }, "authenticating SMB session"},
		{"missing share", func(c *config) { c.Share = "missing-" + uuid.NewString() }, "mounting SMB share"},
		{"read only", func(c *config) { c.Share = "readonly" }, "writable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "read only" && !composeFixture {
				t.Skip("read-only share requires the Compose fixture")
			}
			c := cfg
			tt.change(&c)
			raw, err := json.Marshal(c)
			require.NoError(t, err)
			_, err = driver.Validate(t.Context(), &pm.Request_Validate{ConfigJson: raw})
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
				return
			}
			require.NoError(t, err)
			files, err := share.ReadDir(smbPath(cfg.Directory))
			require.NoError(t, err)
			require.Empty(t, files, "validation must delete its probe file")
		})
	}
}

func TestIntegrationMaterialize(t *testing.T) {
	cfg := integrationConfig(t)
	_, share := integrationStore(t, cfg)
	_, err := share.Stat(smbPath(cfg.Directory))
	require.True(t, os.IsNotExist(err), "destination must start without the configured directory")

	catalog, err := os.ReadFile("testdata/materialize.flow.yaml")
	require.NoError(t, err)
	var spec map[string]any
	require.NoError(t, yaml.Unmarshal(catalog, &spec))
	const taskName = "acmeCo/tests/materialize-smb"
	task := spec["materializations"].(map[string]any)[taskName].(map[string]any)
	task["endpoint"].(map[string]any)["local"].(map[string]any)["config"] = cfg
	encoded, err := json.Marshal(spec)
	require.NoError(t, err)
	source := filepath.Join(t.TempDir(), "materialize.flow.json")
	require.NoError(t, os.WriteFile(source, encoded, 0600))

	preview := func(fixture string, state json.RawMessage) json.RawMessage {
		args := []string{"raw", "preview-next", "--shards", "1"}
		if testutil.RuntimeV1() {
			args = []string{"preview", "--sessions=-1,0"}
		}
		args = append(args, "--name", taskName, "--source", source,
			"--fixture", fixture, "--timeout", "5m", "--network", "flow-test", "--output-state")
		if state != nil {
			args = append(args, "--initial-state", string(state))
		}
		output := testutil.RunFlowctl(t, args...)
		var final json.RawMessage
		for _, line := range bytes.Split(bytes.TrimSpace(output), []byte("\n")) {
			var record []json.RawMessage
			require.NoError(t, json.Unmarshal(line, &record))
			require.Len(t, record, 2)
			if string(record[0]) != `"connectorState"` {
				continue
			}
			var checkpoint struct {
				Updated    json.RawMessage `json:"updated"`
				MergePatch bool            `json:"mergePatch"`
			}
			require.NoError(t, json.Unmarshal(record[1], &checkpoint))
			require.False(t, checkpoint.MergePatch, "filesink emits full state replacements")
			final = checkpoint.Updated
		}
		require.NotEmpty(t, final, "preview must emit connector state for resume")
		return final
	}

	state := preview("testdata/fixture.initial.json", nil)
	initial, rows := readCSVFiles(t, share, cfg.Directory)
	t.Run("initial", func(t *testing.T) { cupaloy.SnapshotT(t, strings.Join(rows, "\n")) })

	preview("testdata/fixture.resume.json", state)
	resumed, rows := readCSVFiles(t, share, cfg.Directory)
	for key, contents := range initial {
		require.Equal(t, contents, resumed[key], "resuming must preserve %s", key)
	}
	require.Greater(t, len(resumed), len(initial), "resuming must write new files")
	t.Run("resumed", func(t *testing.T) { cupaloy.SnapshotT(t, strings.Join(rows, "\n")) })
}

type csvTestShare interface {
	ReadDir(string) ([]os.FileInfo, error)
	ReadFile(string) ([]byte, error)
}

func readCSVFiles(t *testing.T, share csvTestShare, directory string) (map[string][]byte, []string) {
	t.Helper()
	files := make(map[string][]byte)
	var rows []string
	headers := map[string][]string{
		"orders":     {"id", "canary", "status"},
		"events/raw": {"region", "seq", "canary"},
	}
	namePattern := regexp.MustCompile(`^(orders|events/raw)/v[0-9]{10}/[0-9]{20}\.csv\.gz$`)
	var walk func(string)
	walk = func(dir string) {
		entries, err := share.ReadDir(smbPath(dir))
		require.NoError(t, err)
		for _, entry := range entries {
			key := path.Join(dir, entry.Name())
			if entry.IsDir() {
				walk(key)
				continue
			}
			rel := strings.TrimPrefix(key, directory+"/")
			match := namePattern.FindStringSubmatch(rel)
			require.Len(t, match, 2, "unexpected file or uncommitted partial: %s", key)
			binding := match[1]
			contents, err := share.ReadFile(smbPath(key))
			require.NoError(t, err)
			files[key] = contents
			gz, err := gzip.NewReader(bytes.NewReader(contents))
			require.NoError(t, err)
			records, err := csv.NewReader(gz).ReadAll()
			require.NoError(t, err)
			require.NoError(t, gz.Close())
			require.Greater(t, len(records), 1, "file must have a header and at least one row")
			require.Equal(t, headers[binding], records[0], "CSV header must put keys first")
			for _, record := range records[1:] {
				values := make(map[string]string)
				for i, field := range records[0] {
					values[field] = record[i]
				}
				row, err := json.Marshal([]any{binding, values})
				require.NoError(t, err)
				rows = append(rows, string(row))
			}
		}
	}
	walk(directory)
	slices.Sort(rows)
	return files, rows
}
