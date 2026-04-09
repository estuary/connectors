package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"gopkg.in/yaml.v3"

	_ "github.com/snowflakedb/gosnowflake/v2"
)

func mustGetCfg(t *testing.T) config {
	if testing.Short() {
		t.Skip("skipping test in short mode")
		return config{}
	}

	yamlBytes, err := os.ReadFile("testdata/config.yaml")
	require.NoError(t, err)

	var raw map[string]interface{}
	require.NoError(t, yaml.Unmarshal(yamlBytes, &raw))
	jsonBytes, err := json.Marshal(raw)
	require.NoError(t, err)

	if gjson.GetBytes(jsonBytes, "sops").Exists() {
		sopsCmd := exec.Command("sops", "--decrypt", "--input-type", "json", "--output-type", "json", "/dev/stdin")
		jqCmd := exec.Command("jq", `walk( if type == "object" then with_entries(.key |= rtrimstr("_sops")) else . end)`)
		sopsCmd.Stdin = bytes.NewReader(jsonBytes)
		jqCmd.Stdin, err = sopsCmd.StdoutPipe()
		require.NoError(t, err)
		require.NoError(t, sopsCmd.Start())
		jsonBytes, err = jqCmd.Output()
		require.NoError(t, err)
		require.NoError(t, sopsCmd.Wait())
	}

	var out config
	require.NoError(t, json.Unmarshal(jsonBytes, &out))
	require.NoError(t, out.Validate())

	return out
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table:  table,
			Schema: "ESTUARY_SCHEMA",
			Delta:  delta,
		}
	}

	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`@flow_v1/[a-fA-F0-9\-]{36}`).ReplaceAllString(s, `<uuid>`)
		},
		func(s string) string {
			return regexp.MustCompile(`"Path":\s*"[^"]*"`).ReplaceAllString(s, `"Path": "<uuid>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"PipeStartTime":\s*"[^"]+"`).ReplaceAllString(s, `"PipeStartTime": "<timestamp>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"EncryptionKey":\s*"[^"]+"`).ReplaceAllString(s, `"EncryptionKey": "<encryption_key>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"channel_name":\s*"([^_]+_[^_]+_[^_]+_[^_]+_)[A-F0-9]+(_[^"]+)"`).ReplaceAllString(s, `"channel_name":"${1}<channel_id>${2}"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"path":\s*"[^"]*"`).ReplaceAllString(s, `"path": "<path>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"md5":\s*"[^"]*"`).ReplaceAllString(s, `"md5": "<md5>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"chunk_length":\s*\d+`).ReplaceAllString(s, `"chunk_length": "<chunk_length>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"chunk_length_uncompressed":\s*\d+`).ReplaceAllString(s, `"chunk_length_uncompressed": "<chunk_length_uncompressed>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"chunk_md5":\s*"[^"]*"`).ReplaceAllString(s, `"chunk_md5": "<chunk_md5>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"encryption_key_id":\s*\d+`).ReplaceAllString(s, `"encryption_key_id": "<encryption_key_id>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"first_insert_time_in_ms":\s*\d+`).ReplaceAllString(s, `"first_insert_time_in_ms": "<first_insert_time_in_ms>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"last_insert_time_in_ms":\s*\d+`).ReplaceAllString(s, `"last_insert_time_in_ms": "<last_insert_time_in_ms>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"flush_start_ms":\s*\d+`).ReplaceAllString(s, `"flush_start_ms": "<flush_start_ms>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"build_duration_ms":\s*\d+`).ReplaceAllString(s, `"build_duration_ms": "<build_duration_ms>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"upload_duration_ms":\s*\d+`).ReplaceAllString(s, `"upload_duration_ms": "<upload_duration_ms>"`)
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newSnowflakeDriver(), "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newSnowflakeDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newSnowflakeDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	cfg := mustGetCfg(t)

	tests := []struct {
		name string
		cfg  func(config) *config
		want []error
	}{
		{
			name: "valid",
			cfg:  func(cfg config) *config { return &cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) *config {
				cfg.Credentials.User = "wrong" + cfg.Credentials.User
				return &cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) *config {
				cfg.Credentials.Password = "wrong" + cfg.Credentials.Password
				return &cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong role",
			cfg: func(cfg config) *config {
				cfg.Role = "wrong" + cfg.Role
				return &cfg
			},
			want: []error{fmt.Errorf("role %q does not exist", "wrong"+cfg.Role)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(context.Background(), *tt.cfg(cfg)).Unwrap())
		})
	}
}

