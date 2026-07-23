package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"testing"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	_ "github.com/snowflakedb/gosnowflake/v2"
)

func mustGetCfg(t *testing.T) config {
	if testing.Short() {
		t.Skip("skipping test in short mode")
		return config{}
	}

	jsonBytes, err := exec.Command("sops", "--decrypt", "--output-type", "json", "testdata/config.yaml").Output()
	require.NoError(t, err)

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

	t.Run("apply-drain", func(t *testing.T) {
		cfg := mustGetCfg(t)
		tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
		res := makeResourceFn(tableName, false).WithDefaults(cfg)

		seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
			// A staged transaction is a query persisted in the connector
			// state, keyed by the binding's state key, along with the staged
			// directory holding its files. The directory is cleaned up after
			// the query executes; a directory with no files under the
			// connector's flow_v1 stage (created by Setup during the base
			// Apply) stands in for one whose files were already consumed.
			query := sql.DrainSeedInsertQuery(t, newSnowflakeDriver(), cfg, appliedSpec, "PARSE_JSON('{}')")
			state, err := json.Marshal(map[string]any{
				appliedSpec.Bindings[0].StateKey: map[string]any{
					"Table":     tableName,
					"Query":     query,
					"StagedDir": fmt.Sprintf("@flow_v1/applydrain-%s", uuid.NewString()),
				},
			})
			require.NoError(t, err)
			return state
		}

		verifyDrained := func(t *testing.T, _ *pf.MaterializationSpec, _ []string, rows [][]any) {
			require.Len(t, rows, 1, "the staged transaction's row must have been committed")
		}

		sql.RunApplyDrainTest(t, newSnowflakeDriver(), cfg, res, seedPending, verifyDrained)
	})

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
			want: []error{fmt.Errorf("JWT token is invalid")},
		},
		{
			name: "wrong private key",
			cfg: func(cfg config) *config {
				cfg.Credentials.Password = "wrong" + cfg.Credentials.Password
				return &cfg
			},
			want: []error{fmt.Errorf("JWT token is invalid")},
		},
		{
			name: "wrong role",
			cfg: func(cfg config) *config {
				cfg.Role = "wrong" + cfg.Role
				return &cfg
			},
			want: []error{fmt.Errorf("JWT token is invalid")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(context.Background(), *tt.cfg(cfg)).Unwrap())
		})
	}
}
