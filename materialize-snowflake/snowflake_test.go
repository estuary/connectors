package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/snowflakedb/gosnowflake"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{
		Credentials: &snowflake_auth.CredentialConfig{
			AuthType: snowflake_auth.JWT,
		},
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"SNOWFLAKE_HOST", &out.Host},
		{"SNOWFLAKE_ACCOUNT", &out.Account},
		{"SNOWFLAKE_USER", &out.Credentials.User},
		{"SNOWFLAKE_DATABASE", &out.Database},
		{"SNOWFLAKE_SCHEMA", &out.Schema},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	key, err := os.ReadFile(os.Getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
	require.NoError(t, err)
	out.Credentials.PrivateKey = string(key)

	if err := out.Validate(); err != nil {
		t.Fatal(err)
	}

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

func TestSpecification(t *testing.T) {
	var resp, err = newSnowflakeDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestPrereqs(t *testing.T) {
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

func TestValidHost(t *testing.T) {
	for _, tt := range []struct {
		host string
		want error
	}{
		{"orgname-accountname.snowflakecomputing.com", nil},
		{"identifer.snowflakecomputing.com", nil},
		{"ORGNAME-accountname.snowFLAKEcomputing.coM", nil},
		{"orgname-accountname.aws.us-east-2.snowflakecomputing.com", nil},
		{"http://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "http://orgname-accountname.snowflakecomputing.com")},
		{"https://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "https://orgname-accountname.snowflakecomputing.com")},
		{"orgname-accountname.snowflakecomputin.com", fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", "orgname-accountname.snowflakecomputin.com")},
	} {
		t.Run(tt.host, func(t *testing.T) {
			require.Equal(t, tt.want, validHost(tt.host))
		})
	}
}
