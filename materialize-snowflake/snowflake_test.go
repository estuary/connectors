package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/snowflakedb/gosnowflake"
)

var testDialect = snowflakeDialect("PUBLIC", timestampTypeLTZ, featureFlagDefaults)
var testTemplates = renderTemplates(testDialect)

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
			NoFlowDocument: true,
			FeatureFlags:   "allow_existing_tables_for_new_bindings",
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

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:  "TARGET",
		Schema: "PUBLIC",
	}

	dsn, err := cfg.toURI(true)
	require.NoError(t, err)

	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newSnowflakeDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:  "TARGET",
		Schema: "PUBLIC",
	}

	dsn, err := cfg.toURI(true)
	require.NoError(t, err)

	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newSnowflakeDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "0")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "PARSE_JSON('{}')")
			q := fmt.Sprintf("insert into %s (%s) SELECT %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
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

func TestRetry(t *testing.T) {
	var ErrTemporary = errors.New("temporary error")
	var ErrPermanent = errors.New("permenant error")
	tests := []struct {
		name         string
		succeedAfter int
		returnError  error
		attempts     int
		err          bool
	}{
		{
			name:         "success",
			succeedAfter: 1,
			returnError:  &TemporaryError{ErrTemporary},
			attempts:     1,
			err:          false,
		},
		{
			name:         "eventual success",
			succeedAfter: 3,
			returnError:  &TemporaryError{ErrTemporary},
			attempts:     3,
			err:          false,
		},
		{
			name:         "failure",
			succeedAfter: 6,
			returnError:  &TemporaryError{ErrTemporary},
			attempts:     5,
			err:          true,
		},
		{
			name:         "permanent-failure",
			succeedAfter: 6,
			returnError:  ErrPermanent,
			attempts:     1,
			err:          true,
		},
	}
	var testDelays = []float64{0.0, 0.0, 0.0, 0.0, 0.0}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			attempts := 0
			err := Retry[*TemporaryError](testDelays, func() error {
				attempts++
				if attempts >= tt.succeedAfter {
					return nil
				}
				return tt.returnError
			})

			if tt.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.attempts, attempts)
		})
	}
}
