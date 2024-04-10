package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/snowflakedb/gosnowflake"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{}

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"SNOWFLAKE_HOST", &out.Host},
		{"SNOWFLAKE_ACCOUNT", &out.Account},
		{"SNOWFLAKE_USER", &out.User},
		{"SNOWFLAKE_PASSWORD", &out.Password},
		{"SNOWFLAKE_DATABASE", &out.Database},
		{"SNOWFLAKE_SCHEMA", &out.Schema},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

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

	db, err := stdsql.Open("snowflake", cfg.ToURI("testing", true))
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
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = %s",
				testDialect.Identifier(cfg.Schema, sql.DefaultFlowMaterializations),
				testDialect.Literal(materialization.String()),
			))
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
	// These tests assume that the configuration obtained from environment variables forms a valid
	// config that could be used to materialize into Snowflake. Various parameters of the
	// configuration are then manipulated to test assertions for incorrect configs.

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
			name: "wrong account identifier in host",
			cfg: func(cfg config) *config {
				cfg.Host = "wrong.snowflakecomputing.com"
				return &cfg
			},
			want: []error{fmt.Errorf("incorrect account identifier %q in host URL", "wrong")},
		},
		{
			name: "wrong username",
			cfg: func(cfg config) *config {
				cfg.User = "wrong" + cfg.User
				return &cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) *config {
				cfg.Password = "wrong" + cfg.Password
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

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(cfg)

			client, err := newClient(ctx, &sql.Endpoint{Config: cfg})
			require.NoError(t, err)
			defer client.Close()

			require.Equal(t, tt.want, client.PreReqs(context.Background()).Unwrap())
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
