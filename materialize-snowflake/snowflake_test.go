package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TESTDB") != "yes" {
		t.Skipf("skipping %q: ${TESTDB} != \"yes\"", t.Name())
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

func TestSpecification(t *testing.T) {
	var resp, err = newSnowflakeDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips required for this test to run it is not run normally.
	// Enable it via the TESTDB environment variable. It will take several minutes for this test to
	// complete (you should run it with a sufficient -timeout value).
	cfg := mustGetCfg(t)

	client := client{uri: cfg.ToURI("tenant")}

	ctx := context.Background()

	sql.RunFenceTestCases(t,
		sql.FenceSnapshotPath,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		snowflakeDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var err = client.withDB(func(db *stdsql.DB) error {
				var fenceUpdate strings.Builder
				if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				var _, err = db.Exec(fenceUpdate.String())
				return err
			})
			return err
		},
		func(table sql.Table) (out string, err error) {
			err = client.withDB(func(db *stdsql.DB) error {
				out, err = sql.StdDumpTable(ctx, db, table)
				return err
			})
			return
		},
	)
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, prereqs(context.Background(), &sql.Endpoint{
				Config: tt.cfg(cfg),
				Tenant: "tenant",
			}).Unwrap())
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
