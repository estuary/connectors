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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v4/stdlib"
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
		{"REDSHIFT_ADDRESS", &out.Address},
		{"REDSHIFT_USER", &out.User},
		{"REDSHIFT_PASSWORD", &out.Password},
		{"REDSHIFT_DATABASE", &out.Database},
		{"REDSHIFT_BUCKET", &out.Bucket},
		{"REDSHIFT_SCHEMA", &out.Schema},
		{"AWS_ACCESS_KEY_ID", &out.AWSAccessKeyID},
		{"AWS_SECRET_ACCESS_KEY", &out.AWSSecretAccessKey},
		{"AWS_REGION", &out.Region},
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
		Table:  "target",
		Schema: "public",
	}

	db, err := stdsql.Open("pgx", cfg.toURI())
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newRedshiftDriver(),
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

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", rsDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = %s",
				rsDialect.Identifier(cfg.Schema, sql.DefaultFlowMaterializations),
				rsDialect.Literal(materialization.String()),
			))
		},
	)
}

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips required for this test to run it is not run normally.
	// Enable it via the TESTDB environment variable. It will take several minutes for this test to
	// complete (you should run it with a sufficient -timeout value).
	cfg := mustGetCfg(t)

	ctx := context.Background()

	c, err := newClient(ctx, &sql.Endpoint{Config: &cfg})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		rsDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			err = c.(*client).withDB(func(db *stdsql.DB) error {
				out, err = sql.StdDumpTable(ctx, db, table)
				return err
			})
			return
		},
	)
}

func TestPrereqs(t *testing.T) {
	// These tests assume that the configuration obtained from environment variables forms a valid
	// config that could be used to materialize into Redshift. Various parameters of the
	// configuration are then manipulated to test assertions for incorrect configs.

	cfg := mustGetCfg(t)

	nonExistentBucket := uuid.NewString()

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
			name: "wrong database",
			cfg: func(cfg config) *config {
				cfg.Database = "wrong" + cfg.Database
				return &cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) *config {
				cfg.Address = "wrong." + cfg.Address
				return &cfg
			},
			want: []error{fmt.Errorf("host at address %q cannot be found", "wrong."+cfg.Address)},
		},
		{
			name: "bucket doesn't exist",
			cfg: func(cfg config) *config {
				cfg.Bucket = nonExistentBucket
				return &cfg
			},
			want: []error{fmt.Errorf("bucket %q does not exist", nonExistentBucket)},
		},
		{
			name: "unauthorized to bucket: access key id",
			cfg: func(cfg config) *config {
				cfg.AWSAccessKeyID = "wrong" + cfg.AWSAccessKeyID
				return &cfg
			},
			want: []error{fmt.Errorf("not authorized to write to %q", cfg.Bucket)},
		},
		{
			name: "unauthorized to bucket: secret access key",
			cfg: func(cfg config) *config {
				cfg.AWSSecretAccessKey = "wrong" + cfg.AWSSecretAccessKey
				return &cfg
			},
			want: []error{fmt.Errorf("not authorized to write to %q", cfg.Bucket)},
		},
		{
			name: "database problem and bucket problem",
			cfg: func(cfg config) *config {
				cfg.Database = "wrong" + cfg.Database
				cfg.Bucket = nonExistentBucket
				return &cfg
			},
			want: []error{
				fmt.Errorf("database %q does not exist", "wrong"+cfg.Database),
				fmt.Errorf("bucket %q does not exist", nonExistentBucket),
			},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(cfg)

			client, err := newClient(ctx, &sql.Endpoint{Config: cfg})
			require.NoError(t, err)
			defer client.Close()

			require.Equal(t, tt.want, client.PreReqs(ctx).Unwrap())
		})
	}
}

func TestSpecification(t *testing.T) {
	var resp, err = newRedshiftDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	// Note that this diverges from canonical protobuf field names.
	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
