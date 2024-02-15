package main

import (
	"context"
	"encoding/base64"
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

	_ "github.com/marcboeker/go-duckdb"
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
		{"MOTHERDUCK_TOKEN", &out.Token},
		{"MOTHERDUCK_DATABASE", &out.Database},
		{"MOTHERDUCK_SCHEMA", &out.Schema},
		{"MOTHERDUCK_BUCKET", &out.Bucket},
		{"AWS_ACCESS_KEY_ID", &out.AWSAccessKeyID},
		{"AWS_SECRET_ACCESS_KEY", &out.AWSSecretAccessKey},
		{"AWS_REGION", &out.Region},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	require.NoError(t, out.Validate())

	return out
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:    "target",
		Schema:   cfg.Schema,
		Delta:    true,
		database: cfg.Database,
	}

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newDuckDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)
			defer db.Close()

			sch, err := sql.StdGetSchema(ctx, db, cfg.Database, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)
			defer db.Close()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", duckDialect.Identifier(cfg.Database, resourceConfig.Schema, resourceConfig.Table)))

			_, _ = db.ExecContext(ctx, fmt.Sprintf(
				"delete from %s where materialization = %s",
				duckDialect.Identifier(cfg.Database, cfg.Schema, sql.DefaultFlowMaterializations),
				duckDialect.Literal(materialization.String()),
			))
		},
	)
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()

	var cfg = mustGetCfg(t)

	c, err := newClient(ctx, &sql.Endpoint{Config: &cfg})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{cfg.Database, cfg.Schema, "temp_test_fencing_checkpoints"},
		duckDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			return sql.StdDumpTable(ctx, c.(*client).db, table)
		},
	)
}

func TestPrereqs(t *testing.T) {
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
			name: "token is well formed but wrong",
			cfg: func(cfg config) *config {
				cfg.Token = fmt.Sprintf(
					"%s.%s.%s",
					base64.RawURLEncoding.EncodeToString([]byte("first")),
					base64.RawURLEncoding.EncodeToString([]byte("middle")),
					base64.RawURLEncoding.EncodeToString([]byte("last")),
				)
				return &cfg
			},
			want: []error{fmt.Errorf("invalid token: unauthenticated")},
		},
		{
			name: "wrong schema",
			cfg: func(cfg config) *config {
				cfg.Schema = "wrong" + cfg.Schema
				return &cfg
			},
			want: []error{fmt.Errorf("schema %q does not exist", "wrong"+cfg.Schema)},
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
				cfg.Schema = "wrong" + cfg.Schema
				cfg.Bucket = nonExistentBucket
				return &cfg
			},
			want: []error{
				fmt.Errorf("schema %q does not exist", "wrong"+cfg.Schema),
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
	var resp, err = newDuckDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
