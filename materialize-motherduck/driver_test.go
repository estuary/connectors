package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	bp_test "github.com/estuary/connectors/materialize-boilerplate/testing"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
		{"DUCKDB_BUCKET", &out.Bucket},
		{"AWS_ACCESS_KEY_ID", &out.AWSAccessKeyID},
		{"AWS_SECRET_ACCESS_KEY", &out.AWSSecretAccessKey},
		{"AWS_REGION", &out.Region},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	require.NoError(t, out.Validate())

	return out
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()

	var cfg = mustGetCfg(t)
	db, err := cfg.db(ctx)
	require.NoError(t, err)
	var client = client{db: db}

	sql.RunFenceTestCases(t,
		client,
		[]string{"temp_test_fencing_checkpoints"},
		duckDialect,
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(cfg)
			db, err := cfg.db(context.Background())
			require.NoError(t, err)
			client := client{db: db}
			require.Equal(t, tt.want, client.PreReqs(context.Background(), &sql.Endpoint{
				Config: cfg,
				Tenant: "tenant",
			}).Unwrap())
		})
	}
}

func TestApply(t *testing.T) {
	cfg := mustGetCfg(t)
	ctx := context.Background()

	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	firstTable := "first-table"
	secondTable := "second-table"

	firstResource := tableConfig{
		Table:    firstTable,
		Schema:   cfg.Schema,
		Delta:    true,
		database: cfg.Database,
	}
	firstResourceJson, err := json.Marshal(firstResource)
	require.NoError(t, err)

	secondResource := tableConfig{
		Table:    secondTable,
		Schema:   cfg.Schema,
		Delta:    true,
		database: cfg.Database,
	}
	secondResourceJson, err := json.Marshal(secondResource)
	require.NoError(t, err)

	bp_test.RunApplyTestCases(
		t,
		newDuckDriver(),
		configJson,
		[2]json.RawMessage{firstResourceJson, secondResourceJson},
		[2][]string{firstResource.Path(), secondResource.Path()},
		func(t *testing.T) []string {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)

			rows, err := db.QueryContext(
				ctx,
				fmt.Sprintf(
					"select table_name from information_schema.tables where table_catalog = '%s' and table_schema = '%s';",
					cfg.Database,
					cfg.Schema,
				))
			require.NoError(t, err)

			out := []string{}
			for rows.Next() {
				var tableName string
				require.NoError(t, rows.Scan(&tableName))
				out = append(out, tableName)
			}

			return out
		},
		func(t *testing.T, resourcePath []string) string {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)

			q := fmt.Sprintf(`
				select column_name, is_nullable, data_type
				from information_schema.columns
				where 
					table_catalog = '%s' 
					and table_schema = '%s'
					and table_name = '%s';
			`,
				resourcePath[0],
				resourcePath[1],
				resourcePath[2],
			)

			rows, err := db.QueryContext(ctx, q)
			require.NoError(t, err)

			type foundColumn struct {
				Name     string
				Nullable string // string "YES" or "NO"
				Type     string
			}

			cols := []foundColumn{}
			for rows.Next() {
				var c foundColumn
				require.NoError(t, rows.Scan(&c.Name, &c.Nullable, &c.Type))
				cols = append(cols, c)
			}

			b, err := json.MarshalIndent(cols, "", "  ")
			require.NoError(t, err)

			return string(b)
		},
		func(t *testing.T) {
			t.Helper()

			db, err := cfg.db(ctx)
			require.NoError(t, err)

			for _, tbl := range []string{firstTable, secondTable} {
				_, err := db.ExecContext(ctx, fmt.Sprintf(
					"drop table %s",
					duckDialect.Identifier(cfg.Database, cfg.Schema, tbl),
				))
				require.NoError(t, err)
			}
		},
	)
}

func TestSpecification(t *testing.T) {
	var resp, err = newDuckDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
