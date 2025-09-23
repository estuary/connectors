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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}

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
		Table:  "target",
		Schema: "public",
	}

	db, err := stdsql.Open("pgx", cfg.toURI())
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
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
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", testDialect.Identifier(resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, testDialect.Identifier(resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips required for this test to run it is not run normally.
	// Enable it via the TESTDB environment variable. It will take several minutes for this test to
	// complete (you should run it with a sufficient -timeout value).
	cfg := mustGetCfg(t)

	ctx := context.Background()

	c, err := newClient(ctx, &sql.Endpoint[config]{Config: cfg})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		testDialect,
		testTemplates.createTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			conn, err := pgx.Connect(ctx, cfg.toURI())
			if err != nil {
				return fmt.Errorf("store pgx.Connect: %w", err)
			}
			defer conn.Close(ctx)

			txn, err := conn.BeginTx(ctx, pgx.TxOptions{})
			if err != nil {
				return err
			}
			defer txn.Rollback(ctx)

			if updateFence(ctx, txn, testDialect, fence) != nil {
				return err
			}

			return txn.Commit(ctx)
		},
		func(table sql.Table) (out string, err error) {
			err = c.(*client).withDB(func(db *stdsql.DB) error {
				var sql = fmt.Sprintf(
					"select materialization, key_begin, key_end, fence, FROM_VARBYTE(checkpoint, 'utf8') from %s order by materialization, key_begin, key_end asc;",
					table.Identifier,
				)

				rows, err := db.Query(sql)
				if err != nil {
					return err
				}
				defer rows.Close()

				var b strings.Builder
				b.WriteString("materialization, key_begin, key_end, fence, checkpoint")

				for rows.Next() {
					b.WriteString("\n")
					var materialization string
					var keyBegin, keyEnd, fence int
					var checkpoint string
					if err = rows.Scan(&materialization, &keyBegin, &keyEnd, &fence, &checkpoint); err != nil {
						return err
					} else if base64Bytes, err := base64.StdEncoding.DecodeString(checkpoint); err != nil {
						return err
					} else if decompressed, err := maybeDecompressBytes(base64Bytes); err != nil {
						return err
					} else {
						b.WriteString(fmt.Sprintf(
							"%s, %d, %d, %d, %s",
							materialization, keyBegin, keyEnd, fence, base64.StdEncoding.EncodeToString(decompressed)),
						)
					}
				}

				out = b.String()
				return nil
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
		cfg  func(config) config
		want []error
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) config {
				cfg.User = "wrong" + cfg.User
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
		{
			name: "bucket doesn't exist",
			cfg: func(cfg config) config {
				cfg.Bucket = nonExistentBucket
				return cfg
			},
			want: []error{fmt.Errorf("bucket %q does not exist", nonExistentBucket)},
		},
		{
			name: "unauthorized to bucket: access key id",
			cfg: func(cfg config) config {
				cfg.AWSAccessKeyID = "wrong" + cfg.AWSAccessKeyID
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to %q", cfg.Bucket)},
		},
		{
			name: "unauthorized to bucket: secret access key",
			cfg: func(cfg config) config {
				cfg.AWSSecretAccessKey = "wrong" + cfg.AWSSecretAccessKey
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to %q", cfg.Bucket)},
		},
		{
			name: "database problem and bucket problem",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				cfg.Bucket = nonExistentBucket
				return cfg
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
			require.Equal(t, tt.want, preReqs(ctx, tt.cfg(cfg)).Unwrap())
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
