package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql-v2"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
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
		{"GCP_SERVICE_ACCOUNT_KEY", &out.CredentialsJSON},
		{"GCP_BQ_PROJECT_ID", &out.ProjectID},
		{"GCP_BQ_DATASET", &out.Dataset},
		{"GCP_BQ_REGION", &out.Region},
		{"GCP_BQ_BUCKET", &out.Bucket},
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
		Table:     "target",
		Dataset:   cfg.Dataset,
		projectID: cfg.ProjectID,
	}

	client, err := cfg.client(ctx, &sql.Endpoint[config]{Dialect: testDialect})
	require.NoError(t, err)
	defer client.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newBigQueryDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()
			return dumpSchema(t, ctx, client, cfg, resourceConfig)
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = client.query(ctx, fmt.Sprintf(
				"drop table %s;",
				testDialect.Identifier(cfg.ProjectID, cfg.Dataset, resourceConfig.Table),
			))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:     "target",
		Dataset:   cfg.Dataset,
		projectID: cfg.ProjectID,
	}

	client, err := cfg.client(ctx, &sql.Endpoint[config]{Dialect: testDialect})
	require.NoError(t, err)
	defer client.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newBigQueryDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()
			return dumpSchema(t, ctx, client, cfg, resourceConfig)
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "false")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "JSON '{}'")

			for i := range values {
				// bigquery does not support more than 6 fractional second precision, and will fail if we try
				// to insert a value with 9
				if keys[i] == "datetimeValue" {
					values[i] = "'2024-01-01 01:01:01.111111'"
				}
				if keys[i] == "int64ToNumber" {
					values[i] = "BIGNUMERIC '" + values[i] + "'"
				}
			}

			_, err = client.query(ctx, fmt.Sprintf(
				"insert into %s (%s) VALUES (%s);",
				testDialect.Identifier(cfg.ProjectID, cfg.Dataset, resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","),
			))
			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()
			var b strings.Builder

			var sql = fmt.Sprintf("select * from %s order by %s asc;", testDialect.Identifier(cfg.ProjectID, cfg.Dataset, resourceConfig.Table), testDialect.Identifier("key"))

			job, err := client.query(ctx, sql)
			require.NoError(t, err)

			it, err := job.Read(ctx)
			require.NoError(t, err)

			var values []bigquery.Value
			var firstResult = true
			for {
				if err = it.Next(&values); err == iterator.Done {
					break
				} else if err != nil {
					require.NoError(t, err)
				}

				if firstResult {
					for i, col := range it.Schema {
						if i > 0 {
							b.WriteString(", ")
						}
						b.WriteString(col.Name)
						b.WriteString(" (" + string(col.Type) + ")")
					}
					b.WriteString("\n")

					firstResult = false
				}

				for i, v := range values {
					if i > 0 {
						b.WriteString(", ")
					}
					b.WriteString(fmt.Sprintf("%v", v))
				}
			}

			return b.String()
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = client.query(ctx, fmt.Sprintf(
				"drop table %s;",
				testDialect.Identifier(cfg.ProjectID, cfg.Dataset, resourceConfig.Table),
			))
		},
	)
}

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips to bigquery required for this test to run it is not run
	// normally. Enable it via the RUN_FENCE_TESTS environment variable. It will take several
	// minutes for this test to complete.
	cfg := mustGetCfg(t)

	var ctx = context.Background()
	client, err := cfg.client(ctx, &sql.Endpoint[config]{Dialect: testDialect})
	require.NoError(t, err)

	var templates = renderTemplates(testDialect)

	sql.RunFenceTestCases(t,
		client,
		[]string{cfg.ProjectID, cfg.Dataset, "temp_test_fencing_checkpoints"},
		testDialect,
		templates.createTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := templates.updateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			} else if _, err := client.query(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("executing fence update: %w", err)
			}

			return nil
		},
		func(table sql.Table) (out string, err error) {
			job, err := client.query(ctx, fmt.Sprintf("SELECT * FROM %s;", table.Identifier))
			if err != nil {
				return "", err
			}

			it, err := job.Read(ctx)
			if err != nil {
				return "", err
			}

			rows := [][]bigquery.Value{}

			for {
				var values []bigquery.Value
				err := it.Next(&values)
				if err == iterator.Done {
					break
				}
				if err != nil {
					return "", err
				}

				rows = append(rows, values)
			}

			var b strings.Builder

			// Sort the results by materialization name, then key_begin, then key_end.
			sort.Slice(rows, func(i, j int) bool {
				if rows[i][0].(string) != rows[j][0].(string) {
					return rows[i][0].(string) < rows[j][0].(string)
				}

				if rows[i][1].(int64) != rows[j][1].(int64) {
					return rows[i][1].(int64) < rows[j][1].(int64)
				}

				return rows[i][2].(int64) < rows[j][2].(int64)
			})

			b.WriteString("materialization, key_begin, key_end, fence, checkpoint\n")
			for idx, r := range rows {
				b.WriteString(fmt.Sprint(r[0]) + ", ") // materialization
				b.WriteString(fmt.Sprint(r[1]) + ", ") // key_begin
				b.WriteString(fmt.Sprint(r[2]) + ", ") // key_end
				b.WriteString(fmt.Sprint(r[3]) + ", ") // fence
				b.WriteString(fmt.Sprint(r[4]))        // checkpoint

				if idx < len(rows)-1 {
					b.WriteString("\n")
				}
			}

			return b.String(), nil
		},
	)
}

func TestPrereqs(t *testing.T) {
	// Due to the nature of configuring the connector with a JSON service account key and the
	// difficulties in discriminating between error responses from BigQuery there's only a handful
	// of cases that can be explicitly tested.

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
			name: "bucket doesn't exist",
			cfg: func(cfg config) config {
				cfg.Bucket = nonExistentBucket
				return cfg
			},
			want: []error{fmt.Errorf("bucket %q does not exist", nonExistentBucket)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(context.Background(), tt.cfg(cfg), "testing").Unwrap())
		})
	}
}

func TestSpecification(t *testing.T) {
	var resp, err = newBigQueryDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func dumpSchema(t *testing.T, ctx context.Context, client *client, cfg config, resourceConfig tableConfig) string {
	t.Helper()

	job, err := client.query(ctx, fmt.Sprintf(
		"select column_name, is_nullable, data_type from %s where table_name = %s;",
		testDialect.Identifier(cfg.Dataset, "INFORMATION_SCHEMA", "COLUMNS"),
		testDialect.Literal(resourceConfig.Table),
	))
	require.NoError(t, err)

	it, err := job.Read(ctx)
	require.NoError(t, err)

	type foundColumn struct {
		Name     string `bigquery:"column_name"`
		Nullable string `bigquery:"is_nullable"`
		Type     string `bigquery:"data_Type"`
	}

	cols := []foundColumn{}
	for {
		var c foundColumn
		if err = it.Next(&c); err == iterator.Done {
			break
		} else if err != nil {
			require.NoError(t, err)
		}
		cols = append(cols, c)
	}

	slices.SortFunc(cols, func(a, b foundColumn) int {
		return strings.Compare(a.Name, b.Name)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, c := range cols {
		require.NoError(t, enc.Encode(c))
	}

	return out.String()
}
