package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

var testDialect = bqDialect(featureFlagDefaults)
var testTemplates = renderTemplates(testDialect)

func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{
		Advanced: advancedConfig{
			NoFlowDocument: true,
			FeatureFlags:   "allow_existing_tables_for_new_bindings",
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
			require.Equal(t, tt.want, preReqs(context.Background(), tt.cfg(cfg)).Unwrap())
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
