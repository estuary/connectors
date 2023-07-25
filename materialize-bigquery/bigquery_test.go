package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
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

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips to bigquery required for this test to run it is not run
	// normally. Enable it via the RUN_FENCE_TESTS environment variable. It will take several
	// minutes for this test to complete.
	cfg := mustGetCfg(t)

	tablePath := []string{cfg.ProjectID, cfg.Dataset, "temp_test_fencing_checkpoints"}

	var ctx = context.Background()
	client, err := cfg.client(ctx)
	require.NoError(t, err)

	sql.RunFenceTestCases(t,
		sql.FenceSnapshotPath,
		client,
		tablePath,
		bqDialect,
		tplCreateTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
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
	// These tests assume that the configuration obtained from environment variables forms a valid
	// config that could be used to materialize into Bigquery. Various parameters of the
	// configuration are then manipulated to test assertions for incorrect configs.

	// Due to the nature of configuring the connector with a JSON service account key and the
	// difficulties in discriminating between error responses from BigQuery there's only a handful
	// of cases that can be explicitly tested.

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
			name: "bucket doesn't exist",
			cfg: func(cfg config) *config {
				cfg.Bucket = nonExistentBucket
				return &cfg
			},
			want: []error{fmt.Errorf("bucket %q does not exist", nonExistentBucket)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg(cfg)
			client, err := cfg.client(context.Background())
			require.NoError(t, err)

			require.Equal(t, tt.want, prereqs(context.Background(), &sql.Endpoint{
				Config: cfg,
				Client: client,
				Tenant: "tenant",
			}).Unwrap())
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
