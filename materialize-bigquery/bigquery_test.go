package connector

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bradleyjkemp/cupaloy"
	driverSql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

var testCredentialsPath = flag.String(
	"creds_path",
	"/path/to/credentials.base64",
	"Path to the base64-encoded credentials JSON to use for test authentication",
)
var testProjectID = flag.String(
	"project_id",
	"(project_id)",
	"The project ID to interact with during automated tests",
)
var testDataset = flag.String(
	"dataset",
	"(dataset)",
	"The bigquery dataset used during automated tests",
)
var testRegion = flag.String(
	"region",
	"(region)",
	"The region of the resources used during automated tests",
)
var testBucket = flag.String(
	"bucket",
	"(bucket)",
	"The bucket to interact with during automated tests",
)

func TestFencingCases(t *testing.T) {
	// Because of the number of round-trips to bigquery required for this test to run it is not run
	// normally. Enable it via the RUN_FENCE_TESTS environment variable. It will take several
	// minutes for this test to complete.

	if os.Getenv("RUN_FENCE_TESTS") != "yes" {
		t.Skipf("skipping %q: ${RUN_FENCE_TESTS} != \"yes\"", t.Name())
	}

	flag.Parse()

	f, err := os.ReadFile(*testCredentialsPath)
	require.NoError(t, err)

	var cfg = config{
		ProjectID:       *testProjectID,
		Dataset:         *testDataset,
		Region:          *testRegion,
		Bucket:          *testBucket,
		CredentialsJSON: string(f),
	}

	tablePath := []string{cfg.ProjectID, cfg.Dataset, "temp_test_fencing_checkpoints"}

	var ctx = context.Background()
	client, err := cfg.client(ctx)
	require.NoError(t, err)

	driverSql.RunFenceTestCases(t,
		client,
		tablePath,
		bqDialect,
		tplCreateTargetTable,
		func(table driverSql.Table, fence driverSql.Fence) error {
			var fenceUpdate strings.Builder
			if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			} else if _, err := client.query(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("executing fence update: %w", err)
			}

			return nil
		},
		func(table driverSql.Table) (out string, err error) {
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

func TestSpecification(t *testing.T) {
	var resp, err = newBigQueryDriver().
		Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_AIRBYTE_SOURCE})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
