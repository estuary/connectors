package connector

import (
	"context"
	"errors"
	"os/exec"
	"sort"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
)

func testConfig() *config {
	return &config{
		AWSAccessKeyID:     "anything",
		AWSSecretAccessKey: "anything",
		Region:             "anything",
		Advanced: advancedConfig{
			Endpoint:     "http://localhost:8000",
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) resource {
		return resource{Table: table}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("truncate", func(t *testing.T) {
		if boilerplate.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}

		ctx := context.Background()
		client, err := testConfig().client(ctx)
		require.NoError(t, err)

		for _, table := range []string{"truncate_standard", "truncate_no_published_at"} {
			require.NoError(t, dropTestTable(ctx, client, table))
		}

		boilerplate.RunFlowctl(t, "raw", "preview-next",
			"--name", "acmeCo/tests/materialize-dynamodb-truncate",
			"--source", "testdata/truncate.flow.yaml",
			"--fixture", "testdata/truncate.fixture.json",
			"--shards", "1",
			"--timeout", "5m",
			"--network", "flow-test",
		)

		// The fixture stores ids 1-3, then re-stores only id 1 during a
		// backfill. Only truncate_standard has flow_published_at selected,
		// so only it loses the ids the backfill didn't re-send.
		for table, want := range map[string][]int64{
			"truncate_standard":        {1},
			"truncate_no_published_at": {1, 2, 3},
		} {
			require.Equal(t, want, scanIDs(ctx, t, client, table), table)
		}
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTest(t, NewMaterializer, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}

// dropTestTable deletes table, tolerating it not existing from a previous
// test run.
func dropTestTable(ctx context.Context, client *client, table string) error {
	err := deleteTable(ctx, client, table)
	if _, ok := errors.AsType[*types.ResourceNotFoundException](err); ok {
		return nil
	}
	return err
}

// scanIDs returns the sorted ids stored in table.
func scanIDs(ctx context.Context, t *testing.T, client *client, table string) []int64 {
	t.Helper()

	out, err := client.db.Scan(ctx, &dynamodb.ScanInput{
		TableName:      aws.String(table),
		ConsistentRead: aws.Bool(true),
	})
	require.NoError(t, err)

	var ids []int64
	for _, item := range out.Items {
		n, ok := item["id"].(*types.AttributeValueMemberN)
		require.True(t, ok, "item missing numeric id: %v", item)
		v, err := strconv.ParseInt(n.Value, 10, 64)
		require.NoError(t, err)
		ids = append(ids, v)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	return ids
}
