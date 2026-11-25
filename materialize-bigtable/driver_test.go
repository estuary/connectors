package connector

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"testing"

	"cloud.google.com/go/bigtable"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
)

// testAll controls whether integration tests run against a real Cloud
// Bigtable instance in addition to the local emulator. The real-instance
// path requires the encrypted `config.gcp.yaml` to decrypt and a live
// instance pre-provisioned per `testdata/README.md`, so by default we
// only exercise the emulator. Opt in to the full matrix by setting the
// `BIGTABLE_TEST_ALL` environment variable or passing
// `-bigtable.test-all` to `go test`.
var testAll = flag.Bool("bigtable.test-all", false, "run integration tests against a real Cloud Bigtable instance in addition to the local emulator")

func testConfig() *config {
	return &config{
		ProjectID:  "test-project",
		InstanceID: "test-instance",
		Advanced: advancedConfig{
			Endpoint: "localhost:8086",
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

	all := *testAll || os.Getenv("BIGTABLE_TEST_ALL") != ""

	materializeSpec := "testdata/materialize-local.flow.yaml"
	applySpec := "testdata/apply-local.flow.yaml"
	if all {
		materializeSpec = "testdata/materialize.flow.yaml"
		applySpec = "testdata/apply.flow.yaml"
	}

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, materializeSpec, makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("truncate", func(t *testing.T) {
		boilerplate.SkipRuntimeV1(t)

		ctx := t.Context()
		cfg := *testConfig()
		client, err := cfg.dataClient(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		bundled := boilerplate.RunFlowctl(t, "raw", "bundle", "--source", "testdata/truncate.flow.yaml")
		task := boilerplate.PrepareTestTask(t, NewMaterializer, bundled,
			"acmeCo/tests/materialize-bigtable-truncate", cfg, makeResourceFn)

		boilerplate.RunFlowctl(t, "raw", "preview-next",
			"--name", task.Name,
			"--source", task.SourcePath,
			"--fixture", "testdata/truncate.fixture.json",
			"--shards", "1",
			"--timeout", "5m",
			"--network", "flow-test",
		)

		// The fixture stores ids 1-3, then re-stores only id 1 during a
		// backfill. Only truncate_standard has flow_published_at selected, so
		// only it loses the ids the backfill didn't re-send.
		for table, want := range map[string][]int64{
			"truncate_standard":        {1},
			"truncate_no_published_at": {1, 2, 3},
		} {
			ids, err := readIDs(ctx, client, task.Resource(t, table))
			require.NoError(t, err)
			require.Equal(t, want, ids, table)
		}
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, applySpec, makeResourceFn, boilerplate.RuntimeConfig{})
	})
}

// readIDs returns the sorted ids stored in table.
func readIDs(ctx context.Context, client *bigtable.Client, table string) ([]int64, error) {
	var ids []int64
	var readErr error
	filter := bigtable.ChainFilters(
		bigtable.FamilyFilter(columnFamily),
		bigtable.ColumnFilter("id"),
		bigtable.LatestNFilter(1),
	)
	err := client.Open(table).ReadRows(ctx, bigtable.InfiniteRange(""), func(r bigtable.Row) bool {
		cells := r[columnFamily]
		if len(cells) != 1 || len(cells[0].Value) != 8 {
			readErr = fmt.Errorf("row %q has no 8-byte id cell: %v", r.Key(), cells)
			return false
		}
		ids = append(ids, int64(binary.BigEndian.Uint64(cells[0].Value)))
		return true
	}, bigtable.RowFilter(filter))
	if err != nil {
		return nil, fmt.Errorf("reading table %q: %w", table, err)
	} else if readErr != nil {
		return nil, readErr
	}
	slices.Sort(ids)

	return ids, nil
}
