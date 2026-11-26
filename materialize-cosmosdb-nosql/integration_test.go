package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"sort"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	bptest "github.com/estuary/connectors/materialize-boilerplate/testutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test requires Docker and flowctl")
	}

	t.Cleanup(func() {
		out, err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").CombinedOutput()
		require.NoError(t, err, "%s", out)
	})
	out, err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait", "--wait-timeout", "180").CombinedOutput()
	require.NoError(t, err, "%s", out)

	makeResourceFn := func(container string, delta bool) resource {
		res := resource{Container: container, DeltaUpdates: delta}
		switch {
		case strings.HasPrefix(container, "events_"):
			res.PartitionKey = "/seq"
		case strings.HasPrefix(container, "not_simple_"), strings.HasPrefix(container, "challengingnames_"):
			// The emulator rejects the first key's field name as a partition path.
			// Keep every field and key component, partitioning on the other key.
			res.PartitionKey = "/_id"
		}
		return res
	}

	t.Run("materialize", func(t *testing.T) {
		bptest.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn,
			[]func(string) string{bptest.SanitizeCheckpointHashes(`"containerRid":"([^"\n]+)"`, "container")},
			bptest.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("apply", func(t *testing.T) {
		bptest.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn, bptest.RuntimeConfig{})
	})

	t.Run("migrate", func(t *testing.T) {
		bptest.RunMigrationTest(t, NewMaterializer, "testdata/migrate.flow.yaml", makeResourceFn, nil, bptest.RuntimeConfig{})
	})

	t.Run("cosmos", func(t *testing.T) {
		bptest.RunTestAllTasks(t, "testdata/cosmos.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			ctx := context.Background()
			task := bptest.PrepareTestTask(t, NewMaterializer, bundled, taskName, cfg, makeResourceFn)
			mat, err := NewMaterializer(ctx, task.Name, cfg, nil)
			require.NoError(t, err)
			defer mat.Close(ctx)
			client := mat.(*materialization).client

			for _, phase := range []string{"initial", "updates"} {
				t.Run(phase, func(t *testing.T) {
					args := []string{"raw", "preview-next", "--shards", "1"}
					if bptest.RuntimeV1() {
						args = []string{"preview", "--sessions=-1,0"}
					}
					bptest.RunFlowctl(t, append(args,
						"--name", task.Name,
						"--source", task.SourcePath,
						"--fixture", "testdata/fixture."+phase+".json",
						"--network", "flow-test",
						"--timeout", "5m",
					)...)
					cupaloy.SnapshotT(t, snapshotContainers(t, ctx, mat.(*materialization), task))
				})
			}

			t.Run("apply", func(t *testing.T) {
				bptest.SkipRuntimeV1(t)
				spec := testMaterialization(t, cfg, false)
				for _, b := range spec.Bindings {
					name := b.ResourcePath[1]
					res := makeResourceFn(task.Resource(t, name), false)
					b.ResourceConfigJson, err = json.Marshal(res)
					require.NoError(t, err)
					b.ResourcePath = task.Path(t, name)
				}
				d := driver{}
				_, err = d.Validate(ctx, validateRequest(spec))
				require.NoError(t, err)
				_, err = d.Apply(ctx, &pm.Request_Apply{Materialization: spec, Version: "v1"})
				require.NoError(t, err)

				mismatch := validateRequest(spec)
				mismatch.Bindings[1].ResourceConfigJson, err = json.Marshal(resource{Container: task.Resource(t, "events"), PartitionKey: "/region"})
				require.NoError(t, err)
				_, err = d.Validate(ctx, mismatch)
				require.ErrorContains(t, err, `expected a single partition key at "/region"`)

				backfill := proto.Clone(spec).(*pf.MaterializationSpec)
				backfill.Bindings[0].Backfill++
				applied, err := d.Apply(ctx, &pm.Request_Apply{Materialization: backfill, LastMaterialization: spec, Version: "v2"})
				require.NoError(t, err)
				require.Contains(t, applied.ActionDescription, "delete container")
				require.Contains(t, applied.ActionDescription, "create container")
				container, err := client.NewContainer(cfg.Database, task.Resource(t, "orders"))
				require.NoError(t, err)
				_, err = container.ReadItem(ctx, azcosmos.NewPartitionKeyString("1"), "1", nil)
				require.True(t, isStatus(err, http.StatusNotFound), "%v", err)
				cupaloy.SnapshotT(t, snapshotContainers(t, ctx, mat.(*materialization), task))

				applied, err = d.Apply(ctx, &pm.Request_Apply{Materialization: backfill, LastMaterialization: backfill, Version: "v2"})
				require.NoError(t, err)
				require.Empty(t, applied.ActionDescription)
			})
		})
	})
}

func snapshotContainers(t *testing.T, ctx context.Context, mat *materialization, task bptest.TestTask) string {
	t.Helper()
	var snap strings.Builder
	for _, name := range []string{"orders", "events", "orders_delta", "events_delta"} {
		path := task.Path(t, name)
		container, err := mat.client.NewContainer(path[0], path[1])
		require.NoError(t, err)
		properties, err := container.Read(ctx, nil)
		require.NoError(t, err)
		pk, err := json.Marshal(properties.ContainerProperties.PartitionKeyDefinition)
		require.NoError(t, err)
		fmt.Fprintf(&snap, "Container: %s\nPartition key: %s\n", name, pk)

		data, err := bptest.SnapshotResource(ctx, mat, path)
		require.NoError(t, err)
		var lines []string
		for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
			if line == "" {
				continue
			}
			var doc map[string]any
			dec := json.NewDecoder(strings.NewReader(line))
			dec.UseNumber()
			require.NoError(t, dec.Decode(&doc))
			// Publication UUIDs vary between preview runtimes. Keep all other metadata.
			meta, ok := doc["_meta"].(map[string]any)
			require.True(t, ok)
			require.NotEmpty(t, meta["uuid"])
			meta["uuid"] = "UUID"
			encoded, err := json.Marshal(doc)
			require.NoError(t, err)
			lines = append(lines, string(encoded))
		}
		sort.Strings(lines)
		for _, line := range lines {
			fmt.Fprintln(&snap, line)
		}
		fmt.Fprintln(&snap)
	}
	return snap.String()
}
