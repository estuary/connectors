package connector

import (
	"context"
	"os/exec"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TestIsIntegerKeyType guards the eligibility filter that decides which key
// fields storeDocument attempts to restore. Only a solely-integer type (modulo
// null) qualifies: a polymorphic key isn't guaranteed to pack as int64, so
// restoring it unconditionally could flip its stored BSON type inconsistently
// across documents.
func TestIsIntegerKeyType(t *testing.T) {
	cases := []struct {
		name  string
		types []string
		want  bool
	}{
		{"integer", []string{"integer"}, true},
		{"nullable integer", []string{"integer", "null"}, true},
		{"string", []string{"string"}, false},
		{"boolean", []string{"boolean"}, false},
		{"polymorphic integer or string", []string{"integer", "string"}, false},
		{"only null", []string{"null"}, false},
		{"empty", nil, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, isIntegerKeyType(c.types))
		})
	}
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(collection string, delta bool) resource {
		return resource{Collection: collection, DeltaUpdates: delta}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTest(t, NewMaterializer, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	t.Run("truncate", func(t *testing.T) {
		if boilerplate.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		boilerplate.RunTestAllTasks(t, "testdata/truncate.flow.yaml", func(t *testing.T, _ []byte, taskName string, cfg config) {
			var ctx = context.Background()
			client, err := connect(ctx, cfg)
			require.NoError(t, err)
			defer client.Disconnect(ctx)
			var db = client.Database(cfg.Database)

			for _, name := range []string{"truncate_standard", "truncate_delta"} {
				require.NoError(t, db.Collection(name).Drop(ctx))
			}

			boilerplate.RunFlowctl(t, "raw", "preview-next",
				"--name", taskName,
				"--source", "testdata/truncate.flow.yaml",
				"--fixture", "testdata/truncate.fixture.json",
				"--shards", "1",
				"--timeout", "5m",
			)

			// The fixture stores ids 1-3, then re-stores only id 1 during a
			// backfill. Only the standard-updates collection loses the
			// documents published before the backfill.
			for name, want := range map[string][]int64{
				"truncate_standard": {1},
				"truncate_delta":    {1, 1, 2, 3},
			} {
				cur, err := db.Collection(name).Find(ctx, bson.D{}, options.Find().SetSort(bson.D{{Key: "id", Value: 1}}))
				require.NoError(t, err)
				var docs []struct {
					ID int64 `bson:"id"`
				}
				require.NoError(t, cur.All(ctx, &docs))
				var ids []int64
				for _, d := range docs {
					ids = append(ids, d.ID)
				}
				require.Equal(t, want, ids, name)
			}
		})
	})
}
