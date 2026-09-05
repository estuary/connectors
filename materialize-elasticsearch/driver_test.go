package connector

import (
	"os/exec"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(index string, delta bool) resource {
		return resource{Index: index, DeltaUpdates: delta}
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

	// OpenSearch is an API-compatible fork of Elasticsearch. The same connector
	// supports both, auto-detecting OpenSearch and adapting a few field mapping
	// types (notably 'flat_object' in place of 'flattened'). These run against
	// the opensearch service in the compose file and have their own snapshots,
	// which differ from the Elasticsearch ones in the affected mapping types.
	t.Run("opensearch", func(t *testing.T) {
		t.Run("materialize", func(t *testing.T) {
			boilerplate.RunMaterializationTest(t, NewMaterializer, "testdata/materialize.opensearch.flow.yaml", makeResourceFn, nil,
				boilerplate.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})
		})

		t.Run("apply", func(t *testing.T) {
			boilerplate.RunApplyTest(t, &driver{}, NewMaterializer, "testdata/apply.opensearch.flow.yaml", makeResourceFn)
		})
	})

	// Elasticsearch does not support migrations since index mappings are
	// immutable, so the migration test is not applicable.
}
