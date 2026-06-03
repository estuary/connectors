package main

import (
	"os/exec"
	"testing"

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
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply.flow.yaml", makeResourceFn)
	})

	// OpenSearch is an API-compatible fork of Elasticsearch. The same connector
	// supports both, auto-detecting OpenSearch and adapting a few field mapping
	// types (notably 'flat_object' in place of 'flattened'). These run against
	// the opensearch service in the compose file and have their own snapshots,
	// which differ from the Elasticsearch ones in the affected mapping types.
	t.Run("opensearch", func(t *testing.T) {
		t.Run("materialize", func(t *testing.T) {
			boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.opensearch.flow.yaml", makeResourceFn, nil)
		})

		t.Run("apply", func(t *testing.T) {
			boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply.opensearch.flow.yaml", makeResourceFn)
		})
	})

	// Elasticsearch does not support migrations since index mappings are
	// immutable, so the migration test is not applicable.
}
