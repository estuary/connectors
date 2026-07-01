package main

import (
	"os/exec"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate/testutil"
	"github.com/stretchr/testify/require"
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
		boilerplate.RunMaterializationTest(t, newMaterialization, "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		boilerplate.RunApplyTest(t, &driver{}, newMaterialization, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		boilerplate.RunMigrationTest(t, newMaterialization, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}
