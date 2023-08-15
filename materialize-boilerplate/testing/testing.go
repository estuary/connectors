package testing

import (
	"context"
	"embed"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./generate-spec-proto.sh testdata/base.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/remove-required.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/remove-optional.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/add-new-required.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/add-new-optional.flow.yaml

//go:embed testdata/generated_specs
var specFs embed.FS

func RunApplyTestCases(
	t *testing.T,
	driver boilerplate.Connector,
	configJson json.RawMessage,
	resourceJson json.RawMessage,
	resourcePath []string,
	dumpSchema func(t *testing.T) string,
	cleanup func(t *testing.T),
) {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	defer cleanup(t)

	tests := []struct {
		name     string
		specFile string
	}{
		{
			name:     "base",
			specFile: "base.flow.proto",
		},
		{
			name:     "remove original required field",
			specFile: "remove-required.flow.proto",
		},
		{
			name:     "re-add removed original required field",
			specFile: "base.flow.proto",
		},
		{
			name:     "remove original optional field",
			specFile: "remove-optional.flow.proto",
		},
		{
			name:     "re-add removed original optional field",
			specFile: "base.flow.proto",
		},
		{
			name:     "add new required field",
			specFile: "add-new-required.flow.proto",
		},
		{
			name:     "remove new required field",
			specFile: "base.flow.proto",
		},
		{
			name:     "add new optional field",
			specFile: "add-new-optional.flow.proto",
		},
		{
			name:     "remove new optional field",
			specFile: "base.flow.proto",
		},
	}

	ctx := context.Background()

	var snap strings.Builder

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			specBytes, err := specFs.ReadFile(filepath.Join("testdata/generated_specs", tt.specFile))
			require.NoError(t, err)

			var spec pf.MaterializationSpec
			require.NoError(t, spec.Unmarshal(specBytes))

			spec.ConfigJson = configJson
			// Test specs must have only a single binding.
			spec.Bindings[0].ResourceConfigJson = resourceJson
			spec.Bindings[0].ResourcePath = resourcePath

			req := pm.Request_Apply{
				Materialization: &spec,
				Version:         "",
				DryRun:          false,
			}

			_, err = driver.Apply(ctx, &req)
			require.NoError(t, err)

			snap.WriteString("--- Begin " + tt.name + " ---\n")
			snap.WriteString(dumpSchema(t))
			snap.WriteString("\n--- End " + tt.name + " ---\n\n")
		})
	}

	cupaloy.SnapshotT(t, snap.String())

}
