package testing

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/validate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./generate-spec-proto.sh testdata/apply/base.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/apply/remove-required.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/apply/remove-optional.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/apply/add-new-required.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/apply/add-new-optional.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/apply/add-new-binding.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/apply/remove-original-binding.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/validate/base.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/validate/unsatisfiable.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/validate/forbidden.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/validate/alternate-root-projection.flow.yaml
//go:generate ./generate-spec-proto.sh testdata/validate/format-change.flow.yaml

//go:embed testdata/apply/generated_specs
var applyFs embed.FS

//go:embed testdata/validate/generated_specs
var validateFs embed.FS

func loadSpec(t *testing.T, fs embed.FS, path string) *pf.MaterializationSpec {
	t.Helper()

	dirs := map[embed.FS]string{
		applyFs:    "testdata/apply/generated_specs",
		validateFs: "testdata/validate/generated_specs",
	}

	specBytes, err := fs.ReadFile(filepath.Join(dirs[fs], path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

func RunApplyTestCases(
	t *testing.T,
	driver boilerplate.Connector,
	configJson json.RawMessage,
	resourceJsons [2]json.RawMessage,
	resourcePaths [2][]string,
	listTables func(t *testing.T) []string,
	dumpSchema func(t *testing.T, resourcePath []string) string,
	cleanup func(t *testing.T),
) {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q capture: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	defer cleanup(t)

	tests := []struct {
		name string
		spec *pf.MaterializationSpec
	}{
		{
			name: "base",
			spec: loadSpec(t, applyFs, "base.flow.proto"),
		},
		{
			name: "remove original required field",
			spec: loadSpec(t, applyFs, "remove-required.flow.proto"),
		},
		{
			name: "re-add removed original required field",
			spec: loadSpec(t, applyFs, "base.flow.proto"),
		},
		{
			name: "remove original optional field",
			spec: loadSpec(t, applyFs, "remove-optional.flow.proto"),
		},
		{
			name: "re-add removed original optional field",
			spec: loadSpec(t, applyFs, "base.flow.proto"),
		},
		{
			name: "add new required field",
			spec: loadSpec(t, applyFs, "add-new-required.flow.proto"),
		},
		{
			name: "remove new required field",
			spec: loadSpec(t, applyFs, "base.flow.proto"),
		},
		{
			name: "add new optional field",
			spec: loadSpec(t, applyFs, "add-new-optional.flow.proto"),
		},
		{
			name: "remove new optional field",
			spec: loadSpec(t, applyFs, "base.flow.proto"),
		},
		{
			name: "add new binding",
			spec: loadSpec(t, applyFs, "add-new-binding.flow.proto"),
		},
		{
			name: "remove new binding",
			spec: loadSpec(t, applyFs, "base.flow.proto"),
		},
		{
			name: "remove original binding",
			spec: loadSpec(t, applyFs, "remove-original-binding.flow.proto"),
		},
	}

	ctx := context.Background()

	var snap strings.Builder
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := tt.spec
			spec.ConfigJson = configJson
			for idx := range spec.Bindings {
				spec.Bindings[idx].ResourceConfigJson = resourceJsons[idx]
				spec.Bindings[idx].ResourcePath = resourcePaths[idx]
			}

			_, err := driver.Apply(ctx, &pm.Request_Apply{
				Materialization: spec,
				Version:         "",
				DryRun:          false,
			})
			require.NoError(t, err)

			snap.WriteString("--- Begin " + tt.name + " ---\n")
			snap.WriteString(fmt.Sprintf("* Table list: %s:", listTables(t)))

			if len(spec.Bindings) > 0 {
				for idx := range spec.Bindings {
					snap.WriteString(fmt.Sprintf("\n* Schema for %s:\n", spec.Bindings[idx].ResourcePath))
					snap.WriteString(dumpSchema(t, spec.Bindings[idx].ResourcePath))
				}
			}

			snap.WriteString("\n--- End " + tt.name + " ---\n\n")
		})
	}
	cupaloy.SnapshotT(t, snap.String())
}

func RunValidateTestCases(t *testing.T, v validate.Validator) {
	t.Helper()

	tests := []struct {
		name         string
		existingSpec *pf.MaterializationSpec
		newSpec      *pf.MaterializationSpec
		deltaUpdates bool
	}{
		{
			name:         "new binding - standard updates",
			existingSpec: nil,
			newSpec:      loadSpec(t, validateFs, "base.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "new binding - delta updates",
			existingSpec: nil,
			newSpec:      loadSpec(t, validateFs, "base.flow.proto"),
			deltaUpdates: true,
		},
		{
			name:         "with existing binding - standard updates",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "base.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "with existing binding - delta updates",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "base.flow.proto"),
			deltaUpdates: true,
		},
		{
			name:         "numeric string format change",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "format-change.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "unsatisfiable type change",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "unsatisfiable.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "forbidden type change",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "forbidden.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "root document projection change - standard updates",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "alternate-root-projection.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "root document projection change - delta updates",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "alternate-root-projection.flow.proto"),
			deltaUpdates: true,
		},
	}

	var snap strings.Builder
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs, err := v.ValidateBinding([]string{"key_value"}, tt.deltaUpdates, tt.newSpec.Bindings[0].Collection, tt.existingSpec)
			require.NoError(t, err)

			j, err := json.MarshalIndent(cs, "", "\t")
			require.NoError(t, err)

			snap.WriteString("--- Begin " + tt.name + " ---\n")
			snap.WriteString(string(j))
			snap.WriteString("\n--- End " + tt.name + " ---\n\n")
		})
	}
	cupaloy.SnapshotT(t, snap.String())
}
