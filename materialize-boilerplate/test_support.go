package boilerplate

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./testdata/generate-spec-proto.sh testdata/apply/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/remove-required.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/remove-optional.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/add-new-required.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/add-new-optional.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/add-new-binding.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/remove-original-binding.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/replace-original-binding.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/unsatisfiable.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/forbidden.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/alternate-root-projection.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/remove-format.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/more-multiple.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/object-to-array.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/numeric-string-to-numeric.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/nullability.flow.yaml

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
	driver Connector,
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
			name: "replace original binding table",
			spec: loadSpec(t, applyFs, "replace-original-binding.flow.proto"),
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
					snap.WriteString(fmt.Sprintf("\n* Schema for %s:\n", strings.Join(spec.Bindings[idx].ResourcePath, ".")))
					snap.WriteString(dumpSchema(t, spec.Bindings[idx].ResourcePath))
				}
			}

			snap.WriteString("\n--- End " + tt.name + " ---\n\n")
		})
	}
	cupaloy.SnapshotT(t, snap.String())
}

func RunValidateTestCases(t *testing.T, makeValidator func(*testing.T, *pf.MaterializationSpec) Validator, snapshotPath string) {
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
			name:         "[field: numericString] remove numeric string format",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "remove-format.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "[field: scalarValue and numericString] numeric and numeric format string interactions",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "numeric-string-to-numeric.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "[field: key] unsatisfiable type change",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "unsatisfiable.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "[field: multiple] add even more types",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "more-multiple.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "[field: nonScalarValue] change type from object to array",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "object-to-array.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "[field: scalarValue] forbidden type change",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "forbidden.flow.proto"),
			deltaUpdates: false,
		},
		{
			name:         "[field: scalarValue] remove from list of required fields",
			existingSpec: loadSpec(t, validateFs, "base.flow.proto"),
			newSpec:      loadSpec(t, validateFs, "nullability.flow.proto"),
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
			cs, err := makeValidator(t, tt.existingSpec).ValidateBinding([]string{"key_value"}, tt.deltaUpdates, 0, tt.newSpec.Bindings[0].Collection, nil, tt.existingSpec)
			require.NoError(t, err)

			j, err := json.MarshalIndent(cs, "", "\t")
			require.NoError(t, err)

			snap.WriteString("--- Begin " + tt.name + " ---\n")
			snap.WriteString(string(j))
			snap.WriteString("\n--- End " + tt.name + " ---\n\n")
		})
	}

	t.Run("[field: key] unsatisfiable type change with backfill counter increment", func(t *testing.T) {
		existingSpec := loadSpec(t, validateFs, "base.flow.proto")
		newSpec := loadSpec(t, validateFs, "unsatisfiable.flow.proto")

		cs, err := makeValidator(t, existingSpec).ValidateBinding([]string{"key_value"}, false, 1, newSpec.Bindings[0].Collection, nil, existingSpec)
		require.NoError(t, err)

		j, err := json.MarshalIndent(cs, "", "\t")
		require.NoError(t, err)

		snap.WriteString("--- Begin [field: key] unsatisfiable type change with backfill counter increment ---\n")
		snap.WriteString(string(j))
		snap.WriteString("\n--- End [field: key] unsatisfiable type change with backfill counter increment ---\n\n")
	})

	cupaloy.New(cupaloy.SnapshotSubdirectory(snapshotPath)).SnapshotT(t, snap.String())
}

// BasicInfoFromSpec creates a vanilla InfoSchema from a Flow MaterializationSpec which may be
// useful for testing scenarios that do not require the use of a specific endpoint's type system.
func BasicInfoFromSpec(t *testing.T, spec *pf.MaterializationSpec, mapType func(*pf.Projection) (string, bool)) *InfoSchema {
	is := NewInfoSchema(
		func(in []string) []string { return in },
		func(in string) string { return in },
	)

	if spec != nil {
		for _, b := range spec.Bindings {
			selectedFields := append(b.FieldSelection.Keys, b.FieldSelection.Values...)
			if b.FieldSelection.Document != "" {
				selectedFields = append(selectedFields, b.FieldSelection.Document)
			}

			for _, f := range selectedFields {
				p := b.Collection.GetProjection(f)
				mapped, mustExist := mapType(p)

				is.PushField(EndpointField{
					Name:     f,
					Nullable: !mustExist,
					Type:     mapped,
				}, b.ResourcePath...)
			}
		}
	}

	return is
}

func testMapType(p *pf.Projection) (string, bool) {
	mustExist := p.Inference.Exists == pf.Inference_MUST
	if slices.Contains(p.Inference.Types, "null") {
		mustExist = false
	}
	return strings.Join(p.Inference.Types, ","), mustExist
}
