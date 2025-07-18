package boilerplate

import (
	"context"
	"embed"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./testdata/generate-spec-proto.sh testdata/apply/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/remove-required.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/add-new-required.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/add-new-binding.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/replace-original-binding.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/apply/make-nullable.flow.yaml

//go:embed testdata/apply/generated_specs
var applyFS embed.FS

func loadApplySpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := applyFS.ReadFile(filepath.Join("testdata/apply/generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

func TestApply(t *testing.T) {
	ctx := context.Background()

	type testCase struct {
		name         string
		originalSpec *pf.MaterializationSpec
		newSpec      *pf.MaterializationSpec
		want         testResults
	}

	tests := []testCase{
		{
			name:         "new materialization",
			originalSpec: nil,
			newSpec:      loadApplySpec(t, "base.flow.proto"),
			want:         testResults{createdResources: 1},
		},
		{
			name:         "remove required field",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "remove-required.flow.proto"),
			want:         testResults{nullabledProjections: 1},
		},
		{
			name:         "add required field",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "add-new-required.flow.proto"),
			want:         testResults{addedProjections: 1},
		},
		{
			name:         "add binding",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "add-new-binding.flow.proto"),
			want:         testResults{createdResources: 1},
		},
		{
			name:         "replace binding",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "replace-original-binding.flow.proto"),
			want:         testResults{deletedResources: 1, createdResources: 1},
		},
		{
			name:         "replace binding disabled -> enabled",
			originalSpec: specWithBindingsDisabled(loadApplySpec(t, "base.flow.proto")),
			newSpec:      loadApplySpec(t, "replace-original-binding.flow.proto"),
			want:         testResults{deletedResources: 1, createdResources: 1},
		},
		{
			name:         "field is newly nullable",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "make-nullable.flow.proto"),
			want:         testResults{nullabledProjections: 1},
		},
	}

	var snap strings.Builder

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &testApplier{
				storedSpec: tt.originalSpec,
			}
			is := testInfoSchemaFromSpec(t, tt.originalSpec, simpleTestTransform)

			req := &pm.Request_Apply{Materialization: tt.newSpec, Version: "aVersion", LastMaterialization: tt.originalSpec}

			// Not concurrent.
			got, err := ApplyChanges(ctx, req, app, is, false)
			require.NoError(t, err)
			require.Equal(t, tt.want, app.getResults())
			actions := got.ActionDescription

			// Concurrent.
			got, err = ApplyChanges(ctx, req, app, is, true)
			require.NoError(t, err)
			require.Equal(t, tt.want, app.getResults())

			require.Equal(t, actions, got.ActionDescription)

			if idx > 0 {
				snap.WriteString("\n\n")
			}
			snap.WriteString(fmt.Sprintf("* %s:\n", tt.name))
			snap.WriteString(got.ActionDescription)
		})
	}

	cupaloy.SnapshotT(t, snap.String())
}

type testResults struct {
	createdResources      int
	deletedResources      int
	addedProjections      int
	nullabledProjections  int
	changedToDeltaUpdates bool
}

var _ Applier = (*testApplier)(nil)

type testApplier struct {
	mu         sync.Mutex
	storedSpec *pf.MaterializationSpec
	results    testResults
}

func (a *testApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	return fmt.Sprintf("create resource for %q", binding.ResourcePath), func(ctx context.Context) error {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.results.createdResources += 1
		return nil
	}, nil
}

func (a *testApplier) DeleteResource(ctx context.Context, path []string) (string, ActionApplyFn, error) {
	return fmt.Sprintf("delete resource %q", path), func(ctx context.Context) error {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.results.deletedResources += 1
		return nil
	}, nil
}

func (a *testApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate BindingUpdate) (string, ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	if len(bindingUpdate.NewProjections) == 0 &&
		len(bindingUpdate.NewlyNullableFields) == 0 &&
		!bindingUpdate.NewlyDeltaUpdates {
		return "", nil, nil
	}

	action := fmt.Sprintf(
		"update resource for %q [new projections: %d, newly nullable fields: %d, newly delta updates: %t]",
		binding.ResourcePath,
		len(bindingUpdate.NewProjections),
		len(bindingUpdate.NewlyNullableFields),
		bindingUpdate.NewlyDeltaUpdates,
	)

	return action, func(ctx context.Context) error {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.results.addedProjections += len(bindingUpdate.NewProjections)
		a.results.nullabledProjections += len(bindingUpdate.NewlyNullableFields)
		a.results.changedToDeltaUpdates = bindingUpdate.NewlyDeltaUpdates

		return nil
	}, nil
}

func (a *testApplier) getResults() testResults {
	res := a.results
	a.results = testResults{}
	return res
}

// testInfoSchemaFromSpec constructs a mock InfoSchema from a spec that represents an existing table
// with all the fields from the field selection.
func testInfoSchemaFromSpec(t *testing.T, s *pf.MaterializationSpec, transform func(string) string) *InfoSchema {
	t.Helper()

	transformPath := func(in []string) []string {
		out := make([]string, 0, len(in))
		for _, p := range in {
			out = append(out, transform(p))
		}
		return out
	}

	is := NewInfoSchema(transformPath, transform, false)

	if s == nil || len(s.Bindings)+len(s.InactiveBindings) == 0 {
		return is
	}

	for _, b := range append(s.Bindings, s.InactiveBindings...) {
		res := is.PushResource(transformPath(b.ResourcePath)...)
		for _, f := range b.FieldSelection.AllFields() {
			proj := *b.Collection.GetProjection(f)

			res.PushField(ExistingField{
				Name:     transform(f),
				Nullable: proj.Inference.Exists != pf.Inference_MUST || slices.Contains(proj.Inference.Types, "null"),
				Type:     strings.Join(proj.Inference.Types, ","),
				CharacterMaxLength: func() int {
					if proj.Inference.String_ != nil {
						return int(proj.Inference.String_.MaxLength)
					}
					return 0
				}(),
			})
		}
	}

	return is
}

func simpleTestTransform(in string) string {
	return in + "_transformed"
}

func ambiguousTestTransform(in string) string {
	return strings.ToLower(in)
}
