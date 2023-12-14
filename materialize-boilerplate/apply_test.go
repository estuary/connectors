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
			want: testResults{
				createdMetaTables: true,
				putSpec:           true,
				createdResources:  1,
			},
		},
		{
			name:         "remove required field",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "remove-required.flow.proto"),
			want: testResults{
				createdMetaTables:    false,
				putSpec:              true,
				nullabledProjections: 1,
			},
		},
		{
			name:         "add required field",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "add-new-required.flow.proto"),
			want: testResults{
				createdMetaTables: false,
				putSpec:           true,
				addedProjections:  1,
			},
		},
		{
			name:         "add binding",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "add-new-binding.flow.proto"),
			want: testResults{
				createdMetaTables: false,
				putSpec:           true,
				createdResources:  1,
			},
		},
		{
			name:         "replace binding",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "replace-original-binding.flow.proto"),
			want: testResults{
				createdMetaTables: false,
				putSpec:           true,
				replaceResources:  1,
			},
		},
		{
			name:         "field is newly nullable",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "make-nullable.flow.proto"),
			want: testResults{
				createdMetaTables:    false,
				putSpec:              true,
				nullabledProjections: 1,
			},
		},
	}

	var snap strings.Builder

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := &testApplier{
				storedSpec: tt.originalSpec,
			}
			is := testInfoSchemaFromSpec(t, tt.originalSpec)

			req := &pm.Request_Apply{Materialization: tt.newSpec, Version: "aVersion"}

			// Not a dry run, not concurrent.
			req.DryRun = false
			got, err := ApplyChanges(ctx, req, app, is, false)
			require.NoError(t, err)
			require.Equal(t, tt.want, app.getResults())
			actions := got.ActionDescription

			// Dry run, not concurrent.
			req.DryRun = true
			got, err = ApplyChanges(ctx, req, app, is, false)
			require.NoError(t, err)
			require.Equal(t, testResults{}, app.getResults())
			require.Equal(t, actions, got.ActionDescription)

			// Not a dry run, concurrent.
			req.DryRun = false
			got, err = ApplyChanges(ctx, req, app, is, true)
			require.NoError(t, err)
			require.Equal(t, tt.want, app.getResults())
			actions = got.ActionDescription

			// Dry run, concurrent
			req.DryRun = true
			got, err = ApplyChanges(ctx, req, app, is, true)
			require.NoError(t, err)
			require.Equal(t, testResults{}, app.getResults())
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
	createdMetaTables     bool
	putSpec               bool
	createdResources      int
	replaceResources      int
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

func (a *testApplier) CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, ActionApplyFn, error) {
	return "create meta tables", func(ctx context.Context) error {
		a.results.createdMetaTables = true
		return nil
	}, nil
}

func (a *testApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	return fmt.Sprintf("create resource for collection %q", binding.Collection.Name.String()), func(ctx context.Context) error {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.results.createdResources += 1
		return nil
	}, nil
}

func (a *testApplier) LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	return a.storedSpec, nil
}

func (a *testApplier) PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, exists bool) (string, ActionApplyFn, error) {
	return fmt.Sprintf("put spec with version %q", version), func(ctx context.Context) error {
		a.results.putSpec = true
		return nil
	}, nil
}

func (a *testApplier) ReplaceResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	return fmt.Sprintf("replace resource for collection %q", binding.Collection.Name.String()), func(ctx context.Context) error {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.results.replaceResources += 1
		return nil
	}, nil
}

func (a *testApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, applyParams BindingUpdate) (string, ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	if len(applyParams.NewProjections) == 0 &&
		len(applyParams.NewlyNullableFields) == 0 &&
		!applyParams.NewlyDeltaUpdates {
		return "", nil, nil
	}

	action := fmt.Sprintf(
		"update resource for collection %q [new projections: %d, newly nullable fields: %d, newly delta updates: %t]",
		binding.Collection.Name.String(),
		len(applyParams.NewProjections),
		len(applyParams.NewlyNullableFields),
		applyParams.NewlyDeltaUpdates,
	)

	return action, func(ctx context.Context) error {
		a.mu.Lock()
		defer a.mu.Unlock()

		a.results.addedProjections += len(applyParams.NewProjections)
		a.results.nullabledProjections += len(applyParams.NewlyNullableFields)
		a.results.changedToDeltaUpdates = applyParams.NewlyDeltaUpdates

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
func testInfoSchemaFromSpec(t *testing.T, s *pf.MaterializationSpec) *InfoSchema {
	t.Helper()

	// Hypothetical one-way transformation of path components and field names. I really hope no
	// system we ever materialize to actually does this.
	transform := func(in string) string {
		return in + "_transformed"
	}
	transformPath := func(in []string) []string {
		out := make([]string, 0, len(in))
		for _, p := range in {
			out = append(out, transform(p))
		}
		return out
	}

	is := NewInfoSchema(transformPath, transform)

	if s == nil || len(s.Bindings) == 0 {
		return is
	}

	for _, b := range s.Bindings {
		for _, f := range b.FieldSelection.AllFields() {
			proj := *b.Collection.GetProjection(f)

			is.PushField(EndpointField{
				Name:     transform(f),
				Nullable: proj.Inference.Exists != pf.Inference_MUST || slices.Contains(proj.Inference.Types, "null"),
				Type:     strings.Join(proj.Inference.Types, ","),
				CharacterMaxLength: func() int {
					if proj.Inference.String_ != nil {
						return int(proj.Inference.String_.MaxLength)
					}
					return 0
				}(),
			}, transformPath(b.ResourcePath)...)
		}
	}

	return is
}
