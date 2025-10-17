package boilerplate

import (
	"embed"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
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

func TestComputeCommonUpdates(t *testing.T) {
	type testCase struct {
		name         string
		originalSpec *pf.MaterializationSpec
		newSpec      *pf.MaterializationSpec
		want         *computedUpdates
	}

	tests := []testCase{
		{
			name:         "new materialization",
			originalSpec: nil,
			newSpec:      loadApplySpec(t, "base.flow.proto"),
			want: &computedUpdates{
				newBindings:     []int{0},
				updatedBindings: map[int]updateBinding{},
			},
		},
		{
			name:         "remove required field",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "remove-required.flow.proto"),
			want: &computedUpdates{
				updatedBindings: map[int]updateBinding{
					0: {
						newlyNullableFields: []ExistingField{{
							Name:               "requiredVal1_transformed",
							Nullable:           false,
							Type:               "string",
							CharacterMaxLength: 0,
							HasDefault:         false,
						}},
					},
				},
			},
		},
		{
			name:         "add required field",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "add-new-required.flow.proto"),
			want: &computedUpdates{
				updatedBindings: map[int]updateBinding{
					0: {
						newProjections: []pf.Projection{{
							Ptr:   "/requiredVal2",
							Field: "requiredVal2",
							Inference: pf.Inference{
								Types:   []string{"string"},
								String_: &pf.Inference_String{},
								Exists:  pf.Inference_MUST,
							},
						}},
					},
				},
			},
		},
		{
			name:         "add binding",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "add-new-binding.flow.proto"),
			want: &computedUpdates{
				newBindings:     []int{1},
				updatedBindings: map[int]updateBinding{0: {}},
			},
		},
		{
			name:         "replace binding",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "replace-original-binding.flow.proto"),
			want: &computedUpdates{
				backfillBindings: []int{0},
				updatedBindings:  map[int]updateBinding{},
			},
		},
		{
			name:         "replace binding disabled -> enabled",
			originalSpec: specWithBindingsDisabled(loadApplySpec(t, "base.flow.proto")),
			newSpec:      loadApplySpec(t, "replace-original-binding.flow.proto"),
			want: &computedUpdates{
				backfillBindings: []int{0},
				updatedBindings:  map[int]updateBinding{},
			},
		},
		{
			name:         "field is newly nullable",
			originalSpec: loadApplySpec(t, "base.flow.proto"),
			newSpec:      loadApplySpec(t, "make-nullable.flow.proto"),
			want: &computedUpdates{
				updatedBindings: map[int]updateBinding{
					0: {
						newlyNullableFields: []ExistingField{{
							Name:               "requiredVal1_transformed",
							Nullable:           false,
							Type:               "string",
							CharacterMaxLength: 0,
							HasDefault:         false,
						}},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := testInfoSchemaFromSpec(t, tt.originalSpec, simpleTestTransform)
			got, err := computeCommonUpdates(tt.originalSpec, tt.newSpec, is)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
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

	is := NewInfoSchema(transformPath, transform, transform, false, false)

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
