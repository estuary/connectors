package validate

import (
	"embed"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ../testing/generate-spec-proto.sh testdata/base.flow.yaml
//go:generate ../testing/generate-spec-proto.sh testdata/alternate-root-projection.flow.yaml
//go:generate ../testing/generate-spec-proto.sh testdata/multiple-root-projections.flow.yaml
//go:generate ../testing/generate-spec-proto.sh testdata/new-binding.flow.yaml

//go:embed testdata/generated_specs
var specFs embed.FS

func loadSpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := specFs.ReadFile(filepath.Join("testdata/generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

func TestValidateSelectedFields(t *testing.T) {
	validator := NewValidator(testConstrainter{})

	tests := []struct {
		name           string
		storedSpec     *pf.MaterializationSpec
		newSpec        *pf.MaterializationSpec
		fieldSelection pf.FieldSelection
		deltaUpdates   bool
		wantErr        string
	}{
		{
			name:       "new materialization",
			storedSpec: nil,
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "",
		},
		{
			name:       "binding update with no changes",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "",
		},
		{
			name:       "recommend fields not included is allowed",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"locRequiredVal"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "",
		},
		{
			name:       "missing key",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "This field is a key in the current materialization",
		},
		{
			name:       "missing required non-key, non-document location",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "This location is required to be materialized",
		},
		{
			name:       "standard updates with no root document",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "",
			},
			deltaUpdates: false,
			wantErr:      "This field is the document in the current materialization",
		},
		{
			name:       "delta updates with no root document is allowed",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "",
			},
			deltaUpdates: true,
			wantErr:      "",
		},
		{
			name:       "forbidden field selected",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal", "nullVal"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "Cannot materialize this field",
		},
		{
			name:       "non-existent field selected",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "base.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal", "bogus"},
				Document: "flow_document",
			},
			deltaUpdates: false,
			wantErr:      "no such projection for field 'bogus'",
		},
		{
			name:       "root document projection change is not allowed for standard updates",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "alternate-root-projection.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "alternate_root",
			},
			deltaUpdates: false,
			wantErr:      "The root document must be materialized as field 'flow_document'",
		},
		{
			name:       "root document projection change is allowed for delta updates",
			storedSpec: loadSpec(t, "base.flow.proto"),
			newSpec:    loadSpec(t, "alternate-root-projection.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal"},
				Document: "alternate_root",
			},
			deltaUpdates: true,
			wantErr:      "",
		},
		{
			name:       "can't select multiple root projections for standard updates",
			storedSpec: nil,
			newSpec:    loadSpec(t, "multiple-root-projections.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal", "second_root"},
				Document: "first_root",
			},
			deltaUpdates: false,
			wantErr:      "only a single root document projection can be included in the field selection for a standard updates materialization",
		},
		{
			name:       "can select multiple root projections for delta updates",
			storedSpec: nil,
			newSpec:    loadSpec(t, "multiple-root-projections.flow.proto"),
			fieldSelection: pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "boolVal", "locRequiredVal", "second_root"},
				Document: "first_root",
			},
			deltaUpdates: true,
			wantErr:      "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newSpec := tt.newSpec
			newSpec.Bindings[0].DeltaUpdates = tt.deltaUpdates
			newBinding := newSpec.Bindings[0]
			newBinding.FieldSelection = tt.fieldSelection

			err := validator.ValidateSelectedFields(newBinding, tt.storedSpec)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}

	t.Run("additional tests", func(t *testing.T) {
		t.Run("delta updates to standard updates is not allowed", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			storedSpec.Bindings[0].DeltaUpdates = true
			newSpec := loadSpec(t, "base.flow.proto")

			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.ErrorContains(t, err, "from delta updates to standard updates is not allowed")
		})

		t.Run("unsatisfiable type change for a value", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			newSpec := loadSpec(t, "base.flow.proto")
			require.Equal(t, "boolVal", newSpec.Bindings[0].Collection.Projections[0].Field)
			newSpec.Bindings[0].Collection.Projections[0].Inference.Types = []string{pf.JsonTypeObject}

			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.ErrorContains(t, err, "Field 'boolVal' is already being materialized as type 'boolean' and cannot be changed to type 'object'")
		})

		t.Run("unsatisfiable type change for a key", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			newSpec := loadSpec(t, "base.flow.proto")
			require.Equal(t, "intKey", newSpec.Bindings[0].Collection.Projections[3].Field)
			newSpec.Bindings[0].Collection.Projections[3].Inference.Types = []string{pf.JsonTypeString}

			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.ErrorContains(t, err, "Field 'intKey' is already being materialized as type 'integer' and cannot be changed to type 'string'")
		})

		t.Run("forbidden type change for a value", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			newSpec := loadSpec(t, "base.flow.proto")
			require.Equal(t, "boolVal", newSpec.Bindings[0].Collection.Projections[0].Field)
			newSpec.Bindings[0].Collection.Projections[0].Inference.Types = []string{pf.JsonTypeNull}

			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.ErrorContains(t, err, "Cannot materialize this field")
		})

		t.Run("projection is removed", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			newSpec := loadSpec(t, "base.flow.proto")
			require.Equal(t, "boolVal", newSpec.Bindings[0].Collection.Projections[0].Field)
			newSpec.Bindings[0].Collection.Projections = newSpec.Bindings[0].Collection.Projections[1:]
			newSpec.Bindings[0].FieldSelection = pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "locRequiredVal"}, // Removed boolVal
				Document: "flow_document",
			}

			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.NoError(t, err)
		})

		t.Run("target conflict", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			newSpec := loadSpec(t, "base.flow.proto")
			newSpec.Bindings[0].Collection.Name = pf.Collection("other")

			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.ErrorContains(t, err, "cannot add a new binding to materialize collection 'other' to '[key_value]' because an existing binding for collection 'key/value' is already materializing to '[key_value]'")
		})

		t.Run("new binding to existing materialization", func(t *testing.T) {
			storedSpec := loadSpec(t, "base.flow.proto")
			newSpec := loadSpec(t, "new-binding.flow.proto")
			require.Equal(t, "extra/collection", newSpec.Bindings[1].Collection.Name.String())

			err := validator.ValidateSelectedFields(newSpec.Bindings[1], storedSpec)
			require.NoError(t, err)
		})
	})
}

func TestAsFormattedNumeric(t *testing.T) {
	tests := []struct {
		name         string
		inference    pf.Inference
		isPrimaryKey bool
		want         StringWithNumericFormat
	}{
		{
			name: "integer formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "number formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "nullable integer formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"null", "string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "nullable number formatted string",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"null", "string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "integer formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"integer", "string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "number formatted string with number",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"number", "string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "nullable integer formatted string with integer",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"integer", "null", "string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatInteger,
		},
		{
			name: "nullable number formatted string with number",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"null", "number", "string"},
				String_: &pf.Inference_String{
					Format: "number",
				},
			},
			isPrimaryKey: false,
			want:         StringFormatNumber,
		},
		{
			name: "doesn't apply to collection keys",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"string"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: true,
			want:         "",
		},
		{
			name: "doesn't apply to other types",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"object"},
			},
			isPrimaryKey: false,
			want:         "",
		},
		{
			name: "doesn't apply to strings with other formats",
			inference: pf.Inference{
				Exists: pf.Inference_MAY,
				Types:  []string{"null", "number", "string"},
				String_: &pf.Inference_String{
					Format: "base64",
				},
			},
			isPrimaryKey: false,
			want:         "",
		},
		{
			name: "doesn't apply if there are other types",
			inference: pf.Inference{
				Exists: pf.Inference_MUST,
				Types:  []string{"string", "object"},
				String_: &pf.Inference_String{
					Format: "integer",
				},
			},
			isPrimaryKey: false,
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formatted, ok := AsFormattedNumeric(&pf.Projection{
				Inference:    tt.inference,
				IsPrimaryKey: tt.isPrimaryKey,
			})

			require.Equal(t, tt.want, formatted)
			if tt.want == "" {
				require.False(t, ok)
			} else {
				require.True(t, ok)
			}
		})
	}
}

type testConstrainter struct{}

func (testConstrainter) Compatible(existing *pf.Projection, proposed *pf.Projection) bool {
	if !reflect.DeepEqual(existing.Inference.Types, proposed.Inference.Types) {
		return false
	}

	return reflect.DeepEqual(existing.Inference.String_, proposed.Inference.String_)
}

func (testConstrainter) DescriptionForType(p *pf.Projection) string {
	return strings.Join(p.Inference.Types, ", ")
}

func (testConstrainter) NewConstraints(p *pf.Projection, deltaUpdates bool) *pm.Response_Validated_Constraint {
	_, numericString := AsFormattedNumeric(p)

	var constraint = new(pm.Response_Validated_Constraint)
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document is required for a standard updates materialization"
	case p.Field == "locRequiredVal":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "This location is required to be materialized"
	case p.Inference.IsSingleScalarType() || numericString:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case reflect.DeepEqual(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize this field"

	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"
	}
	return constraint
}
