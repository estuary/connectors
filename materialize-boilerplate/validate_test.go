package boilerplate

import (
	"embed"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./testdata/generate-spec-proto.sh testdata/validate_selected_fields/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_selected_fields/base-with-extra-field.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_selected_fields/alternate-root-projection.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_selected_fields/multiple-root-projections.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate_selected_fields/new-binding.flow.yaml

//go:embed testdata/validate_selected_fields/generated_specs
var specFs embed.FS

func loadSelectedFieldsSpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := specFs.ReadFile(filepath.Join("testdata/validate_selected_fields/generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

func TestValidateSelectedFields(t *testing.T) {
	t.Run("field selection changes", func(t *testing.T) {
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
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "base.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "alternate-root-projection.flow.proto"),
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
				storedSpec: loadSelectedFieldsSpec(t, "base.flow.proto"),
				newSpec:    loadSelectedFieldsSpec(t, "alternate-root-projection.flow.proto"),
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
				newSpec:    loadSelectedFieldsSpec(t, "multiple-root-projections.flow.proto"),
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
				newSpec:    loadSelectedFieldsSpec(t, "multiple-root-projections.flow.proto"),
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
				validator := NewValidator(testConstrainter{}, BasicInfoFromSpec(t, tt.storedSpec, testMapType))
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
	})

	t.Run("other kinds of changes", func(t *testing.T) {
		tests := []struct {
			name        string
			mutateSpecs func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec)
			wantErr     string
		}{
			{
				name: "delta updates to standard updates is not allowed",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					stored.Bindings[0].DeltaUpdates = true
					return stored, new
				},
				wantErr: "from delta updates to standard updates is not allowed",
			},
			{
				name: "unsatisfiable type change for a value",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					require.Equal(t, "boolVal", new.Bindings[0].Collection.Projections[0].Field)
					new.Bindings[0].Collection.Projections[0].Inference.Types = []string{pf.JsonTypeObject}
					return stored, new
				},
				wantErr: "Field 'boolVal' is already being materialized as endpoint type 'boolean' and cannot be changed to type 'object'",
			},
			{
				name: "unsatisfiable type change for a key",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					require.Equal(t, "intKey", new.Bindings[0].Collection.Projections[3].Field)
					new.Bindings[0].Collection.Projections[3].Inference.Types = []string{pf.JsonTypeString}
					return stored, new
				},
				wantErr: "Field 'intKey' is already being materialized as endpoint type 'integer' and cannot be changed to type 'string'",
			},
			{
				name: "unsatisfiable type change for a value with backfill counter increment",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					require.Equal(t, "boolVal", new.Bindings[0].Collection.Projections[0].Field)
					new.Bindings[0].Collection.Projections[0].Inference.Types = []string{pf.JsonTypeObject}
					new.Bindings[0].Backfill = new.Bindings[0].Backfill + 1
					return stored, new
				},
				wantErr: "",
			},
			{
				name: "backfill counter decrement is not allowed",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					stored.Bindings[0].Backfill = 2
					new.Bindings[0].Backfill = 1
					return stored, new
				},
				wantErr: "validating binding for collection 'key/value': backfill count 1 is less than previously applied count of 2",
			},
			{
				name: "forbidden type change for a value",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					require.Equal(t, "boolVal", new.Bindings[0].Collection.Projections[0].Field)
					new.Bindings[0].Collection.Projections[0].Inference.Types = []string{pf.JsonTypeNull}
					return stored, new
				},
				wantErr: "Cannot materialize this field",
			},
			{
				name: "projection is removed from field selection",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					require.Equal(t, "boolVal", new.Bindings[0].Collection.Projections[0].Field)
					new.Bindings[0].Collection.Projections = new.Bindings[0].Collection.Projections[1:]
					new.Bindings[0].FieldSelection = pf.FieldSelection{
						Keys:     []string{"stringKey", "intKey"},
						Values:   []string{"stringFormatIntVal", "locRequiredVal"}, // Removed boolVal
						Document: "flow_document",
					}
					return stored, new
				},
				wantErr: "",
			},
			{
				name: "target conflict",
				mutateSpecs: func(stored, new *pf.MaterializationSpec) (*pf.MaterializationSpec, *pf.MaterializationSpec) {
					new.Bindings[0].Collection.Name = pf.Collection("other")
					return stored, new
				},
				wantErr: "cannot add a new binding to materialize collection 'other' to '[key_value]' because an existing binding for collection 'key/value' is already materializing to '[key_value]'",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				storedSpec, newSpec := tt.mutateSpecs(loadSelectedFieldsSpec(t, "base.flow.proto"), loadSelectedFieldsSpec(t, "base.flow.proto"))
				validator := NewValidator(testConstrainter{}, BasicInfoFromSpec(t, storedSpec, testMapType))

				err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
				if tt.wantErr == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tt.wantErr)
				}
			})
		}
	})

	t.Run("modified destination tests", func(t *testing.T) {
		// These tests use a different spec for the InfoSchema, as a proxy for representing a
		// materialized destination resource that is different than the field selection in some way.
		t.Run("projection is removed from destination but not field selection", func(t *testing.T) {
			storedSpec := loadSelectedFieldsSpec(t, "base.flow.proto")
			newSpec := loadSelectedFieldsSpec(t, "base.flow.proto")
			isSpec := loadSelectedFieldsSpec(t, "base.flow.proto")
			require.Equal(t, "boolVal", isSpec.Bindings[0].Collection.Projections[0].Field)
			isSpec.Bindings[0].Collection.Projections = isSpec.Bindings[0].Collection.Projections[1:]
			isSpec.Bindings[0].FieldSelection = pf.FieldSelection{
				Keys:     []string{"stringKey", "intKey"},
				Values:   []string{"stringFormatIntVal", "locRequiredVal"}, // Removed boolVal
				Document: "flow_document",
			}

			validator := NewValidator(testConstrainter{}, BasicInfoFromSpec(t, isSpec, testMapType))
			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.NoError(t, err)
		})

		t.Run("field exists in destination but not in field selection", func(t *testing.T) {
			storedSpec := loadSelectedFieldsSpec(t, "base.flow.proto")
			newSpec := loadSelectedFieldsSpec(t, "base.flow.proto")
			isSpec := loadSelectedFieldsSpec(t, "base-with-extra-field.flow.proto")

			validator := NewValidator(testConstrainter{}, BasicInfoFromSpec(t, isSpec, testMapType))
			err := validator.ValidateSelectedFields(newSpec.Bindings[0], storedSpec)
			require.NoError(t, err)
		})
	})

	t.Run("new binding to existing materialization", func(t *testing.T) {
		storedSpec := loadSelectedFieldsSpec(t, "base.flow.proto")
		newSpec := loadSelectedFieldsSpec(t, "new-binding.flow.proto")
		require.Equal(t, "extra/collection", newSpec.Bindings[1].Collection.Name.String())

		validator := NewValidator(testConstrainter{}, BasicInfoFromSpec(t, storedSpec, testMapType))
		err := validator.ValidateSelectedFields(newSpec.Bindings[1], storedSpec)
		require.NoError(t, err)
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

func (testConstrainter) Compatible(existing EndpointField, proposed *pf.Projection, _ json.RawMessage) (bool, error) {
	return existing.Type == strings.Join(proposed.Inference.Types, ","), nil
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
