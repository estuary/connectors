package boilerplate

import (
	"embed"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./testdata/generate-spec-proto.sh testdata/validate/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/incompatible-changes.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/fewer-fields.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/alternate-root.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/increment-backfill.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/ambiguous-fields.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/ambiguous-fields-incompatible.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/nullable-key.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/long-fields.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/key-subset.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/validate/ambiguous-key.flow.yaml

//go:embed testdata/validate/generated_specs
var validateFS embed.FS

func loadValidateSpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := validateFS.ReadFile(filepath.Join("testdata/validate/generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

func TestValidate(t *testing.T) {
	type testCase struct {
		name               string
		deltaUpdates       bool
		specForInfoSchema  *pf.MaterializationSpec
		existingSpec       *pf.MaterializationSpec
		proposedSpec       *pf.MaterializationSpec
		fieldNameTransform func(string) string
		maxFieldLength     int
		featureFlags       map[string]bool
	}

	tests := []testCase{
		{
			name:               "new materialization - standard updates",
			deltaUpdates:       false,
			specForInfoSchema:  nil,
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "base.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "same binding again - standard updates",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "base.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "new materialization - delta updates",
			deltaUpdates:       true,
			specForInfoSchema:  nil,
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "base.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "same binding again - delta updates",
			deltaUpdates:       true,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "base.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "binding update with incompatible changes",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "incompatible-changes.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "fields exist in destination but not in collection",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "fewer-fields.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "change root document projection for standard updates",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "alternate-root.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "change root document projection for delta updates",
			deltaUpdates:       true,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "alternate-root.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "increment backfill counter",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "increment-backfill.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "increment backfill counter for disabled -> enabled binding",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       specWithBindingsDisabled(loadValidateSpec(t, "base.flow.proto")),
			proposedSpec:       loadValidateSpec(t, "increment-backfill.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "table already exists with identical spec",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "base.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
			featureFlags:       map[string]bool{"allow_existing_tables_for_new_bindings": true},
		},
		{
			name:               "table already exists with incompatible proposed spec",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "incompatible-changes.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
			featureFlags:       map[string]bool{"allow_existing_tables_for_new_bindings": true},
		},
		{
			name:               "new materialization with ambiguous fields",
			deltaUpdates:       false,
			specForInfoSchema:  nil,
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			fieldNameTransform: ambiguousTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "table already exists with a column for an ambiguous field for a new materialization",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			fieldNameTransform: ambiguousTestTransform,
			maxFieldLength:     0,
			featureFlags:       map[string]bool{"allow_existing_tables_for_new_bindings": true},
		},
		{
			name:               "table already exists with a key column for an ambiguous field for a new materialization",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "ambiguous-key.flow.proto"),
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "ambiguous-key.flow.proto"),
			fieldNameTransform: ambiguousTestTransform,
			maxFieldLength:     0,
			featureFlags:       map[string]bool{"allow_existing_tables_for_new_bindings": true},
		},
		{
			name:               "update an existing materialization with ambiguous fields",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			existingSpec:       loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			fieldNameTransform: ambiguousTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "update an existing materialization with ambiguous fields in an incompatbile way",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			existingSpec:       loadValidateSpec(t, "ambiguous-fields.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "ambiguous-fields-incompatible.flow.proto"),
			fieldNameTransform: ambiguousTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "field names over the length limit are forbidden",
			deltaUpdates:       false,
			specForInfoSchema:  nil,
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "long-fields.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     20,
		},
		{
			name:               "can materialize a subset of key fields",
			deltaUpdates:       false,
			specForInfoSchema:  nil,
			existingSpec:       nil,
			proposedSpec:       loadValidateSpec(t, "key-subset.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
		{
			name:               "cannot add or remove selected key fields for standard updates",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "key-subset.flow.proto"),
			existingSpec:       loadValidateSpec(t, "key-subset.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "key-subset.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
	}

	var snap strings.Builder
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := testInfoSchemaFromSpec(t, tt.specForInfoSchema, tt.fieldNameTransform)
			validator := NewValidator(testConstrainter{}, is, tt.maxFieldLength, true, tt.featureFlags)

			cs, err := validator.ValidateBinding(
				[]string{"key_value"},
				tt.deltaUpdates,
				tt.proposedSpec.Bindings[0].Backfill,
				tt.proposedSpec.Bindings[0].Collection,
				tt.proposedSpec.Bindings[0].FieldSelection.FieldConfigJsonMap,
				tt.existingSpec,
			)
			require.NoError(t, err)

			snap.WriteString(tt.name + ":\n")
			snap.WriteString(SnapshotConstraints(t, cs) + "\n")
		})
	}
	cupaloy.SnapshotT(t, snap.String())

	// Test the new feature flag behavior for existing tables
	t.Run("new binding with existing table - feature flag disabled (default)", func(t *testing.T) {
		// Table exists but no lastBinding (new binding) - should error
		is := testInfoSchemaFromSpec(t, loadValidateSpec(t, "base.flow.proto"), simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil) // No feature flags

		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			loadValidateSpec(t, "base.flow.proto").Bindings[0].Backfill,
			loadValidateSpec(t, "base.flow.proto").Bindings[0].Collection,
			loadValidateSpec(t, "base.flow.proto").Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil, // No existing spec (new binding)
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "already exists for new binding")
		require.Contains(t, err.Error(), "allow_existing_tables_for_new_bindings")
	})

	t.Run("new binding with existing table - feature flag enabled", func(t *testing.T) {
		// Table exists but no lastBinding (new binding) - should succeed with feature flag
		is := testInfoSchemaFromSpec(t, loadValidateSpec(t, "base.flow.proto"), simpleTestTransform)
		featureFlags := map[string]bool{"allow_existing_tables_for_new_bindings": true}
		validator := NewValidator(testConstrainter{}, is, 0, true, featureFlags)

		cs, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			loadValidateSpec(t, "base.flow.proto").Bindings[0].Backfill,
			loadValidateSpec(t, "base.flow.proto").Bindings[0].Collection,
			loadValidateSpec(t, "base.flow.proto").Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil, // No existing spec (new binding)
		)
		require.NoError(t, err)
		require.NotNil(t, cs)
	})

	t.Run("folded fields are set when field names are transformed", func(t *testing.T) {
		proposed := loadValidateSpec(t, "base.flow.proto")

		// Use the simpleTestTransform which appends "_transformed" to field names
		is := testInfoSchemaFromSpec(t, nil, simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil)

		cs, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil,
		)
		require.NoError(t, err)

		// Verify that folded fields are set for all constraints
		for field, constraint := range cs {
			// simpleTestTransform adds "_transformed" to all field names
			expectedFolded := field + "_transformed"
			require.Equal(t, expectedFolded, constraint.FoldedField,
				"FoldedField should be set to the transformed field name for field %q", field)
		}
	})

	t.Run("folded fields are not set when field names are unchanged", func(t *testing.T) {
		proposed := loadValidateSpec(t, "base.flow.proto")

		// Use identity transform (no change to field names)
		identityTransform := func(s string) string { return s }
		is := testInfoSchemaFromSpec(t, nil, identityTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil)

		cs, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil,
		)
		require.NoError(t, err)

		// Verify that folded fields are empty when no transformation occurs
		for field, constraint := range cs {
			require.Empty(t, constraint.FoldedField,
				"FoldedField should be empty when field name is unchanged for field %q", field)
		}
	})

	t.Run("at least one required location must not be too long", func(t *testing.T) {
		proposed := loadValidateSpec(t, "long-fields.flow.proto")

		require.Equal(t, "key", proposed.Bindings[0].Collection.Projections[3].Field)
		proposed.Bindings[0].Collection.Projections[3].Field = "keyRenamedToSomethingThatIsReallyLong"

		is := testInfoSchemaFromSpec(t, nil, simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 20, true, nil)

		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil,
		)

		require.ErrorContains(t, err, "at least one field from location '/key' is required to be materialized, but all projections exceed the maximum length allowable by the destination")
	})

	t.Run("a required field cannot be too long", func(t *testing.T) {
		proposed := loadValidateSpec(t, "long-fields.flow.proto")

		// This is somewhat contrived because currently the only way a field can be required is if
		// it has already been materialized, and is either the root document projection or a
		// collection key.
		is := testInfoSchemaFromSpec(t, proposed, simpleTestTransform)
		featureFlags := map[string]bool{"allow_existing_tables_for_new_bindings": true}
		validator := NewValidator(testConstrainter{}, is, 20, true, featureFlags)

		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil,
		)

		require.ErrorContains(t, err, "field 'longKeyLongKeyLongKey' is required to be materialized but has a length of 21 which exceeds the maximum length allowable by the destination")
	})

	t.Run("can't switch from delta to standard updates", func(t *testing.T) {
		existing := loadValidateSpec(t, "base.flow.proto")
		proposed := loadValidateSpec(t, "base.flow.proto")

		existing.Bindings[0].DeltaUpdates = true

		is := testInfoSchemaFromSpec(t, existing, simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil)

		// Enabled binding.
		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			existing,
		)

		require.ErrorContains(t, err, "changing from delta updates to standard updates is not allowed")

		// Disabled -> enabled binding also errors.
		_, err2 := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			specWithBindingsDisabled(existing),
		)

		require.Equal(t, err, err2)
	})

	t.Run("can switch from delta updates to standard updates", func(t *testing.T) {
		existing := loadValidateSpec(t, "base.flow.proto")
		proposed := loadValidateSpec(t, "base.flow.proto")

		proposed.Bindings[0].DeltaUpdates = true

		is := testInfoSchemaFromSpec(t, existing, simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil)

		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			true,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			existing,
		)

		require.NoError(t, err)
	})

	t.Run("can't materialize a nullable collection key with no default value", func(t *testing.T) {
		proposed := loadValidateSpec(t, "nullable-key.flow.proto")

		require.Equal(t, "key", proposed.Bindings[0].Collection.Projections[3].Field)
		proposed.Bindings[0].Collection.Projections[3].Inference.DefaultJson = nil

		is := testInfoSchemaFromSpec(t, nil, simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil)

		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil,
		)

		require.ErrorContains(t, err, "cannot materialize collection 'key/value' with nullable key field 'key' unless it has a default value annotation")
	})

	t.Run("can materialize a nullable collection key with a default value", func(t *testing.T) {
		proposed := loadValidateSpec(t, "nullable-key.flow.proto")

		is := testInfoSchemaFromSpec(t, nil, simpleTestTransform)
		validator := NewValidator(testConstrainter{}, is, 0, true, nil)

		_, err := validator.ValidateBinding(
			[]string{"key_value"},
			false,
			proposed.Bindings[0].Backfill,
			proposed.Bindings[0].Collection,
			proposed.Bindings[0].FieldSelection.FieldConfigJsonMap,
			nil,
		)

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

func (testConstrainter) Compatible(existing ExistingField, proposed *pf.Projection, _ json.RawMessage) (bool, error) {
	return existing.Type == strings.Join(proposed.Inference.Types, ","), nil
}

func (testConstrainter) DescriptionForType(p *pf.Projection, _ json.RawMessage) (string, error) {
	return strings.Join(p.Inference.Types, ", "), nil
}

func (testConstrainter) NewConstraints(p *pf.Projection, deltaUpdates bool, _ json.RawMessage) (*pm.Response_Validated_Constraint, error) {
	_, numericString := AsFormattedNumeric(p)

	var constraint = new(pm.Response_Validated_Constraint)
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "All Locations that are part of the collections key are recommended"
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
	return constraint, nil
}

func specWithBindingsDisabled(spec *pf.MaterializationSpec) *pf.MaterializationSpec {
	// Simulates a prior spec with all bindings inactive, to see transitions
	// from disabled -> enabled bindings.
	bindings := spec.Bindings
	spec.Bindings = nil
	spec.InactiveBindings = bindings
	return spec
}
