package sql

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestValidateMigrations(t *testing.T) {
	type testCase struct {
		name               string
		deltaUpdates       bool
		specForInfoSchema  *pf.MaterializationSpec
		existingSpec       *pf.MaterializationSpec
		proposedSpec       *pf.MaterializationSpec
		fieldNameTransform func(string) string
		maxFieldLength     int
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
			name:               "binding update with migratable changes",
			deltaUpdates:       false,
			specForInfoSchema:  loadValidateSpec(t, "base.flow.proto"),
			existingSpec:       loadValidateSpec(t, "base.flow.proto"),
			proposedSpec:       loadValidateSpec(t, "migratable-changes.flow.proto"),
			fieldNameTransform: simpleTestTransform,
			maxFieldLength:     0,
		},
	}

	var snap strings.Builder
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := testInfoSchemaFromSpec(t, tt.specForInfoSchema, tt.fieldNameTransform)
			validator := boilerplate.NewValidator(testConstrainter{}, is, tt.maxFieldLength, true, nil)

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
			snap.WriteString(boilerplate.SnapshotConstraints(t, cs) + "\n")
		})
	}
	cupaloy.SnapshotT(t, snap.String())
}

type testConstrainter struct{}

func (testConstrainter) Compatible(existing boilerplate.ExistingField, proposed *pf.Projection, _ json.RawMessage) (bool, error) {
	var migratable = (existing.Type == "integer,string" && strings.Join(proposed.Inference.Types, ",") == "string") ||
		(existing.Type == "integer" && proposed.Inference.Types[0] == "number")
	return existing.Type == strings.Join(proposed.Inference.Types, ",") || migratable, nil
}

func (testConstrainter) DescriptionForType(p *pf.Projection, _ json.RawMessage) (string, error) {
	return strings.Join(p.Inference.Types, ", "), nil
}

func (testConstrainter) NewConstraints(p *pf.Projection, deltaUpdates bool, _ json.RawMessage) (*pm.Response_Validated_Constraint, error) {
	_, numericString := boilerplate.AsFormattedNumeric(p)

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
	return constraint, nil
}

func simpleTestTransform(in string) string {
	return in + "_transformed"
}

// testInfoSchemaFromSpec constructs a mock InfoSchema from a spec that represents an existing table
// with all the fields from the field selection.
func testInfoSchemaFromSpec(t *testing.T, s *pf.MaterializationSpec, transform func(string) string) *boilerplate.InfoSchema {
	t.Helper()

	transformPath := func(in []string) []string {
		out := make([]string, 0, len(in))
		for _, p := range in {
			out = append(out, transform(p))
		}
		return out
	}

	is := boilerplate.NewInfoSchema(transformPath, transform)

	if s == nil || len(s.Bindings) == 0 {
		return is
	}

	for _, b := range s.Bindings {
		res := is.PushResource(transformPath(b.ResourcePath)...)
		for _, f := range b.FieldSelection.AllFields() {
			proj := *b.Collection.GetProjection(f)

			res.PushField(boilerplate.ExistingField{
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
