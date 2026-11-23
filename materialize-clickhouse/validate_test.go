package connector

import (
	"slices"
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestForbidUnwritableFields(t *testing.T) {
	const (
		optional    = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		recommended = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		required    = pm.Response_Validated_Constraint_FIELD_REQUIRED
		locRequired = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		forbidden   = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
	)
	type constraint struct {
		field string
		typ   pm.Response_Validated_Constraint_Type
	}

	// Projections are sorted by field, as in a built collection.
	var collection = &pf.CollectionSpec{Projections: []pf.Projection{
		{Field: "back`tick", Ptr: "/tick"},
		{Field: `key\alias`, Ptr: "/key"},
		{Field: `only\key`, Ptr: "/only"},
		{Field: `path\to`, Ptr: "/path"},
		{Field: "plain", Ptr: "/plain"},
		{Field: "plain_key", Ptr: "/key"},
	}}

	for _, tt := range []struct {
		name    string
		in      []constraint
		want    []constraint
		wantErr string
	}{
		{
			name: "plain fields are untouched",
			in:   []constraint{{"plain", recommended}, {"plain_key", locRequired}},
			want: []constraint{{"plain", recommended}, {"plain_key", locRequired}},
		},
		{
			name: "backslash and backtick fields are forbidden",
			in:   []constraint{{"back`tick", optional}, {`path\to`, recommended}, {"plain", recommended}},
			want: []constraint{{"back`tick", forbidden}, {`path\to`, forbidden}, {"plain", recommended}},
		},
		{
			name: "several constraints collapse into one",
			in:   []constraint{{`path\to`, recommended}, {`path\to`, optional}},
			want: []constraint{{`path\to`, forbidden}},
		},
		{
			name: "a required location keeps its writable projection",
			in:   []constraint{{`key\alias`, locRequired}, {"plain_key", locRequired}},
			want: []constraint{{`key\alias`, forbidden}, {"plain_key", locRequired}},
		},
		{
			name:    "a required location with no writable projection",
			in:      []constraint{{`only\key`, locRequired}},
			wantErr: "/only",
		},
		{
			name:    "a required field",
			in:      []constraint{{`path\to`, required}},
			wantErr: `path\to`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var in []*pm.Response_Validated_ProjectionConstraint
			for _, c := range tt.in {
				in = append(in, &pm.Response_Validated_ProjectionConstraint{
					Field:      c.field,
					Constraint: &pm.Response_Validated_Constraint{Type: c.typ},
				})
			}

			out, err := forbidUnwritableFields(collection, in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			var got []constraint
			for _, pc := range out {
				got = append(got, constraint{pc.Field, pc.Constraint.Type})
			}
			require.Equal(t, tt.want, got)
		})
	}
}

// TestValidateForbidsUnwritableFields checks that Validate applies
// forbidUnwritableFields to the constraints of each binding.
func TestValidateForbidsUnwritableFields(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var req = validatePartitionByReq(t, cfg, "test_validate_unwritable_fields", "", 0, nil)

	var collection = &req.Bindings[0].Collection
	collection.Projections = append(slices.Clone(collection.Projections), pf.Projection{
		Field:     `path\to`,
		Ptr:       "/path",
		Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MAY, String_: &pf.Inference_String{}},
	})
	slices.SortFunc(collection.Projections, func(a, b pf.Projection) int { return strings.Compare(a.Field, b.Field) })

	resp, err := NewDriver().Validate(t.Context(), req)
	require.NoError(t, err)

	var types []pm.Response_Validated_Constraint_Type
	for _, pc := range resp.Bindings[0].ProjectionConstraints {
		if pc.Field == `path\to` {
			types = append(types, pc.Constraint.Type)
		}
	}
	require.Equal(t, []pm.Response_Validated_Constraint_Type{pm.Response_Validated_Constraint_FIELD_FORBIDDEN}, types)
}
