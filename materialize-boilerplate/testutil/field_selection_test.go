package testutil

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

// TestSelectedFields covers how the harness emulates control-plane field
// selection from a binding's projection_constraints, including multiple
// constraints per field. Notably a field that is INCOMPATIBLE but also required
// is still selected (presuming a backfill), matching build_selection in flow's
// field_selection.rs.
func TestSelectedFields(t *testing.T) {
	c := func(typ pm.Response_Validated_Constraint_Type) *pm.Response_Validated_Constraint {
		return &pm.Response_Validated_Constraint{Type: typ}
	}

	// Projections must be sorted by field for CollectionSpec.GetProjection.
	collection := pf.CollectionSpec{
		Projections: []pf.Projection{
			{Field: "bare_incompat", Ptr: "/bare_incompat"},
			{Field: "doc", Ptr: ""}, // root document projection
			{Field: "forbidden", Ptr: "/forbidden"},
			{Field: "id", Ptr: "/id", IsPrimaryKey: true},
			{Field: "opt", Ptr: "/opt"},
			{Field: "recommended", Ptr: "/recommended"},
		},
	}

	binding := &pm.Response_Validated_Binding{
		ProjectionConstraints: []*pm.Response_Validated_ProjectionConstraint{
			{Field: "id", Constraint: c(pm.Response_Validated_Constraint_LOCATION_REQUIRED)},
			// Required AND incompatible: must still be selected as the document.
			{Field: "doc", Constraint: c(pm.Response_Validated_Constraint_LOCATION_REQUIRED)},
			{Field: "doc", Constraint: c(pm.Response_Validated_Constraint_INCOMPATIBLE)},
			{Field: "recommended", Constraint: c(pm.Response_Validated_Constraint_LOCATION_RECOMMENDED)},
			{Field: "opt", Constraint: c(pm.Response_Validated_Constraint_FIELD_OPTIONAL)},
			{Field: "forbidden", Constraint: c(pm.Response_Validated_Constraint_FIELD_FORBIDDEN)},
			// Incompatible with no accompanying requirement: dropped.
			{Field: "bare_incompat", Constraint: c(pm.Response_Validated_Constraint_INCOMPATIBLE)},
		},
	}

	t.Run("without optionals", func(t *testing.T) {
		got := selectedFields(binding, collection, false)
		require.Equal(t, []string{"id"}, got.Keys)
		require.Equal(t, "doc", got.Document)
		require.Equal(t, []string{"recommended"}, got.Values)
	})

	t.Run("with optionals", func(t *testing.T) {
		got := selectedFields(binding, collection, true)
		require.Equal(t, []string{"id"}, got.Keys)
		require.Equal(t, "doc", got.Document)
		require.Equal(t, []string{"opt", "recommended"}, got.Values)
	})
}
