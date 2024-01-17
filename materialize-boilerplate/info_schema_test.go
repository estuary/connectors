package boilerplate

import (
	"strings"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestInfoSchema(t *testing.T) {
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

	// This is a convenience for testing - typically these endpoint-specific transformations are
	// lossy and one-way.
	untransform := func(in string) string {
		return strings.TrimSuffix(in, "_transformed")
	}

	is := NewInfoSchema(transformPath, transform)

	nullableField := EndpointField{
		Name:               transform("nullableField"),
		Nullable:           true,
		Type:               transform("string"),
		CharacterMaxLength: 0,
	}
	loc1 := []string{"first", "location"}

	nonNullableField := EndpointField{
		Name:               transform("nonNullableField"),
		Nullable:           false,
		Type:               transform("integer"),
		CharacterMaxLength: 0,
	}
	loc2 := []string{"second", "location"}

	is.PushField(nullableField, transformPath(loc1)...)
	is.PushField(nonNullableField, transformPath(loc2)...)

	t.Run("can't push a duplicate", func(t *testing.T) {
		require.Panics(t, func() { is.PushField(nullableField, transformPath(loc1)...) })
	})

	t.Run("GetField", func(t *testing.T) {
		// Gets the first field alright.
		got, err := is.GetField(loc1, untransform(nullableField.Name))
		require.NoError(t, err)
		require.Equal(t, nullableField, got)

		// Also gets the second field.
		got, err = is.GetField(loc2, untransform(nonNullableField.Name))
		require.NoError(t, err)
		require.Equal(t, nonNullableField, got)

		// Wrong path.
		got, err = is.GetField(loc2, untransform(nullableField.Name))
		require.Equal(t, EndpointField{}, got)
		require.Error(t, err)

		// Right path but wrong field.
		got, err = is.GetField(loc1, untransform(nullableField.Name)+"d'oh")
		require.Equal(t, EndpointField{}, got)
		require.Error(t, err)

		// Bogus path.
		got, err = is.GetField([]string{"d'", "oh"}, untransform(nullableField.Name))
		require.Equal(t, EndpointField{}, got)
		require.Error(t, err)
	})

	t.Run("HasField", func(t *testing.T) {
		require.True(t, is.HasField(loc1, untransform(nullableField.Name)))
		require.False(t, is.HasField([]string{"d'", "oh"}, untransform(nullableField.Name)))
		require.False(t, is.HasField(loc1, untransform(nullableField.Name)+"d'oh"))
	})

	t.Run("FieldsForResource", func(t *testing.T) {
		// Exists.
		got, err := is.FieldsForResource(loc1)
		require.NoError(t, err)
		require.Len(t, got, 1)

		// Doesn't exist - wrong resource path.
		got, err = is.FieldsForResource([]string{"d'", "oh"})
		require.Error(t, err)
		require.Nil(t, got)
	})

	t.Run("InSelectedFields", func(t *testing.T) {
		fs := pf.FieldSelection{
			Values: []string{untransform(nullableField.Name)},
		}

		// Is in the selected fields.
		got, err := is.inSelectedFields(nullableField.Name, fs)
		require.NoError(t, err)
		require.True(t, got)

		// Isn't in the selected fields.
		got, err = is.inSelectedFields(nonNullableField.Name, fs)
		require.NoError(t, err)
		require.False(t, got)

		// Ambiguous case.
		fs = pf.FieldSelection{
			Values: []string{untransform(nullableField.Name), untransform(nullableField.Name)},
		}
		got, err = is.inSelectedFields(nullableField.Name, fs)
		require.Error(t, err)
		require.False(t, got)
	})

	t.Run("ExtractProjection", func(t *testing.T) {
		p := pf.Projection{Field: untransform(nullableField.Name)}

		collection := pf.CollectionSpec{
			Projections: []pf.Projection{p},
		}

		// Projection exists.
		proj, found, err := is.extractProjection(nullableField.Name, collection)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, p, proj)

		// Projection doesn't exist.
		proj, found, err = is.extractProjection(nonNullableField.Name, collection)
		require.NoError(t, err)
		require.False(t, found)
		require.Equal(t, pf.Projection{}, proj)

		// Ambiguous case.
		collection.Projections = append(collection.Projections, p)
		proj, found, err = is.extractProjection(nullableField.Name, collection)
		require.Error(t, err)
		require.False(t, found)
		require.Equal(t, pf.Projection{}, proj)
	})

	t.Run("AmbiguousResourcePaths", func(t *testing.T) {
		translate := func(in string) string {
			return strings.ToLower(in)
		}

		is := NewInfoSchema(
			func(in []string) []string {
				out := make([]string, 0, len(in))
				for _, i := range in {
					out = append(out, translate(i))
				}
				return out
			},
			translate,
		)

		paths := [][]string{
			{"schema", "table"},
			{"Schema", "table"},
			{"schema", "TABLE"},
			{"other", "table"},
			{"other", "otherTable"},
			{"yetAnother", "table"},
			{"alt", "TABLE"},
			{"ALT", "table"},
		}

		want := [][]string{
			{"alt", "TABLE"},
			{"ALT", "table"},
			{"schema", "table"},
			{"Schema", "table"},
			{"schema", "TABLE"},
		}

		require.Equal(t, want, is.AmbiguousResourcePaths(paths))
	})
}
