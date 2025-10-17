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

	is := NewInfoSchema(transformPath, transform, transform, true, false)

	nullableField := ExistingField{
		Name:               transform("nullableField"),
		Nullable:           true,
		Type:               transform("string"),
		CharacterMaxLength: 0,
	}
	loc1 := []string{"first", "location"}
	res1 := is.PushResource(transformPath(loc1)...)
	// Idempotent resource pushing.
	require.Equal(t, res1, is.PushResource(transformPath(loc1)...))

	nonNullableField := ExistingField{
		Name:               transform("nonNullableField"),
		Nullable:           false,
		Type:               transform("integer"),
		CharacterMaxLength: 0,
	}
	loc2 := []string{"second", "location"}
	res2 := is.PushResource(transformPath(loc2)...)

	res1.PushField(nullableField)
	res2.PushField(nonNullableField)

	t.Run("can't push a duplicate", func(t *testing.T) {
		require.Panics(t, func() { res1.PushField(nullableField) })
	})

	t.Run("GetField", func(t *testing.T) {
		// Gets the first field alright.
		got := res1.GetField(untransform(nullableField.Name))
		require.Equal(t, nullableField, *got)

		// Also gets the second field.
		got = res2.GetField(untransform(nonNullableField.Name))
		require.Equal(t, nonNullableField, *got)

		// Case insensitive match.
		got = res1.GetField(strings.ToLower(untransform(nullableField.Name)))
		require.Equal(t, nullableField, *got)

		// Resource doesn't have the field.
		got = res2.GetField(untransform(nullableField.Name))
		require.Nil(t, got)
	})

	t.Run("AllFields", func(t *testing.T) {
		got := res1.AllFields()
		require.Len(t, got, 1)
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

		// Case insensitive match.
		fs = pf.FieldSelection{
			Values: []string{strings.ToLower(untransform(nullableField.Name))},
		}
		got, err = is.inSelectedFields(nullableField.Name, fs)
		require.NoError(t, err)
		require.True(t, got)
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
			translate,
			false,
			false,
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

	t.Run("namespaces", func(t *testing.T) {
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
			translate,
			false,
			false,
		)

		is.PushNamespace("hello")
		require.Len(t, is.namespaces, 1)
		require.True(t, is.HasNamespace("hello"))
		require.True(t, is.HasNamespace("Hello"))
		require.False(t, is.HasNamespace("Hello_Other"))
		is.PushNamespace("hello")
		require.Len(t, is.namespaces, 1)
		is.PushNamespace("hello_other")
		require.Len(t, is.namespaces, 2)
		require.True(t, is.HasNamespace("Hello_Other"))
	})
}
