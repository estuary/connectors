package sql

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDialect() Dialect {
	mapper := NewDDLMapper(
		FlatTypeMappings{
			ARRAY:          MapStatic("JSON"),
			BINARY:         MapStatic("BYTEA"),
			BOOLEAN:        MapStatic("BOOLEAN"),
			INTEGER:        MapStatic("BIGINT"),
			MULTIPLE:       MapStatic("JSON"),
			NUMBER:         MapStatic("DOUBLE PRECISION"),
			OBJECT:         MapStatic("JSON"),
			STRING_INTEGER: MapStatic("NUMERIC"),
			STRING_NUMBER:  MapStatic("DECIMAL"),
			STRING: MapString(StringMappings{
				Fallback: MapStatic("TEXT"),
				WithFormat: map[string]MapProjectionFn{
					"date-time": MapStatic("TIMESTAMPTZ"),
				},
			}),
		},
		WithNotNullSuffix("NOT NULL"),
	)

	return Dialect{
		TableLocatorer: TableLocatorFn(func(path []string) InfoTableLocation {
			return InfoTableLocation{TableSchema: path[1], TableName: path[2]}
		}),
		ColumnLocatorer: ColumnLocatorFn(func(field string) string { return field }),
		Identifierer: IdentifierFn(JoinTransform(".",
			PassThroughTransform(
				func(s string) bool {
					return IsSimpleIdentifier(s) && strings.ToLower(s) != "reserved"
				},
				QuoteTransform("\"", "\\\""),
			))),
		Literaler: ToLiteralFn(QuoteTransform("'", "''")),
		Placeholderer: PlaceholderFn(func(index int) string {
			return fmt.Sprintf("$%d", index+1)
		}),
		TypeMapper: mapper,
	}
}

func TestTableTemplate(t *testing.T) {
	var (
		shape      = FlowCheckpointsTable([]string{"one", "reserved"})
		dialect    = newTestDialect()
		table, err = ResolveTable(*shape, dialect)
	)
	assert.NoError(t, err)

	var tpl = MustParseTemplate(dialect, "template", `
	CREATE TABLE {{$.Identifier}} (
		{{- range $ind, $col := $.Columns }}
			{{- if $ind}},{{end}}
			{{$col.Identifier}} {{$col.DDL}}
		{{- end }}
		{{- if not $.DeltaUpdates }},
			PRIMARY KEY (
		{{- range $ind, $key := $.Keys }}
			{{- if $ind}}, {{end -}}
			{{$key.Identifier}}
		{{- end -}}
		)
		{{ end }}
	);

	COMMENT ON TABLE {{$.Identifier}} IS {{Literal $.Comment}};
	{{- range $col := .Columns }}
	COMMENT ON COLUMN {{$.Identifier}}.{{$col.Identifier}} IS {{Literal $col.Comment}};
	{{- end}}
	`)

	out, err := RenderTableTemplate(table, tpl)
	require.NoError(t, err)
	cupaloy.SnapshotT(t, out)
}

func TestMergeBoundsBuilder(t *testing.T) {
	literaler := ToLiteralFn(QuoteTransform("'", "''"))

	colA := Column{Identifier: "colA"}
	colB := Column{Identifier: "colB", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{
			Types: []string{"boolean"},
		},
	}}}
	colC := Column{Identifier: "colC"}

	for _, tt := range []struct {
		name       string
		keyColumns []Column
		keys       [][]any
		want       MergeBounds
	}{
		{
			name:       "single key",
			keyColumns: []Column{colA},
			keys: [][]any{
				{"a"},
				{"b"},
				{"c"},
			},
			want: []MergeBound{
				{colA, literaler("a"), literaler("c"), nil},
			},
		},
		{
			name:       "multiple keys ordered",
			keyColumns: []Column{colA, colB, colC},
			keys: [][]any{
				{"a", true, int64(1)},
				{"b", false, int64(2)},
				{"c", true, int64(3)},
			},
			want: []MergeBound{
				{colA, literaler("a"), literaler("c"), nil},
				{colB, "", "", nil},
				{colC, literaler(int64(1)), literaler(int64(3)), nil},
			},
		},
		{
			name:       "multiple keys unordered",
			keyColumns: []Column{colA, colB, colC},
			keys: [][]any{
				{"a", true, int64(3)},
				{"b", false, int64(1)},
				{"c", true, int64(2)},
			},
			want: []MergeBound{
				{colA, literaler("a"), literaler("c"), nil},
				{colB, "", "", nil},
				{colC, literaler(int64(1)), literaler(int64(3)), nil},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := NewMergeBoundsBuilder(tt.keyColumns, literaler)
			for _, store := range tt.keys {
				b.NextKey(store)
			}
			require.Equal(t, tt.want, b.Build())
		})
	}
}

func TestRootLevelColumns(t *testing.T) {
	// Create test columns with different pointer paths
	columns := []Column{
		{Projection: Projection{Projection: pf.Projection{Field: "root_field", Ptr: "/root_field"}}},
		{Projection: Projection{Projection: pf.Projection{Field: "nested_field", Ptr: "/nested/field"}}},
		{Projection: Projection{Projection: pf.Projection{Field: "another_root", Ptr: "/another_root"}}},
		{Projection: Projection{Projection: pf.Projection{Field: "deep_nested", Ptr: "/deep/nested/field"}}},
		{Projection: Projection{Projection: pf.Projection{Field: "root_array", Ptr: "/root_array"}}},
	}

	// Create a table with these columns
	table := Table{
		Keys:   []Column{columns[0]}, // root_field as key
		Values: columns[1:],          // rest as values
	}

	// Call RootLevelColumns method
	rootLevelCols := table.RootLevelColumns()

	// Verify only root-level columns are returned
	expectedFields := []string{"root_field", "another_root", "root_array"}
	actualFields := make([]string, len(rootLevelCols))
	for i, col := range rootLevelCols {
		actualFields[i] = col.Field
	}

	require.Equal(t, 3, len(rootLevelCols), "Expected 3 root-level columns")
	require.ElementsMatch(t, expectedFields, actualFields, "Root-level columns should match expected fields")

	// Verify each returned column is indeed root-level
	for _, col := range rootLevelCols {
		require.True(t, strings.Count(col.Ptr, "/") == 1, "Column %s should be root-level", col.Field)
	}
}

func TestRootPropertyColumns(t *testing.T) {
	var col = func(field, ptr string, mustExist bool, types ...string) Column {
		return Column{
			Projection: Projection{Projection: pf.Projection{Field: field, Ptr: ptr, Inference: pf.Inference{Types: types}}},
			MustExist:  mustExist,
		}
	}

	table := Table{
		Keys: []Column{col("key", "/key", true, "string")},
		Values: []Column{
			col("field_with_projection", "/fieldWithProjection", false, "string"),
			col("fieldWithProjection", "/fieldWithProjection", false, "string"),
			col("a/b~c", "/a~1b~0c", false, "string", "null"),
			col("escapes", "/~01", false, "integer"),
			col("nested/field", "/nested/field", false, "string"),
		},
	}

	var props []string
	for _, c := range table.RootPropertyColumns() {
		props = append(props, c.Field+" => "+c.RootProperty())
	}
	require.Equal(t, []string{
		"key => key",
		"field_with_projection => fieldWithProjection",
		"a/b~c => a/b~c",
		"escapes => ~1",
	}, props)

	require.Equal(t, []string{"fieldWithProjection", "~1"}, table.NullablePropertiesToStrip())
}

func TestMergeBoundsBuilderDatetimeKeys(t *testing.T) {
	literaler := ToLiteralFn(QuoteTransform("'", "''"))
	datetime := Column{Identifier: "ts", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}},
	}}}
	id := Column{Identifier: "id"}
	exact := []MergeBoundsOption{WithExactDatetimeBounds(func(Column) bool { return true })}
	declined := []MergeBoundsOption{WithExactDatetimeBounds(func(Column) bool { return false })}

	// Valid RFC3339 forms of three instants, in neither text nor time order.
	keys := [][]any{
		{"2025-05-05T02:00:00+02:00", "k"}, // 00:00:00Z
		{"2025-05-05T00:00:01.5Z", "k"},    // the latest
		{"2025-05-04T23:59:59.999999999Z", "k"},
		{"2025-05-05T00:00:00.000Z", "k"},  // 00:00:00Z again, different precision
		{"2025-05-04T22:59:59-01:00", "k"}, // 23:59:59Z, the earliest
	}
	withKey := func(k ...any) [][]any { return append(append([][]any{}, keys...), k) }

	for _, tt := range []struct {
		name string
		opts []MergeBoundsOption
		keys [][]any
		want MergeBound
	}{
		{"dates by default", nil, keys, MergeBound{datetime, literaler("2025-05-03"), literaler("2025-05-07"), nil}},
		{"dates when the dialect declines exact bounds", declined, keys, MergeBound{datetime, literaler("2025-05-03"), literaler("2025-05-07"), nil}},
		{"exact bounds keep the original literals", exact, keys,
			MergeBound{datetime, literaler("2025-05-04T22:59:59-01:00"), literaler("2025-05-05T00:00:01.5Z"), nil}},
		{"a single key bounds itself", exact, keys[:1],
			MergeBound{datetime, literaler("2025-05-05T02:00:00+02:00"), literaler("2025-05-05T02:00:00+02:00"), nil}},
		{"a single key widens around its instant, not its wall clock", nil, keys[:1],
			MergeBound{datetime, literaler("2025-05-04"), literaler("2025-05-07"), nil}},
		{"an unparseable value drops the bound", exact, withKey("2025-05-05 00:00:00", "k"), MergeBound{datetime, "", "", nil}},
		{"an unparseable value drops the date bound", nil, withKey("2025-05-05 00:00:00", "k"), MergeBound{datetime, "", "", nil}},
		{"a null key drops the bound", exact, withKey(nil, "k"), MergeBound{datetime, "", "", nil}},
		{"dates before year 1 drop the bound", nil, withKey("0001-01-01T00:00:00Z", "k"), MergeBound{datetime, "", "", nil}},
		{"dates after year 9999 drop the bound", nil, withKey("9999-12-31T00:00:00Z", "k"), MergeBound{datetime, "", "", nil}},
		{"year edges still get exact bounds", exact, withKey("0001-01-01T00:00:00Z", "k"),
			MergeBound{datetime, literaler("0001-01-01T00:00:00Z"), literaler("2025-05-05T00:00:01.5Z"), nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			b := NewMergeBoundsBuilder([]Column{datetime, id}, literaler, tt.opts...)
			for _, k := range tt.keys {
				b.NextKey(k)
			}
			got := b.Build()
			require.Equal(t, tt.want, got[0])
			require.Equal(t, MergeBound{id, literaler("k"), literaler("k"), nil}, got[1], "other keys are unaffected")

			// The next transaction starts clean.
			b.NextKey([]any{"2025-06-01T00:00:00Z", "k"})
			require.Contains(t, []string{literaler("2025-05-31"), literaler("2025-06-01T00:00:00Z")}, b.Build()[0].LiteralLower)
		})
	}
}

// withMaxRanges sets the most ranges a bound splits into.
func withMaxRanges(n int) MergeBoundsOption {
	return func(b *MergeBoundsBuilder) { b.maxRanges = n }
}

func TestMergeBoundsBuilderDatetimeRanges(t *testing.T) {
	literaler := ToLiteralFn(QuoteTransform("'", "''"))
	datetime := Column{Identifier: "ts", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}},
	}}}
	exact := WithExactDatetimeBounds(func(Column) bool { return true })
	declined := WithExactDatetimeBounds(func(Column) bool { return false })

	// A band of recent keys, and two outliers months before it.
	band := func() []string {
		var out []string
		for i := 0; i < 500; i++ {
			out = append(out, time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC).Add(time.Duration(i)*time.Second).Format(time.RFC3339Nano))
		}
		return out
	}()
	withOutliers := append([]string{"2026-03-01T10:00:00+01:00", "2025-11-30T00:00:00Z"}, band...)

	build := func(keys []string, opts ...MergeBoundsOption) MergeBound {
		b := NewMergeBoundsBuilder([]Column{datetime}, literaler, opts...)
		for _, k := range keys {
			b.NextKey([]any{k})
		}
		return b.Build()[0]
	}

	t.Run("outliers get ranges of their own", func(t *testing.T) {
		got := build(withOutliers, exact, WithDatetimeRanges())
		require.Equal(t, literaler("2025-11-30T00:00:00Z"), got.LiteralLower)
		require.Equal(t, literaler(band[len(band)-1]), got.LiteralUpper)
		require.Equal(t, []MergeRange{
			{literaler("2025-11-30T00:00:00Z"), literaler("2025-11-30T00:00:00Z")},
			{literaler("2026-03-01T10:00:00+01:00"), literaler("2026-03-01T10:00:00+01:00")},
			{literaler(band[0]), literaler(band[len(band)-1])},
		}, got.Ranges)
	})

	t.Run("evenly spread keys aren't split", func(t *testing.T) {
		require.Nil(t, build(band, exact, WithDatetimeRanges()).Ranges)
	})

	t.Run("a range limit merges the narrowest gaps", func(t *testing.T) {
		got := build(withOutliers, exact, WithDatetimeRanges(), withMaxRanges(2))
		require.Equal(t, []MergeRange{
			{literaler("2025-11-30T00:00:00Z"), literaler("2026-03-01T10:00:00+01:00")},
			{literaler(band[0]), literaler(band[len(band)-1])},
		}, got.Ranges)
	})

	t.Run("ranges need exact bounds and the option", func(t *testing.T) {
		require.Nil(t, build(withOutliers, exact).Ranges)
		require.Nil(t, build(withOutliers, declined, WithDatetimeRanges()).Ranges)
		require.Nil(t, build(withOutliers, WithDatetimeRanges()).Ranges)
	})

	t.Run("an unparseable value drops the ranges with the bound", func(t *testing.T) {
		require.Equal(t, MergeBound{datetime, "", "", nil}, build(append(withOutliers, "nope"), exact, WithDatetimeRanges()))
	})

	t.Run("the next transaction starts clean", func(t *testing.T) {
		b := NewMergeBoundsBuilder([]Column{datetime}, literaler, exact, WithDatetimeRanges())
		for _, k := range withOutliers {
			b.NextKey([]any{k})
		}
		require.NotNil(t, b.Build()[0].Ranges)
		b.NextKey([]any{band[0]})
		require.Nil(t, b.Build()[0].Ranges)
	})

	t.Run("ranges enclose every key", func(t *testing.T) {
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 200; i++ {
			var keys []string
			var instants []time.Time
			for clusters := rng.Intn(100) + 1; clusters > 0; clusters-- {
				center := time.Unix(rng.Int63n(2e9), 0)
				for n := rng.Intn(20) + 1; n > 0; n-- {
					ts := center.Add(time.Duration(rng.Int63n(int64(time.Hour))))
					keys = append(keys, ts.In(time.FixedZone("", (rng.Intn(48)-24)*1800)).Format(time.RFC3339Nano))
					instants = append(instants, ts)
				}
			}
			rng.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
				instants[i], instants[j] = instants[j], instants[i]
			})

			got := build(keys, exact, WithDatetimeRanges(), withMaxRanges(4))
			require.LessOrEqual(t, len(got.Ranges), 4)
			var parse = func(literal string) time.Time {
				ts, err := time.Parse(time.RFC3339Nano, strings.Trim(literal, "'"))
				require.NoError(t, err)
				return ts
			}
			for j := 1; j < len(got.Ranges); j++ {
				require.True(t, parse(got.Ranges[j-1].LiteralUpper).Before(parse(got.Ranges[j].LiteralLower)))
			}
			for _, ts := range instants {
				require.True(t, len(got.Ranges) == 0 || slices.ContainsFunc(got.Ranges, func(r MergeRange) bool {
					return !ts.Before(parse(r.LiteralLower)) && !ts.After(parse(r.LiteralUpper))
				}), "%s is outside every range", ts)
			}
		}
	})
}

func TestMergeBoundsBuilderStringRanges(t *testing.T) {
	literaler := ToLiteralFn(QuoteTransform("'", "''"))
	str := Column{Identifier: "id", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{Types: []string{"string"}},
	}}}
	datetime := Column{Identifier: "ts", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}},
	}}}
	integer := Column{Identifier: "n", Projection: Projection{Projection: pf.Projection{
		Inference: pf.Inference{Types: []string{"integer"}},
	}}}

	// A band of time-ordered IDs sharing a prefix, and two outliers before it.
	band := func() []string {
		var out []string
		for i := 0; i < 500; i++ {
			out = append(out, fmt.Sprintf("019a2b3c-%04x-7000-8000-000000000000", i))
		}
		return out
	}()
	withOutliers := append([]string{"0185aaaa-0000-7000-8000-000000000000", "0123ffff-0000-7000-8000-000000000000"}, band...)

	build := func(col Column, keys []string, opts ...MergeBoundsOption) MergeBound {
		b := NewMergeBoundsBuilder([]Column{col}, literaler, opts...)
		for _, k := range keys {
			b.NextKey([]any{k})
		}
		return b.Build()[0]
	}

	t.Run("outliers get ranges of their own", func(t *testing.T) {
		got := build(str, withOutliers, WithStringRanges())
		require.Equal(t, literaler(withOutliers[1]), got.LiteralLower)
		require.Equal(t, literaler(band[len(band)-1]), got.LiteralUpper)
		require.Equal(t, []MergeRange{
			{literaler(withOutliers[1]), literaler(withOutliers[1])},
			{literaler(withOutliers[0]), literaler(withOutliers[0])},
			{literaler(band[0]), literaler(band[len(band)-1])},
		}, got.Ranges)
	})

	t.Run("evenly spread keys aren't split", func(t *testing.T) {
		require.Nil(t, build(str, band, WithStringRanges()).Ranges)
	})

	t.Run("uniformly random UUIDs aren't split", func(t *testing.T) {
		rng := rand.New(rand.NewSource(1))
		var keys []string
		for i := 0; i < 10000; i++ {
			var u [16]byte
			rng.Read(u[:])
			keys = append(keys, fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16]))
		}
		require.Nil(t, build(str, keys, WithStringRanges()).Ranges)
	})

	t.Run("keys closer together than their distance resolves aren't split", func(t *testing.T) {
		var keys []string
		for _, suffix := range []string{"0", "1", "2"} {
			keys = append(keys, "a"+strings.Repeat("\xff", 62)+suffix, "b"+strings.Repeat("\x00", 62)+suffix)
		}
		require.Nil(t, build(str, keys, WithStringRanges()).Ranges)
	})

	t.Run("ranges need the option and a plain string column", func(t *testing.T) {
		require.Nil(t, build(str, withOutliers).Ranges)
		require.Nil(t, build(str, withOutliers, WithDatetimeRanges()).Ranges)

		dates := []string{"2025-11-30T00:00:00Z", "2026-10-26T12:00:00Z", "2026-10-26T12:00:01Z"}
		require.Nil(t, build(datetime, dates, WithStringRanges()).Ranges)

		b := NewMergeBoundsBuilder([]Column{integer}, literaler, WithStringRanges())
		for _, k := range []int64{1, 2, 1000000} {
			b.NextKey([]any{k})
		}
		require.Nil(t, b.Build()[0].Ranges)
	})

	t.Run("ranges enclose every key", func(t *testing.T) {
		rng := rand.New(rand.NewSource(1))
		const alphabet = "\x00\x01-09AZaz\x7f\xc3\xa9\xff"
		// Prefixes run past the positions the alphabet tracks.
		var randomKeys = func(maxPrefix int) []string {
			var keys []string
			for clusters := rng.Intn(100) + 1; clusters > 0; clusters-- {
				var prefix = make([]byte, rng.Intn(maxPrefix))
				for j := range prefix {
					prefix[j] = alphabet[rng.Intn(len(alphabet))]
				}
				for n := rng.Intn(20) + 1; n > 0; n-- {
					var suffix = make([]byte, rng.Intn(4))
					for j := range suffix {
						suffix[j] = alphabet[rng.Intn(len(alphabet))]
					}
					keys = append(keys, string(prefix)+string(suffix))
				}
			}
			return keys
		}
		for i := 0; i < 200; i++ {
			// A two-column key, whose columns' keys are drawn independently.
			var cols = [2][]string{randomKeys(6), randomKeys(80)}
			var n = min(len(cols[0]), len(cols[1]))

			b := NewMergeBoundsBuilder([]Column{str, str}, func(v any) string { return v.(string) }, WithStringRanges(), withMaxRanges(4))
			for j := range n {
				b.NextKey([]any{cols[0][j], cols[1][j]})
			}
			for c, got := range b.Build() {
				require.LessOrEqual(t, len(got.Ranges), 4)
				for j := 1; j < len(got.Ranges); j++ {
					require.Less(t, got.Ranges[j-1].LiteralUpper, got.Ranges[j].LiteralLower)
				}
				for _, k := range cols[c][:n] {
					require.True(t, k >= got.LiteralLower && k <= got.LiteralUpper, "%q is outside the bound", k)
					require.True(t, len(got.Ranges) == 0 || slices.ContainsFunc(got.Ranges, func(r MergeRange) bool {
						return r.LiteralLower <= k && k <= r.LiteralUpper
					}), "%q is outside every range", k)
				}
			}
		}
	})
}

func TestDatetimePositionOrdersInstants(t *testing.T) {
	var instants = []time.Time{
		time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(1969, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Unix(0, 0),
		time.Unix(0, 1),
		time.Unix(1, 0),
		time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC),
	}
	for i := 1; i < len(instants); i++ {
		require.Less(t, datetimePosition(instants[i-1]), datetimePosition(instants[i]))
	}

	// A gap is proportional to the time it spans, across second boundaries too.
	var at = time.Date(2026, 10, 26, 12, 0, 0, 999999000, time.UTC)
	var gap = func(d time.Duration) float64 {
		var gap, _ = instantScale{}.distance(datetimePosition(at), datetimePosition(at.Add(d)))
		return gap
	}
	require.InDelta(t, 1, gap(2*time.Microsecond)-gap(time.Microsecond), 1e-9)
	require.InDelta(t, math.Log2(3600), gap(time.Hour)-gap(time.Second), 1e-9)
}

// TestDateBoundsEncloseEverySpelling checks the property the date widening
// relies on: any RFC3339 spelling of an instant within a transaction's range
// sorts, as text, between the widened bounds.
func TestDateBoundsEncloseEverySpelling(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	spell := func(ts time.Time) string {
		offset := (rng.Intn(2*24*60) - 24*60 + 1) * 60 // -23:59 .. +23:59 in seconds
		if rng.Intn(4) == 0 {
			offset = 0
		}
		ts = ts.In(time.FixedZone("", offset))
		sep := []string{"T", "t", " "}[rng.Intn(3)]
		s := ts.Format("2006-01-02") + sep + ts.Format("15:04:05")
		if digits := rng.Intn(10); digits > 0 {
			s += "." + fmt.Sprintf("%09d", ts.Nanosecond())[:digits]
		}
		if offset == 0 && rng.Intn(2) == 0 {
			s += []string{"Z", "z"}[rng.Intn(2)]
		} else {
			s += ts.Format("-07:00")
		}
		return s
	}

	for i := 0; i < 20000; i++ {
		base := time.Unix(rng.Int63n(4e9)-1e9, rng.Int63n(1e9)).UTC()
		span := time.Duration(rng.Int63n(int64(72 * time.Hour)))
		lo, hi, ok := dateBounds(base, base.Add(span))
		require.True(t, ok)

		for j := 0; j < 5; j++ {
			inRange := base.Add(time.Duration(rng.Int63n(int64(span) + 1)))
			s := spell(inRange)
			require.GreaterOrEqual(t, s, lo, "%s spelled as %s", inRange, s)
			require.LessOrEqual(t, s, hi, "%s spelled as %s", inRange, s)
		}
	}
}
