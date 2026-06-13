package main

import (
	"strings"
	"testing"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/stretchr/testify/require"
)

func TestCompatibleFallback(t *testing.T) {
	tests := []struct {
		name         string
		field        string
		path         []string
		existingType string
		udtName      string
		noMeta       bool
		wantOk       bool
	}{
		{
			name:    "normal match",
			field:   "status",
			path:    []string{"public", "orders"},
			udtName: "orders_status_flow_enum",
			wantOk:  true,
		},
		{
			// udt_name as reported by Postgres may differ in case; the
			// comparison is case-insensitive.
			name:    "normal match case-insensitive",
			field:   "status",
			path:    []string{"public", "orders"},
			udtName: "orders_STATUS_flow_enum",
			wantOk:  true,
		},
		{
			name:    "normal no match wrong field",
			field:   "state",
			path:    []string{"public", "orders"},
			udtName: "orders_status_flow_enum",
			wantOk:  false,
		},
		{
			name:         "not user-defined type",
			field:        "status",
			path:         []string{"public", "orders"},
			existingType: "text",
			udtName:      "orders_status_flow_enum",
			wantOk:       false,
		},
		{
			// A pre-existing user-defined type not managed by the connector.
			name:    "foreign user-defined type",
			field:   "status",
			path:    []string{"public", "orders"},
			udtName: "my_custom_enum",
			wantOk:  false,
		},
		{
			// No metadata attached (e.g. udt_name was never populated) cannot
			// be confirmed compatible.
			name:   "missing meta",
			field:  "status",
			path:   []string{"public", "orders"},
			noMeta: true,
			wantOk: false,
		},
		{
			// Regression: a field starting with "_" on a long table truncates
			// its retained field portion down to a single underscore, producing
			// a "..._<hash>_flow_enum" name the old reverse-parsing regex could
			// not recognize — causing an endless backfill. The recompute path
			// recognizes it.
			name:    "underscore field on long table (meta/op)",
			field:   "_meta/op",
			path:    []string{"somelongtenant", "somelongtenant_baaaaaaaaaaaaaaaaaaaaaaaaaaaadocuments"},
			udtName: "somelongtenant_baaaaaaaaaaaaaaaaaaaaaaaaaa___33e4d166_flow_enum",
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &PgEnum{Field: tt.field}
			existingType := tt.existingType
			if existingType == "" {
				existingType = "USER-DEFINED"
			}
			existing := boilerplate.ExistingField{Type: existingType}
			if !tt.noMeta {
				existing.Meta = pgFieldMeta{UDTName: tt.udtName, Path: tt.path}
			}
			require.Equal(t, tt.wantOk, e.Compatible(existing))
		})
	}
}

// TestCompatibleRoundTrip asserts that every name the generator produces is
// recognized as compatible by Compatible, across the truncation/hash regimes.
func TestCompatibleRoundTrip(t *testing.T) {
	dialect := createPgDialect(newTaskFlagDefaults())

	cases := []struct {
		name  string
		path  []string
		field string
	}{
		{"short", []string{"public", "orders"}, "status"},
		{"long field hashes", []string{"public", "orders"}, strings.Repeat("x", 60)},
		{"long table hashes", []string{"public", strings.Repeat("a", 60)}, "status"},
		{"underscore field long table", []string{"somelongtenant", "somelongtenant_baaaaaaaaaaaaaaaaaaaaaaaaaaaadocuments"}, "_meta/op"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, base, _, err := enumTypeNameParts(dialect, tc.path, tc.field)
			require.NoError(t, err)

			e := &PgEnum{Field: tc.field}
			existing := boilerplate.ExistingField{
				Type: "USER-DEFINED",
				Meta: pgFieldMeta{UDTName: base, Path: tc.path},
			}
			require.True(t, e.Compatible(existing), "generated base %q must be compatible", base)
		})
	}
}

func TestEnumTypeNamePartsNormal(t *testing.T) {
	dialect := createPgDialect(newTaskFlagDefaults())

	// Short names: no hash, base <= 63 bytes
	schema, base, typeName, err := enumTypeNameParts(dialect, []string{"public", "orders"}, "status")
	require.NoError(t, err)
	require.Equal(t, "public", schema)
	require.Equal(t, "orders_status_flow_enum", base)
	require.LessOrEqual(t, len([]byte(base)), 63)
	require.Contains(t, typeName, "orders_status_flow_enum")
}

func TestEnumTypeNamePartsHashed(t *testing.T) {
	dialect := createPgDialect(newTaskFlagDefaults())

	// Long field name triggers hash mode
	longField := strings.Repeat("x", 50)
	schema, base, typeName, err := enumTypeNameParts(dialect, []string{"public", "orders"}, longField)
	require.NoError(t, err)
	require.Equal(t, "public", schema)
	require.LessOrEqual(t, len([]byte(base)), 63, "base must fit within Postgres 63-byte identifier limit")
	require.True(t, strings.HasSuffix(base, "_flow_enum"), "base must end with _flow_enum")
	require.NotEmpty(t, typeName)
}

func TestEnumTypeNamePartsLongTableName(t *testing.T) {
	dialect := createPgDialect(newTaskFlagDefaults())

	// tableName at max length (63 bytes) — previously caused base to exceed 63 bytes
	longTable := strings.Repeat("a", 63)
	longField := strings.Repeat("b", 50)
	_, base, _, err := enumTypeNameParts(dialect, []string{"public", longTable}, longField)
	require.NoError(t, err)
	require.LessOrEqual(t, len([]byte(base)), 63, "base must fit within Postgres 63-byte identifier limit")
	require.True(t, strings.HasSuffix(base, "_flow_enum"))
}

func TestEnumTypeNamePartsSinglePathElement(t *testing.T) {
	dialect := createPgDialect(newTaskFlagDefaults())

	schema, base, _, err := enumTypeNameParts(dialect, []string{"mytable"}, "col")
	require.NoError(t, err)
	require.Equal(t, "public", schema)
	require.Equal(t, "mytable_col_flow_enum", base)
}
