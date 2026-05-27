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
		existingType string
		udtName      string
		wantOk       bool
	}{
		{
			name:    "normal match",
			field:   "status",
			udtName: "orders_status_flow_enum",
			wantOk:  true,
		},
		{
			name:    "normal match case-insensitive",
			field:   "Status",
			udtName: "orders_STATUS_flow_enum",
			wantOk:  true,
		},
		{
			name:    "normal no match wrong field",
			field:   "state",
			udtName: "orders_status_flow_enum",
			wantOk:  false,
		},
		{
			name:         "not user-defined type",
			field:        "status",
			existingType: "text",
			udtName:      "orders_status_flow_enum",
			wantOk:       false,
		},
		{
			// Hashed case: field exactly matches the truncated segment
			name:    "hashed match exact field truncation",
			field:   "some_very_long_field_name_for_testing",
			udtName: "table_some_abcd1234_flow_enum",
			wantOk:  true,
		},
		{
			// Hashed case: field prefix is 1 byte
			name:    "hashed match single byte field prefix",
			field:   "status",
			udtName: "table_s_abcd1234_flow_enum",
			wantOk:  true,
		},
		{
			// Hashed suffix with wrong field prefix should not match
			name:    "hashed no match wrong field prefix",
			field:   "color",
			udtName: "table_s_abcd1234_flow_enum",
			wantOk:  false,
		},
		{
			// 8-char suffix is not hex — should not match hashed pattern
			name:    "hashed suffix not hex",
			field:   "status",
			udtName: "table_status_ZZZZZZZZ_flow_enum",
			wantOk:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &PgEnum{Field: tt.field}
			existingType := tt.existingType
			if existingType == "" {
				existingType = "USER-DEFINED"
			}
			existing := boilerplate.ExistingField{
				Type: existingType,
				Meta: pgFieldMeta{UDTName: tt.udtName},
			}
			require.Equal(t, tt.wantOk, e.Compatible(existing))
		})
	}
}


func TestEnumTypeNamePartsNormal(t *testing.T) {
	dialect := createPgDialect(featureFlagDefaults)

	// Short names: no hash, base <= 63 bytes
	schema, base, typeName, err := enumTypeNameParts(dialect, []string{"public", "orders"}, "status")
	require.NoError(t, err)
	require.Equal(t, "public", schema)
	require.Equal(t, "orders_status_flow_enum", base)
	require.LessOrEqual(t, len([]byte(base)), 63)
	require.Contains(t, typeName, "orders_status_flow_enum")
}

func TestEnumTypeNamePartsHashed(t *testing.T) {
	dialect := createPgDialect(featureFlagDefaults)

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
	dialect := createPgDialect(featureFlagDefaults)

	// tableName at max length (63 bytes) — previously caused base to exceed 63 bytes
	longTable := strings.Repeat("a", 63)
	longField := strings.Repeat("b", 50)
	_, base, _, err := enumTypeNameParts(dialect, []string{"public", longTable}, longField)
	require.NoError(t, err)
	require.LessOrEqual(t, len([]byte(base)), 63, "base must fit within Postgres 63-byte identifier limit")
	require.True(t, strings.HasSuffix(base, "_flow_enum"))
}

func TestEnumTypeNamePartsSinglePathElement(t *testing.T) {
	dialect := createPgDialect(featureFlagDefaults)

	schema, base, _, err := enumTypeNameParts(dialect, []string{"mytable"}, "col")
	require.NoError(t, err)
	require.Equal(t, "public", schema)
	require.Equal(t, "mytable_col_flow_enum", base)
}
