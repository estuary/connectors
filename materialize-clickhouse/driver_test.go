package main

import (
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{Table: table, Delta: delta}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newClickHouseDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newClickHouseDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	// TODO: enable once MigratableTypes is defined for the ClickHouse dialect.
	// t.Run("migrate", func(t *testing.T) {
	// 	sql.RunMigrationTest(t, newClickHouseDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	// })
}

// noFlowDocShape builds a TableShape for NoFlowDocument tests with a variety
// of column types: a string key, nullable and non-nullable object/array/multiple
// values, a nullable string, a date-time, and a string_number.
func noFlowDocShape(tableName string) sql.TableShape {
	return sql.TableShape{
		Path:    sql.TablePath{tableName},
		Binding: 0,
		Keys: []sql.Projection{
			{
				Projection: pf.Projection{
					Ptr:   "/id",
					Field: "id",
					Inference: pf.Inference{
						Types:   []string{"string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{},
					},
				},
			},
		},
		Values: []sql.Projection{
			{
				Projection: pf.Projection{
					Ptr:   "/_meta",
					Field: "_meta",
					Inference: pf.Inference{
						Types:  []string{"object"},
						Exists: pf.Inference_MUST,
					},
				},
			},
			{
				Projection: pf.Projection{
					Ptr:   "/tags",
					Field: "tags",
					Inference: pf.Inference{
						Types:  []string{"array", "null"},
						Exists: pf.Inference_MAY,
					},
				},
			},
			{
				Projection: pf.Projection{
					Ptr:   "/extra",
					Field: "extra",
					Inference: pf.Inference{
						Types:  []string{"integer", "object", "boolean", "null"},
						Exists: pf.Inference_MAY,
					},
				},
			},
			{
				Projection: pf.Projection{
					Ptr:   "/note",
					Field: "note",
					Inference: pf.Inference{
						Types:   []string{"string", "null"},
						Exists:  pf.Inference_MAY,
						String_: &pf.Inference_String{},
					},
				},
			},
			{
				Projection: pf.Projection{
					Ptr:   "/score",
					Field: "score",
					Inference: pf.Inference{
						Types:   []string{"number", "string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{Format: "number"},
					},
				},
			},
			{
				Projection: pf.Projection{
					Ptr:   "/flow_published_at",
					Field: "flow_published_at",
					Inference: pf.Inference{
						Types:   []string{"string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{Format: "date-time"},
					},
				},
			},
		},
	}
}

// TestNoFlowDocumentObjectColumns verifies that root-level OBJECT, ARRAY, and
// MULTIPLE columns are correctly embedded as JSON objects/arrays (not
// double-encoded as strings) when reconstructed by the
// queryLoadTableNoFlowDocument template.
func TestNoFlowDocumentObjectColumns(t *testing.T) {
	var cfg = testConfig()
	cfg.Advanced.NoFlowDocument = true
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_no_flow_doc_object"

	var shape = noFlowDocShape(tableName)
	table, err := sql.ResolveTable(shape, dialect)
	require.NoError(t, err)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	storeRows(t, ctx, storeConn, b, cfg.Database, []any{
		"k1",                         // id
		`{"op":"c","extra":"stuff"}`, // _meta (object)
		`["tag1","tag2"]`,            // tags (nullable array)
		`{"nested":true}`,            // extra (nullable multiple)
		"hello",                      // note (nullable string)
		float64(3.14),                // score (string_number → Float64)
		testTime,                     // flow_published_at (date-time)
	})

	docs := loadDocuments(t, ctx, loadConn, b, []any{"k1"})
	require.Len(t, docs, 1)

	var parsed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(docs[0]), &parsed))

	// OBJECT: embedded as JSON object.
	require.JSONEq(t, `{"op":"c","extra":"stuff"}`, string(parsed["_meta"]))

	// ARRAY: embedded as JSON array.
	require.JSONEq(t, `["tag1","tag2"]`, string(parsed["tags"]))

	// MULTIPLE: embedded as its actual JSON type.
	require.JSONEq(t, `{"nested":true}`, string(parsed["extra"]))

	// STRING: properly quoted.
	require.Equal(t, `"hello"`, string(parsed["note"]))

	// STRING_NUMBER: serialized as a JSON string (quoted), not a bare number.
	require.Equal(t, `"3.14"`, string(parsed["score"]))

	// DATE-TIME: RFC3339 string.
	require.Equal(t, `"2024-01-01T00:00:00.000000Z"`, string(parsed["flow_published_at"]))
}

// TestNoFlowDocumentNullValues verifies that nullable columns serialize as JSON
// null (not the string "null" or a missing key) when their value is NULL.
func TestNoFlowDocumentNullValues(t *testing.T) {
	var cfg = testConfig()
	cfg.Advanced.NoFlowDocument = true
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_no_flow_doc_nulls"

	var shape = noFlowDocShape(tableName)
	table, err := sql.ResolveTable(shape, dialect)
	require.NoError(t, err)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store a row where all nullable columns are NULL.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{
		"k1",         // id
		`{"op":"c"}`, // _meta (non-nullable object)
		nil,          // tags (nullable array → NULL)
		nil,          // extra (nullable multiple → NULL)
		nil,          // note (nullable string → NULL)
		float64(1.0), // score (non-nullable string_number)
		testTime,     // flow_published_at
	})

	docs := loadDocuments(t, ctx, loadConn, b, []any{"k1"})
	require.Len(t, docs, 1)

	var parsed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(docs[0]), &parsed))

	// Nullable columns with NULL values must serialize as JSON null.
	require.Equal(t, "null", string(parsed["tags"]))
	require.Equal(t, "null", string(parsed["extra"]))
	require.Equal(t, "null", string(parsed["note"]))

	// Non-nullable columns are still present.
	require.JSONEq(t, `{"op":"c"}`, string(parsed["_meta"]))
	require.Equal(t, `"1"`, string(parsed["score"]))
}
