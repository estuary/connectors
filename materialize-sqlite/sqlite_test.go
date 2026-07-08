package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"text/template"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestStoreInsertThenUpdate(t *testing.T) {
	var ctx = context.Background()
	table, db := setupTestTable(t)

	insertSQL := mustRender(t, table, tplStoreInsert)
	updateSQL := mustRender(t, table, tplStoreUpdate)

	// Insert two rows so the update must discriminate between keys.
	insertRow(t, ctx, db, table, insertSQL, 1, "orig", `{"id":1}`)
	insertRow(t, ctx, db, table, insertSQL, 2, "orig2", `{"id":2}`)

	// Both rows land as inserted.
	byID := readTableByID(t, ctx, db, table.Identifier)
	require.Len(t, byID, 2)
	require.Equal(t, "orig", byID[1]["canary"])
	require.Equal(t, "orig2", byID[2]["canary"])

	// Update id=1 with parameters in natural ConvertAll order — exactly what
	// transactor.Store passes for an existing key. The update's WHERE key is
	// textually last but must bind the leading key parameter; a regression
	// there matches zero rows and silently drops the update.
	updParams, err := table.ConvertAll(tuple.Tuple{int64(1)}, tuple.Tuple{"changed"}, json.RawMessage(`{"id":1,"v":2}`))
	require.NoError(t, err)
	res, err := db.ExecContext(ctx, updateSQL, updParams...)
	require.NoError(t, err)
	affected, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "update must match exactly the keyed row")

	// id=1 reflects the update; id=2 is untouched.
	byID = readTableByID(t, ctx, db, table.Identifier)
	require.Len(t, byID, 2)
	require.Equal(t, "changed", byID[1]["canary"])
	require.JSONEq(t, `{"id":1,"v":2}`, documentJSON(t, byID[1]["flow_document"]))
	require.Equal(t, "orig2", byID[2]["canary"])
	require.JSONEq(t, `{"id":2}`, documentJSON(t, byID[2]["flow_document"]))
}

func TestLoad(t *testing.T) {
	var ctx = context.Background()
	table, db := setupTestTable(t)

	insertSQL := mustRender(t, table, tplStoreInsert)
	insertRow(t, ctx, db, table, insertSQL, 1, "one", `{"id":1,"v":1}`)
	insertRow(t, ctx, db, table, insertSQL, 2, "two", `{"id":2,"v":2}`)

	// Mirror transactor.Load: stage keys into the per-binding temp table, then
	// join it against the target table to fetch the stored documents.
	_, err := db.ExecContext(ctx, mustRender(t, table, tplCreateLoadTable))
	require.NoError(t, err)
	var (
		loadInsertSQL   = mustRender(t, table, tplLoadInsert)
		loadQuerySQL    = mustRender(t, table, tplLoadQuery)
		loadTruncateSQL = mustRender(t, table, tplLoadTruncate)
	)

	stageKeys := func(ids ...int64) {
		for _, id := range ids {
			conv, err := table.ConvertKey(tuple.Tuple{id})
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, loadInsertSQL, conv...)
			require.NoError(t, err)
		}
	}
	loadDocs := func() map[int64]string {
		rows, err := db.QueryContext(ctx, loadQuerySQL)
		require.NoError(t, err)
		defer rows.Close()

		out := map[int64]string{}
		for rows.Next() {
			var binding int
			var doc string
			require.NoError(t, rows.Scan(&binding, &doc))
			require.Equal(t, 0, binding, "loadQuery emits the binding index")

			var probe struct {
				ID int64 `json:"id"`
			}
			require.NoError(t, json.Unmarshal([]byte(doc), &probe))
			out[probe.ID] = doc
		}
		require.NoError(t, rows.Err())
		return out
	}

	// Staging a present and an absent key returns only the present document,
	// exactly as it was stored.
	stageKeys(1, 99)
	got := loadDocs()
	require.Len(t, got, 1)
	require.JSONEq(t, `{"id":1,"v":1}`, got[1])

	// loadTruncate clears staged keys between transactions: a fresh stage of a
	// different key must not resurface the previously-staged one.
	_, err = db.ExecContext(ctx, loadTruncateSQL)
	require.NoError(t, err)
	stageKeys(2)
	got = loadDocs()
	require.Len(t, got, 1)
	require.JSONEq(t, `{"id":2,"v":2}`, got[2])
}

func testTableShape(name string) sql.TableShape {
	return sql.TableShape{
		Path:    sql.TablePath{name},
		Binding: 0,
		Keys: []sql.Projection{{Projection: pf.Projection{
			Field:     "id",
			Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
		}}},
		Values: []sql.Projection{{Projection: pf.Projection{
			Field:     "canary",
			Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST, String_: &pf.Inference_String{}},
		}}},
		Document: &sql.Projection{Projection: pf.Projection{
			Field:     "flow_document",
			Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MUST},
		}},
	}
}

// setupTestTable resolves the test table shape through the real sqliteDialect,
// opens an isolated on-disk database, and creates the target table.
func setupTestTable(t *testing.T) (sql.Table, *stdsql.DB) {
	t.Helper()

	table, err := sql.ResolveTable(testTableShape("test_results"), sqliteDialect)
	require.NoError(t, err)

	db, err := stdsql.Open("sqlite3", filepath.Join(t.TempDir(), "test.db"))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	_, err = db.ExecContext(context.Background(), mustRender(t, table, tplCreateTargetTable))
	require.NoError(t, err)

	return table, db
}

func mustRender(t *testing.T, table sql.Table, tpl *template.Template) string {
	t.Helper()
	out, err := sql.RenderTableTemplate(table, tpl)
	require.NoError(t, err)
	return out
}

func insertRow(t *testing.T, ctx context.Context, db *stdsql.DB, table sql.Table, insertSQL string, id int64, canary, doc string) {
	t.Helper()
	params, err := table.ConvertAll(tuple.Tuple{id}, tuple.Tuple{canary}, json.RawMessage(doc))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, insertSQL, params...)
	require.NoError(t, err)
}

// readTableByID reads the table back through the shared snapshot helper (which
// normalizes driver value types) and indexes rows by the integer id column.
func readTableByID(t *testing.T, ctx context.Context, db *stdsql.DB, identifier string) map[int64]map[string]any {
	t.Helper()

	cols, rows, err := sql.SnapshotTestTable(ctx, db, identifier)
	require.NoError(t, err)

	out := make(map[int64]map[string]any, len(rows))
	for _, row := range rows {
		rec := make(map[string]any, len(cols))
		for i, col := range cols {
			rec[col] = row[i]
		}
		id, ok := rec["id"].(int64)
		require.Truef(t, ok, "id column should be int64, got %T", rec["id"])
		out[id] = rec
	}
	return out
}

func documentJSON(t *testing.T, v any) string {
	t.Helper()

	switch d := v.(type) {
	case json.RawMessage:
		return string(d)
	case []byte:
		return string(d)
	case string:
		return d
	default:
		t.Fatalf("unexpected document column type %T", v)
		return ""
	}
}
