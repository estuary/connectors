package main

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql-v2"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	snap, tables := sql.RunSqlGenTests(
		t,
		duckDialect,
		func(table string) []string {
			return []string{"db", "a-schema", table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				tplCreateTargetTable,
			},
			TplUpdateFence: tplUpdateFence,
		},
	)

	for _, tpl := range []*template.Template{
		tplLoadQuery,
		tplStoreDeleteQuery,
		tplStoreQuery,
	} {
		// Standard updates cases, which use merge bounds for load and merge
		// queries.
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		bounds := []sql.MergeBound{
			{
				Column:       tbl.Keys[0],
				LiteralLower: duckDialect.Literal(int64(10)),
				LiteralUpper: duckDialect.Literal(int64(100)),
			},
			{
				Column: tbl.Keys[1],
				// No bounds - as would be the case for a boolean key, which
				// would be a very weird key, but technically allowed.
			},
			{
				Column:       tbl.Keys[2],
				LiteralLower: duckDialect.Literal("aGVsbG8K"),
				LiteralUpper: duckDialect.Literal("Z29vZGJ5ZQo="),
			},
		}

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &queryParams{
			Table:  tbl,
			Bounds: bounds,
			Files:  []string{"s3://bucket/file1", "s3://bucket/file2"},
		}))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	{
		// Delta updates only run stores, never loads or merges.
		tpl := tplStoreQuery
		tbl := tables[1]
		require.True(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &queryParams{
			Table: tbl,
			Files: []string{"s3://bucket/file1", "s3://bucket/file2"},
		}))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
