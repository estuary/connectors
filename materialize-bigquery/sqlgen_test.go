package connector

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	snap, tables := sql.RunSqlGenTests(
		t,
		bqDialect,
		func(table string, delta bool) sql.Resource {
			return tableConfig{
				Table:     table,
				Delta:     delta,
				projectID: "projectID",
				Dataset:   "dataset",
			}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				tplCreateTargetTable,
				tplLoadQuery,
				tplStoreInsert,
			},
			TplAddColumns:    tplAlterTableColumns,
			TplDropNotNulls:  tplAlterTableColumns,
			TplCombinedAlter: tplAlterTableColumns,
			TplInstallFence:  tplInstallFence,
			TplUpdateFence:   tplUpdateFence,
		},
	)

	{
		tpl := tplStoreUpdate
		tbl := tables[0]
		require.False(t, tbl.DeltaUpdates)
		var testcase = tbl.Identifier + " " + tpl.Name()

		bounds := []sql.MergeBound{
			{
				Column:       tbl.Keys[0],
				LiteralLower: bqDialect.Literal(int64(10)),
				LiteralUpper: bqDialect.Literal(int64(100)),
			},
			{
				Column: tbl.Keys[1],
				// No bounds - as would be the case for a boolean key, which
				// would be a very weird key, but technically allowed.
			},
			{
				Column:       tbl.Keys[2],
				LiteralLower: bqDialect.Literal("aGVsbG8K"),
				LiteralUpper: bqDialect.Literal("Z29vZGJ5ZQo="),
			},
		}

		tf := mergeQueryInput{
			Table:  tbl,
			Bounds: bounds,
		}

		snap.WriteString("--- Begin " + testcase + " ---\n")
		require.NoError(t, tpl.Execute(snap, &tf))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
