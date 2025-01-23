package main

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
		dialect,
		func(table string, delta bool) sql.Resource {
			return tableConfig{
				Table:     table,
				Schema:    "a-schema",
				Delta:     delta,
				warehouse: "a-warehouse",
			}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				tplCreateTargetTable,
			},
			TplAddColumns:  tplAlterTableColumns,
			TplUpdateFence: tplUpdateFence,
		},
	)

	for _, tbl := range tables {
		for _, tpl := range []*template.Template{
			tplStoreCopyIntoQuery,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(snap, &queryParams{
				Table:             tbl,
				URIs:              []string{"https://some/file1", "https://some/file2"},
				StorageAccountKey: "some-storage-account-key",
			}))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	for _, tpl := range []*template.Template{
		tplCreateLoadTable,
		tplLoadQuery,
		tplDropLoadTable,
		tplStoreMergeQuery,
	} {
		tbl := tables[0] // these queries are never run for delta updates mode
		var testcase = tbl.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(snap, &queryParams{
			Table:             tbl,
			URIs:              []string{"https://some/file1", "https://some/file2"},
			StorageAccountKey: "some-storage-account-key",
		}))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
