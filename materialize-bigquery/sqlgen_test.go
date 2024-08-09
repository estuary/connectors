package connector

import (
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
)

func TestSQLGeneration(t *testing.T) {
	snap, _ := sql.RunSqlGenTests(
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
				tplStoreUpdate,
			},
			TplAddColumns:    tplAlterTableColumns,
			TplDropNotNulls:  tplAlterTableColumns,
			TplCombinedAlter: tplAlterTableColumns,
			TplInstallFence:  tplInstallFence,
			TplUpdateFence:   tplUpdateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}
