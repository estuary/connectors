package main

import (
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = mysqlDialect(time.FixedZone("UTC", 0), "db")

func TestSQLGeneration(t *testing.T) {
	var templates = renderTemplates(testDialect)

	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string, delta bool) sql.Resource {
			return tableConfig{
				Table: table,
				Delta: delta,
			}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				templates.createTargetTable,
				templates.createLoadTable,
				templates.createUpdateTable,
				templates.tempTruncate,
				templates.loadLoad,
				templates.loadQuery,
				templates.insertLoad,
				templates.updateLoad,
				templates.updateReplace,
				templates.updateTruncate,
				templates.deleteQuery,
				templates.deleteLoad,
				templates.deleteTruncate,
			},
			TplAddColumns:    templates.alterTableColumns,
			TplDropNotNulls:  templates.alterTableColumns,
			TplCombinedAlter: templates.alterTableColumns,
			TplInstallFence:  templates.installFence,
			TplUpdateFence:   templates.updateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var mapped, err = testDialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "DATETIME(6) NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, "2022-04-04T10:09:08.234567", parsed)
	require.NoError(t, err)
}

func TestDateTimePKColumn(t *testing.T) {
	var mapped, err = testDialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "DATETIME(6) NOT NULL", mapped.DDL)
}

func TestTimeColumn(t *testing.T) {
	var mapped, err = testDialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "TIME(6) NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("10:09:08.234567Z")
	require.Equal(t, "10:09:08.234567", parsed)
	require.NoError(t, err)
}
