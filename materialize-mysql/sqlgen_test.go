package main

import (
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = mysqlDialect(time.UTC, "flow", "mysql", featureFlagDefaults)
var testTemplates = renderTemplates(testDialect, "mysql")

func TestSQLGeneration(t *testing.T) {

	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
				testTemplates.createLoadTable,
				testTemplates.createUpdateTable,
				testTemplates.tempTruncate,
				testTemplates.loadLoad,
				testTemplates.loadQuery,
				testTemplates.loadQueryNoFlowDocument,
				testTemplates.insertLoad,
				testTemplates.updateLoad,
				testTemplates.updateReplace,
				testTemplates.updateTruncate,
				testTemplates.deleteQuery,
				testTemplates.deleteLoad,
				testTemplates.deleteTruncate,
			},
			TplAddColumns:    testTemplates.alterTableColumns,
			TplDropNotNulls:  testTemplates.alterTableColumns,
			TplCombinedAlter: testTemplates.alterTableColumns,
			TplInstallFence:  testTemplates.installFence,
			TplUpdateFence:   testTemplates.updateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var mapped = testDialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "DATETIME(6) NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, "2022-04-04 10:09:08.234567", parsed)
	require.NoError(t, err)
}

func TestDateTimePKColumn(t *testing.T) {
	var mapped = testDialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "VARCHAR(256) NOT NULL", mapped.DDL)
}

func TestTimeColumn(t *testing.T) {
	var mapped = testDialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "TIME(6) NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("10:09:08.234567Z")
	require.Equal(t, "10:09:08.234567", parsed)
	require.NoError(t, err)
}
