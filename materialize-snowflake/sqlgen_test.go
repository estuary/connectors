package main

import (
	"database/sql"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/connectors/testsupport"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	require.NoError(t, testsupport.CatalogExtract(t, "testdata/flow.yaml",
		func(db *sql.DB) error {
			var err error
			spec, err = catalog.LoadMaterialization(db, "test/sqlite")
			return err
		}))
	var jsonSpec, err = json.Marshal(spec)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s", string(jsonSpec))	

	var shape1 = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Table: "target_table",
		Delta: false,
	})
	var shape2 = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Table: "Delta Updates",
		Delta: true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, snowflakeDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, snowflakeDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		withUUID := TableWithUUID{
			Table:      &tbl,
			RandomUUID: uuid.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		}

		for _, tpl := range []*template.Template{
			tplCreateTargetTable,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}

		for _, tpl := range []*template.Template{
			tplLoadQuery,
			tplCopyInto,
			tplMergeInto,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &withUUID))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Table: "target_table_no_values_materialized",
		Delta: false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, snowflakeDialect)
	require.NoError(t, err)

	tableNoValuesWithUUID := TableWithUUID{
		Table:      &tableNoValues,
		RandomUUID: uuid.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	}

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	require.NoError(t, tplMergeInto.Execute(&snap, &tableNoValuesWithUUID))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}
	snap.WriteString("--- Begin Fence Update ---")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}
