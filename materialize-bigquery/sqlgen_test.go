package connector

import (
	"database/sql"
	"strings"
	"testing"
	"fmt"
	"encoding/json"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/connectors/testsupport"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
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

	var shape = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Table:     "target_table",
		Delta:     false,
		projectID: "projectID",
		dataset:   "dataset",
	})

	table, err := sqlDriver.ResolveTable(shape, bqDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplTempTableName,
		tplCreateTargetTable,
		tplLoadQuery,
		tplStoreInsert,
		tplStoreUpdate,
	} {
		var testcase = table.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---\n")
		require.NoError(t, tpl.Execute(&snap, &table))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Table:     "target_table_no_values_materialized",
		Delta:     false,
		projectID: "projectID",
		dataset:   "dataset",
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, bqDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized storeUpdate" + " ---\n")
	require.NoError(t, tplStoreUpdate.Execute(&snap, &tableNoValues))
	snap.WriteString("--- End " + "target_table_no_values_materialized storeUpdate" + " ---\n\n")

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"project", "dataset", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}
	snap.WriteString("--- Begin Fence Install ---\n")
	require.NoError(t, tplInstallFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Install ---\n")

	snap.WriteString("--- Begin Fence Update ---\n")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}
