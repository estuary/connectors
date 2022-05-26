package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/testsupport"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestQueryGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	require.NoError(t, testsupport.CatalogExtract(t, "testdata/flow.yaml",
		func(db *sql.DB) error {
			var err error
			spec, err = catalog.LoadMaterialization(db, "test/sqlite")
			return err
		}))

	generator := SQLGenerator()
	binding, err := newBinding(generator, 123, "test", spec.Bindings[0])
	require.Nil(t, err)

	// Note the intentional missing semicolon, as this is a subquery.
	require.Equal(t, `
		SELECT 123, l.`+"`flow_document`"+`
			`+"FROM `test` AS l"+`
			JOIN flow_temp_load_123 AS r
			`+"ON l.`key1` = r.`key1` AND l.`key2` = r.`key2`"+`
		`,
		binding.load.sql)

	require.Equal(t, `
		MERGE INTO `+"`test`"+` AS l
		USING flow_temp_store_123 AS r
		`+"ON l.`key1` = r.`key1` AND l.`key2` = r.`key2`"+`
		WHEN MATCHED AND r.`+"`flow_document`"+` IS NULL THEN
			DELETE
		WHEN MATCHED THEN
			`+"UPDATE SET l.`boolean` = r.`boolean`, l.`integer` = r.`integer`, l.`number` = r.`number`, l.`string` = r.`string`, l.`flow_document` = r.`flow_document`"+`
		WHEN NOT MATCHED THEN
			`+"INSERT (`key1`, `key2`, `boolean`, `integer`, `number`, `string`, `flow_document`)"+`
			`+"VALUES (r.`key1`, r.`key2`, r.`boolean`, r.`integer`, r.`number`, r.`string`, r.`flow_document`)"+`
		;`,
		binding.store.sql)

	// Enable delta mode binding and test again.
	spec.Bindings[0].DeltaUpdates = true
	binding, err = newBinding(generator, 123, "test", spec.Bindings[0])
	require.Nil(t, err)

	require.Equal(t, `
		`+"INSERT INTO `test` (`key1`, `key2`, `boolean`, `integer`, `number`, `string`, `flow_document`)"+`
		`+"SELECT `key1`, `key2`, `boolean`, `integer`, `number`, `string`, `flow_document` FROM flow_temp_store_123"+`
		;`,
		binding.store.sql)

}

func TestSpecification(t *testing.T) {
	var resp, err = newBigQueryDriver().
		Spec(context.Background(), &pm.SpecRequest{EndpointType: pf.EndpointType_AIRBYTE_SOURCE})
	require.NoError(t, err)

	log.Print(resp)
	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
