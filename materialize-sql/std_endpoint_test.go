/*
materialize-sql is a test-only package which tests the
github.com/estuary/flow/go/protocols/materialize/sql package.

Test code cannot be directly included in that repository
due to its dependence on Flow catalog builds.
*/
package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/estuary/connectors/testsupport"
	"github.com/estuary/flow/go/protocols/catalog"
	pf "github.com/estuary/flow/go/protocols/flow"
	flowSql "github.com/estuary/flow/go/protocols/materialize/sql"
	_ "github.com/mattn/go-sqlite3" // Import for register side-effects.
	"github.com/stretchr/testify/require"
)

func TestStdEndpointExecuteLoadSpec(t *testing.T) {
	var spec *pf.MaterializationSpec
	require.NoError(t, testsupport.CatalogExtract(t, "testdata/flow.yaml",
		func(db *sql.DB) error {
			var err error
			spec, err = catalog.LoadMaterialization(db, "test/sqlite")
			return err
		}))

	// Simple test to get an example spec, persist it and load it to make sure it matches
	// Using sqlite as the implementation for the sql.DB database.
	var db, err = sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	// Leverage the Endpoint interface
	var endpoint = flowSql.NewStdEndpoint(nil, db, flowSql.SQLiteSQLGenerator(), flowSql.FlowTables{
		Checkpoints: flowSql.FlowCheckpointsTable(flowSql.DefaultFlowCheckpoints),
		Specs:       flowSql.FlowMaterializationsTable(flowSql.DefaultFlowMaterializations),
	})

	// Create the spec table.
	createSpecsSQL, err := endpoint.CreateTableStatement(endpoint.FlowTables().Specs)
	require.Nil(t, err)

	// Get an example spec, convert it to bytes.
	specBytes, err := spec.Marshal()
	require.Nil(t, err)

	var insertSpecSQL = fmt.Sprintf("INSERT INTO %s (version, spec, materialization) VALUES (%s, %s, %s);",
		endpoint.FlowTables().Specs.Identifier,
		endpoint.Generator().ValueRenderer.Render("example_version"),
		endpoint.Generator().ValueRenderer.Render(base64.StdEncoding.EncodeToString(specBytes)),
		endpoint.Generator().ValueRenderer.Render(spec.Materialization.String()),
	)

	var ctx = context.Background()

	// Create the table and put the spec in it.
	err = endpoint.ExecuteStatements(ctx, []string{
		createSpecsSQL,
		insertSpecSQL,
	})
	require.Nil(t, err)

	// Load the spec back out of the database and validate it.
	version, destSpec, err := endpoint.LoadSpec(ctx, spec.Materialization)
	require.NoError(t, err)
	require.Equal(t, "example_version", version)
	require.Equal(t, spec, destSpec)

	require.Nil(t, db.Close())
}
