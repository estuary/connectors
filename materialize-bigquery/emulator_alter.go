package main

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
)

// The goccy emulator cannot evolve an existing table's schema in place — this
// is the "schema evolution bug in goccy/bigquery-emulator" that PR #4721
// worked around by disabling tests. Characterized against goccy 0.8.1:
//
//   - A query-path ALTER TABLE is accepted but has no effect. goccy's query
//     engine is github.com/goccy/googlesqlite (v0.3.1), which deliberately
//     routes ResolvedAlterTableStmt to a NoopStmtAction: "statements that the
//     analyzer accepts but whose side effects fall outside the driver's
//     scope" (googlesqlite internal/stmt_action.go). The statement is
//     analyzed, reports success, and changes nothing.
//
//   - The tables.patch / tables.update metadata APIs update only the
//     emulator's metadata repository (goccy server/handler.go
//     tablesPatchHandler), never the SQLite storage layer. A patched-in
//     column shows up in tables.get but any query using it fails with
//     "Column <name> is not present in table <table>".
//
// The one operation that installs a schema in BOTH layers is table creation:
// tables.insert and DDL CREATE TABLE each register the table with the
// metadata repository and the storage layer. So, for the detected emulator
// only, table alterations — including column type migrations, which
// StdColumnTypeMigrations would otherwise render as a temp-column ADD COLUMN
// / UPDATE / DROP COLUMN / RENAME COLUMN sequence — are applied by recreating
// the table:
//
//  1. stage the current rows into a scratch table with CREATE TABLE AS SELECT,
//  2. drop the target and re-create it via tables.insert with the current
//     metadata schema plus the added columns (and any dropped NOT NULLs),
//     with type-changed columns re-declared at their migrated type,
//  3. copy the rows back — casting type-changed columns with the same
//     CastSQL the production migration path uses — and drop the scratch
//     table.
//
// A migrated column is left nullable, matching production: BigQuery's final
// "SET NOT NULL" migration step is unconditionally a no-op (see
// columnMigrationSteps), so a migrated column never ends up NOT NULL there
// either.
//
// This is not atomic: a crash between steps 2 and 3 loses the target table's
// contents (they remain in the scratch table). That is acceptable only
// because this path is gated to a detected emulator, which is a disposable
// test/dev environment by definition.
func (c *client) emulatorAlterTable(ctx context.Context, ta sql.TableAlter) error {
	var projectID, datasetID = ta.Path[0], ta.Path[1]
	var tableName = translateFlowIdentifier(ta.Path[2])
	var scratchName = tableName + "_flow_alter_scratch"

	var dataset = c.bigqueryClient.DatasetInProject(projectID, datasetID)
	var target = dataset.Table(tableName)
	var scratch = dataset.Table(scratchName)

	var targetIdent = c.ep.Identifier(projectID, datasetID, tableName)
	var scratchIdent = c.ep.Identifier(projectID, datasetID, scratchName)

	md, err := target.Metadata(ctx)
	if err != nil {
		return fmt.Errorf("reading metadata of %q for emulator table recreation: %w", targetIdent, err)
	}

	var typeChanges = make(map[string]sql.ColumnTypeMigration, len(ta.ColumnTypeChanges))
	for _, m := range ta.ColumnTypeChanges {
		typeChanges[translateFlowIdentifier(m.Field)] = m
	}

	// The re-created table keeps every physical column the table currently
	// has — including columns no longer part of the materialized field
	// selection, which a production ALTER would also leave in place.
	var newSchema = make(bigquery.Schema, 0, len(md.Schema)+len(ta.AddColumns))
	var insertColumns = make([]string, 0, len(md.Schema))
	var selectExprs = make([]string, 0, len(md.Schema))
	for _, f := range md.Schema {
		var field = *f
		var colIdent = c.ep.Identifier(field.Name)
		insertColumns = append(insertColumns, colIdent)

		var selectExpr = colIdent
		if migration, ok := typeChanges[field.Name]; ok {
			fieldType, err := emulatorFieldType(migration.BareDDL)
			if err != nil {
				return err
			}
			field.Type = fieldType
			field.Required = false
			selectExpr = migration.CastSQL(migration)
		}
		if field.Type == bigquery.FloatFieldType {
			// goccy quirk (see emulatorFloatCastSQL doc): every FLOAT column
			// in the recreated table needs this, not only migrated ones,
			// because the table.Create call below re-creates every column —
			// including untouched ones — through the same tainted API.
			selectExpr = emulatorFloatCastSQL(colIdent)
		}
		selectExprs = append(selectExprs, selectExpr)

		for _, drop := range ta.DropNotNulls {
			if field.Name == drop.Name {
				field.Required = false
			}
		}
		newSchema = append(newSchema, &field)
	}
	for _, col := range ta.AddColumns {
		fieldType, err := emulatorFieldType(col.NullableDDL)
		if err != nil {
			return err
		}
		newSchema = append(newSchema, &bigquery.FieldSchema{
			Name: translateFlowIdentifier(col.Field),
			Type: fieldType,
		})
	}

	// A leftover scratch table from a run that crashed mid-recreation would
	// fail the CREATE TABLE AS SELECT below.
	_ = scratch.Delete(ctx)

	if _, err := c.query(ctx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s;", scratchIdent, targetIdent)); err != nil {
		return fmt.Errorf("staging %q rows for emulator table recreation: %w", targetIdent, err)
	}
	if _, err := c.query(ctx, fmt.Sprintf("DROP TABLE %s;", targetIdent)); err != nil {
		return fmt.Errorf("dropping %q for emulator table recreation: %w", targetIdent, err)
	}
	if err := target.Create(ctx, &bigquery.TableMetadata{Schema: newSchema, Clustering: md.Clustering}); err != nil {
		return fmt.Errorf("re-creating %q with altered schema: %w", targetIdent, err)
	}
	var insertList = strings.Join(insertColumns, ", ")
	var selectList = strings.Join(selectExprs, ", ")
	if _, err := c.query(ctx, fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s;", targetIdent, insertList, selectList, scratchIdent)); err != nil {
		return fmt.Errorf("restoring %q rows after emulator table recreation: %w", targetIdent, err)
	}
	if _, err := c.query(ctx, fmt.Sprintf("DROP TABLE %s;", scratchIdent)); err != nil {
		return fmt.Errorf("dropping emulator recreation scratch table %q: %w", scratchIdent, err)
	}

	return nil
}

// emulatorFloatCastSQL renders the expression that copies a FLOAT64 column's
// value into its re-created counterpart.
//
// Characterized against goccy 0.8.1: a column created via the metadata
// tables.insert API (as every column of a recreated table is, whether
// migrated or merely carried over unchanged) with FieldType FLOAT rejects
// "INSERT ... SELECT CAST(x AS FLOAT64) ..." — and even a bare, uncast
// SELECT of another FLOAT64 column — with "failed to analyze: Query column N
// has type DOUBLE which cannot be inserted into column <name>, which has
// type FLOAT". The identical INSERT succeeds against a column created by a
// DDL `CREATE TABLE` statement, even though the metadata API reports both
// columns' type identically as "FLOAT". Casting with the legacy "FLOAT"
// keyword instead of "FLOAT64" avoids the mismatch on the tables.insert
// -created column in both cases. This is specific to FLOAT64: the same
// hands-on check against BIGNUMERIC, STRING, and BYTES targets (the
// dialect's only other migration targets) found no equivalent issue.
func emulatorFloatCastSQL(colIdent string) string {
	return fmt.Sprintf("CAST(%s AS FLOAT)", colIdent)
}

// emulatorFieldType maps a DDL column type rendered by the emulator dialect
// (see bqDialect) to the metadata-API field type used when re-creating a
// table via tables.insert.
func emulatorFieldType(ddl string) (bigquery.FieldType, error) {
	// Strip a precision suffix, as in BIGNUMERIC(38,0). The metadata API
	// reports plain BIGNUMERIC for such columns and the InfoSchema strips
	// parameters anyway.
	switch base, _, _ := strings.Cut(strings.ToUpper(ddl), "("); base {
	case "STRING":
		return bigquery.StringFieldType, nil
	case "INT64", "INTEGER":
		return bigquery.IntegerFieldType, nil
	case "FLOAT64", "FLOAT":
		return bigquery.FloatFieldType, nil
	case "BOOL", "BOOLEAN":
		return bigquery.BooleanFieldType, nil
	case "BYTES":
		return bigquery.BytesFieldType, nil
	case "DATE":
		return bigquery.DateFieldType, nil
	case "TIMESTAMP":
		return bigquery.TimestampFieldType, nil
	case "BIGNUMERIC":
		return bigquery.BigNumericFieldType, nil
	case "JSON":
		return bigquery.JSONFieldType, nil
	}
	return "", fmt.Errorf("no metadata-API field type known for DDL type %q", ddl)
}
