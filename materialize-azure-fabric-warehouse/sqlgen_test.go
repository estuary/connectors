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
		func(table string) []string {
			return []string{"a-warehouse", "a-schema", table}
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
			tplStoreCopyIntoFromStagedQuery,
			tplStoreCopyIntoDirectQuery,
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
			Bounds: []sql.MergeBound{
				{
					Column:       tbl.Keys[0],
					LiteralLower: dialect.Literal(int64(10)),
					LiteralUpper: dialect.Literal(int64(100)),
				},
				{
					Column: tbl.Keys[1], // boolean key
				},
				{
					Column: tbl.Keys[2], // binary key
				},
			},
		}))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	{
		params := migrateParams{
			SourceTable: "some_table",
			TmpName:     "some_table_tmp",
			Columns: []migrateColumn{
				{Identifier: "not_migrated_column"},
				{Identifier: "is_migrated_column", CastSQL: "CAST(is_migrated_column AS VARCHAR(MAX))"},
				{Identifier: "another_not_migrated_column"},
				{Identifier: "migrated_boolean_column", CastSQL: bitToStringCast(sql.ColumnTypeMigration{
					Column: sql.Column{
						Identifier: "migrated_boolean_column",
						MappedType: sql.MappedType{NullableDDL: "VARCHAR(MAX)"},
					},
				})},
				{Identifier: "yet_another_not_migrated_column"},
			},
		}

		snap.WriteString("--- Begin createMigrationTable")
		require.NoError(t, tplCreateMigrationTable.Execute(snap, params))
		snap.WriteString("--- End createMigrationTable ---\n\n")
	}

	cupaloy.SnapshotT(t, snap.String())
}
