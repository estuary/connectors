package main

import (
	"maps"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = createRsDialect(false, featureFlagDefaults)
var testTemplates = renderTemplates(testDialect)

func flagsWithoutNativeBinary() map[string]bool {
	out := maps.Clone(featureFlagDefaults)
	out["native_binary_column_type"] = false
	return out
}

func TestSQLGeneration(t *testing.T) {
	runSQLGen(t, testDialect, testTemplates)
}

func TestSQLGeneration_NoNativeBinaryColumnType(t *testing.T) {
	dialect := createRsDialect(false, flagsWithoutNativeBinary())
	tpls := renderTemplates(dialect)
	runSQLGen(t, dialect, tpls)
}

func runSQLGen(t *testing.T, testDialect sql.Dialect, testTemplates templates) {
	snap, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{"a-schema", table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
				testTemplates.mergeInto,
				testTemplates.loadQuery,
				testTemplates.loadQueryNoFlowDocument,
				testTemplates.deleteQuery,
			},
		},
	)

	for _, tbl := range tables {
		tpl := testTemplates.createLoadTable
		var testcase = tbl.Identifier + " " + tpl.Name()

		data := loadTableParams{
			Target: tbl,
		}

		snap.WriteString("--- Begin " + testcase + " (no varchar length) ---")
		require.NoError(t, tpl.Execute(snap, data))
		snap.WriteString("--- End " + testcase + " (no varchar length) ---\n\n")
	}

	for _, tbl := range tables {
		tpl := testTemplates.createLoadTable
		var testcase = tbl.Identifier + " " + tpl.Name()

		data := loadTableParams{
			Target:        tbl,
			VarCharLength: 400,
		}

		snap.WriteString("--- Begin " + testcase + " (with varchar length) ---")
		require.NoError(t, tpl.Execute(snap, data))
		snap.WriteString("--- End " + testcase + " (with varchar length) ---\n\n")
	}

	for _, tbl := range tables {
		t.Run("createStoreTable/"+tbl.Identifier, func(t *testing.T) {
			tpl := testTemplates.createStoreTable
			var testcase = tbl.Identifier + " " + tpl.Name()

			meta := make([]VarcharColumnMeta, len(tbl.Columns()))
			data := storeTableParams{
				Target:            &tbl,
				VarcharColumnMeta: meta,
			}

			snap.WriteString("--- Begin " + testcase + " (no varchar length) ---")
			require.NoError(t, tpl.Execute(snap, data))
			snap.WriteString("--- End " + testcase + " (no varchar length) ---\n\n")
		})

		t.Run("createStoreTable/"+tbl.Identifier+"/varchar", func(t *testing.T) {
			tpl := testTemplates.createStoreTable
			var testcase = tbl.Identifier + " " + tpl.Name()

			meta := make([]VarcharColumnMeta, len(tbl.Columns()))
			for idx, col := range tbl.Columns() {
				if col.DDL == "TEXT" {
					meta[idx] = VarcharColumnMeta{MaxLength: idx + 42}
				}
			}
			data := storeTableParams{
				Target:            &tbl,
				VarcharColumnMeta: meta,
			}

			snap.WriteString("--- Begin " + testcase + " (with varchar length) ---")
			require.NoError(t, tpl.Execute(snap, data))
			snap.WriteString("--- End " + testcase + " (with varchar length) ---\n\n")
		})
	}

	for _, tbl := range tables {
		t.Run("createDeleteTable/"+tbl.Identifier, func(t *testing.T) {
			tpl := testTemplates.createDeleteTable
			var testcase = tbl.Identifier + " " + tpl.Name()

			meta := make([]VarcharColumnMeta, len(tbl.Columns()))
			data := deleteTableParams{
				Target:            &tbl,
				VarcharColumnMeta: meta,
			}

			snap.WriteString("--- Begin " + testcase + " (no varchar length) ---")
			require.NoError(t, tpl.Execute(snap, data))
			snap.WriteString("--- End " + testcase + " (no varchar length) ---\n\n")
		})

		t.Run("createDeleteTable/"+tbl.Identifier+"/varchar", func(t *testing.T) {
			tpl := testTemplates.createDeleteTable
			var testcase = tbl.Identifier + " " + tpl.Name()

			meta := make([]VarcharColumnMeta, len(tbl.Columns()))
			for idx, col := range tbl.Columns() {
				if col.DDL == "TEXT" {
					meta[idx] = VarcharColumnMeta{MaxLength: idx + 42}
				}
			}
			data := deleteTableParams{
				Target:            &tbl,
				VarcharColumnMeta: meta,
			}

			snap.WriteString("--- Begin " + testcase + " (with varchar length) ---")
			require.NoError(t, tpl.Execute(snap, data))
			snap.WriteString("--- End " + testcase + " (with varchar length) ---\n\n")
		})
	}

	var copyParams = copyFromS3Params{
		Target: "my_temp_table",
		Columns: []*sqlDriver.Column{
			{
				Identifier: "firstCol",
			},
			{
				Identifier: "secondCol",
			},
		},
		ManifestURL: "s3://some_bucket/files.manifest",
		Config: config{
			AWSAccessKeyID:     "accessKeyID",
			AWSSecretAccessKey: "secretKey",
			Region:             "us-somewhere-1",
		},
		CaseSensitiveIdentifierEnabled: false,
	}

	snap.WriteString("--- Begin Copy From S3 Without Case Sensitive Identifiers or Truncation ---")
	require.NoError(t, testTemplates.copyFromS3.Execute(snap, copyParams))
	snap.WriteString("--- End Copy From S3 Without Case Sensitive Identifier or Truncation ---\n\n")

	copyParams.CaseSensitiveIdentifierEnabled = true
	copyParams.TruncateColumns = true

	snap.WriteString("--- Begin Copy From S3 With Case Sensitive Identifiers and Truncation ---")
	require.NoError(t, testTemplates.copyFromS3.Execute(snap, copyParams))
	snap.WriteString("--- End Copy From S3 With Case Sensitive Identifiers and Truncation ---")

	cupaloy.SnapshotT(t, snap.String())
}

func TestTruncatedIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no truncation",
			input: "hello",
			want:  "hello",
		},
		{
			name:  "truncate ASCII",
			input: strings.Repeat("a", 128),
			want:  strings.Repeat("a", 127),
		},
		{
			name:  "truncate UTF-8",
			input: strings.Repeat("a", 125) + "码",
			want:  strings.Repeat("a", 125),
		},
		{
			name:  "empty input",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, truncatedIdentifier(tt.input))
		})
	}
}

func TestIdentifierer(t *testing.T) {
	tests := []struct {
		name       string
		identifier []string
		expected   string
	}{
		{
			name:       "reserved undocumented",
			identifier: []string{"queryid"},
			expected:   `"queryid"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := testDialect.Identifier(tt.identifier...)
			require.Equal(t, tt.expected, actual)
		})
	}
}
