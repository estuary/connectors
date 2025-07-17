package main

import (
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

var testDialect = rsDialect(false)

func TestSQLGeneration(t *testing.T) {
	var templates = renderTemplates(testDialect)

	snap, tables := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{"a-schema", table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				templates.createTargetTable,
				templates.createStoreTable,
				templates.mergeInto,
				templates.loadQuery,
				templates.createDeleteTable,
				templates.deleteQuery,
			},
		},
	)

	for _, tbl := range tables {
		tpl := templates.createLoadTable
		var testcase = tbl.Identifier + " " + tpl.Name()

		data := loadTableParams{
			Target: tbl,
		}

		snap.WriteString("--- Begin " + testcase + " (no varchar length) ---")
		require.NoError(t, tpl.Execute(snap, data))
		snap.WriteString("--- End " + testcase + " (no varchar length) ---\n\n")
	}

	for _, tbl := range tables {
		tpl := templates.createLoadTable
		var testcase = tbl.Identifier + " " + tpl.Name()

		data := loadTableParams{
			Target:        tbl,
			VarCharLength: 400,
		}

		snap.WriteString("--- Begin " + testcase + " (with varchar length) ---")
		require.NoError(t, tpl.Execute(snap, data))
		snap.WriteString("--- End " + testcase + " (with varchar length) ---\n\n")
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
	require.NoError(t, templates.copyFromS3.Execute(snap, copyParams))
	snap.WriteString("--- End Copy From S3 Without Case Sensitive Identifier or Truncation ---\n\n")

	copyParams.CaseSensitiveIdentifierEnabled = true
	copyParams.TruncateColumns = true

	snap.WriteString("--- Begin Copy From S3 With Case Sensitive Identifiers and Truncation ---")
	require.NoError(t, templates.copyFromS3.Execute(snap, copyParams))
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
