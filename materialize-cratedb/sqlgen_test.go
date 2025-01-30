package main

import (
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	snap, _ := sql.RunSqlGenTests(
		t,
		pgDialect,
		func(table string, delta bool) sql.Resource {
			return tableConfig{
				Table: table,
				Delta: delta,
			}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				tplCreateTargetTable,
				tplCreateLoadTable,
				tplLoadInsert,
				tplLoadQuery,
				tplStoreInsert,
				tplStoreUpdate,
				tplDeleteQuery,
			},
			TplAddColumns:    tplAlterTableColumns,
			TplDropNotNulls:  tplAlterTableColumns,
			TplCombinedAlter: tplAlterTableColumns,
			TplInstallFence:  tplInstallFence,
			TplUpdateFence:   tplUpdateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var mapped, err = pgDialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "TIMESTAMPTZ NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, "2022-04-04T10:09:08.234567Z", parsed)
	require.NoError(t, err)
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
			input: strings.Repeat("a", 64),
			want:  strings.Repeat("a", 63),
		},
		{
			name:  "truncate UTF-8",
			input: strings.Repeat("a", 61) + "码",
			want:  strings.Repeat("a", 61),
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
