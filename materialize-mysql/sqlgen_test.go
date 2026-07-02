package main

import (
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = mysqlDialect(time.UTC, "flow", "mysql", featureFlagDefaults)
var testTemplates = renderTemplates(testDialect, "mysql")

func TestSQLGeneration(t *testing.T) {
	runSQLGen(t, testDialect, testTemplates)
}

func TestSQLGeneration_Singlestore(t *testing.T) {
	dialect := mysqlDialect(time.UTC, "flow", "singlestore", featureFlagDefaults)
	tpls := renderTemplates(dialect, "singlestore")
	runSQLGen(t, dialect, tpls)
}

func runSQLGen(t *testing.T, testDialect sql.Dialect, testTemplates templates) {
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

	// Near-RFC3339 variants: space separator and/or missing offset.
	parsed, err = mapped.Converter("2022-04-04 10:09:08.234567+00:00")
	require.Equal(t, "2022-04-04 10:09:08.234567", parsed)
	require.NoError(t, err)

	parsed, err = mapped.Converter("2022-04-04 10:09:08")
	require.Equal(t, "2022-04-04 10:09:08", parsed)
	require.NoError(t, err)
}

func TestSingleStoreClampDatetime(t *testing.T) {
	for _, tt := range []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: "2023-08-29T16:17:18Z",
			want:  "2023-08-29T16:17:18Z",
		},
		{
			input: "2025-11-29 01:05:28+00:00",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "2025-11-29 01:05:28",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "0999-12-31 23:59:59Z",
			want:  singleStoreMinimumTimestamp,
		},
		{
			input:   "not a timestamp",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := SingleStoreClampDatetime(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSingleStoreClampDate(t *testing.T) {
	for _, tt := range []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: "2023-08-29",
			want:  "2023-08-29",
		},
		{
			input: "0999-12-31",
			want:  singleStoreMinimumDate,
		},
		{
			input:   "not a date",
			wantErr: true,
		},
		{
			input:   "2025-11-29 01:05:28+00:00",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := SingleStoreClampDate(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
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

func TestBinaryPKColumn(t *testing.T) {
	var mapped = testDialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			IsPrimaryKey: true,
			Inference: pf.Inference{
				Types: []string{"string"},
				String_: &pf.Inference_String{
					ContentEncoding: "base64",
				},
				Exists: pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "VARBINARY(256) NOT NULL", mapped.DDL)

	var existing = boilerplate.ExistingField{
		Type: "VARBINARY",
	}
	require.True(t, mapped.Compatible(existing))
}
