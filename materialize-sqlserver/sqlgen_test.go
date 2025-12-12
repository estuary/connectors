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

var testDialect = createSqlServerDialect("Latin1_General_100_BIN2_UTF8", "dbo", featureFlagDefaults)
var testTemplates = renderTemplates(testDialect)

func TestSQLGeneration(t *testing.T) {

	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.tempLoadTruncate,
				testTemplates.tempStoreTruncate,
				testTemplates.createLoadTable,
				testTemplates.createStoreTable,
				testTemplates.createTargetTable,
				testTemplates.directCopy,
				testTemplates.mergeInto,
				testTemplates.loadInsert,
				testTemplates.loadQuery,
				testTemplates.loadQueryNoFlowDocument,
			},
			TplAddColumns:  testTemplates.alterTableColumns,
			TplUpdateFence: testTemplates.updateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "DATETIME2 NOT NULL", mapped.DDL)

	expected, err := time.Parse(time.RFC3339Nano, "2022-04-04T10:09:08.234567Z")
	require.NoError(t, err)
	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, expected, parsed)
	require.NoError(t, err)
}

func TestDateTimePKColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "varchar(900) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL", mapped.DDL)
}

func TestTimeColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "TIME NOT NULL", mapped.DDL)

	expected, err := time.Parse("15:04:05Z07:00", "10:09:08.234567Z")
	require.NoError(t, err)
	parsed, err := mapped.Converter("10:09:08.234567Z")
	require.Equal(t, expected, parsed)
	require.NoError(t, err)
}

func TestBinaryColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "base64"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: false,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "varchar(MAX) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL", mapped.DDL)
}

func TestBinaryPKColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "base64"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "varchar(900) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL", mapped.DDL)
}

func TestCompatibleTextType(t *testing.T) {
	type CompatibleCheck struct {
		existing   boilerplate.ExistingField
		compatible bool
	}
	tests := []struct {
		name        string
		projection  *sql.Projection
		expectedDDL string
		compatCheck []CompatibleCheck
	}{
		{
			name: "PK no maxLength",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types:   []string{"string"},
						String_: &pf.Inference_String{},
						Exists:  pf.Inference_MUST,
					},
					IsPrimaryKey: true,
				},
			},
			expectedDDL: "varchar(900) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			compatCheck: []CompatibleCheck{
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: -1,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 900,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 16,
					},
					compatible: false,
				},
			},
		},
		{
			name: "PK maxLength",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types: []string{"string"},
						String_: &pf.Inference_String{
							MaxLength: 16,
						},
						Exists: pf.Inference_MUST,
					},
					IsPrimaryKey: true,
				},
			},
			expectedDDL: "varchar(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			compatCheck: []CompatibleCheck{
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: -1,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 64,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 63,
					},
					compatible: false,
				},
			},
		},
		{
			name: "no maxLength",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types:   []string{"string"},
						String_: &pf.Inference_String{},
						Exists:  pf.Inference_MUST,
					},
					IsPrimaryKey: false,
				},
			},
			expectedDDL: "varchar(MAX) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			compatCheck: []CompatibleCheck{
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: -1,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 16,
					},
					compatible: false,
				},
			},
		},
		{
			name: "maxLength",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types: []string{"string"},
						String_: &pf.Inference_String{
							MaxLength: 16,
						},
						Exists: pf.Inference_MUST,
					},
					IsPrimaryKey: false,
				},
			},
			expectedDDL: "varchar(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			compatCheck: []CompatibleCheck{
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: -1,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 64,
					},
					compatible: true,
				},
				{
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 63,
					},
					compatible: false,
				},
			},
		},
	}

	var dialect = testDialect
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mapped = dialect.MapType(tt.projection, sql.FieldConfig{})
			require.Equal(t, tt.expectedDDL, mapped.DDL)

			for _, check := range tt.compatCheck {
				require.Equal(t, check.compatible, mapped.Compatible(check.existing), check)
			}

		})
	}
}

func TestCanMigrateTextType(t *testing.T) {
	type CheckExisting struct {
		desc       string
		existing   boilerplate.ExistingField
		canMigrate bool
	}
	tests := []struct {
		name       string
		projection *sql.Projection
		targetDDL  string
		checks     []CheckExisting
	}{
		{
			name: "no size info",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types:   []string{"string"},
						String_: &pf.Inference_String{},
						Exists:  pf.Inference_MUST,
					},
				},
			},
			targetDDL: "varchar(MAX) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			checks: []CheckExisting{
				{
					desc: "already max size",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: -1,
					},
					canMigrate: true,
				},
				{
					desc: "widen to max size is okay",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 64,
					},
					canMigrate: true,
				},
			},
		},
		{
			name: "maxLength",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types: []string{"string"},
						String_: &pf.Inference_String{
							MaxLength: 16,
						},
						Exists: pf.Inference_MUST,
					},
				},
			},
			targetDDL: "varchar(64) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			checks: []CheckExisting{
				{
					desc: "migrate should refuse to narrow MAX to a sized column",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: -1,
					},
					canMigrate: false,
				},
				{
					desc: "widen existing field is okay",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 63,
					},
					canMigrate: true,
				},
				{
					desc: "exact existing field is okay",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 64,
					},
					canMigrate: true,
				},
				{
					desc: "migrate should refuse to narrow existing field; could contain larger data",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 65,
					},
					canMigrate: false,
				},
			},
		},
		{
			name: "maxLength must use max",
			projection: &sql.Projection{
				Projection: pf.Projection{
					Inference: pf.Inference{
						Types: []string{"string"},
						String_: &pf.Inference_String{
							MaxLength: 8001,
						},
						Exists: pf.Inference_MUST,
					},
				},
			},
			targetDDL: "varchar(MAX) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL",
			checks: []CheckExisting{
				{
					desc: "can migrate field from sized to max storage",
					existing: boilerplate.ExistingField{
						Type:               "VARCHAR",
						CharacterMaxLength: 64,
					},
					canMigrate: true,
				},
			},
		},
	}

	var dialect = testDialect
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mapped = dialect.MapType(tt.projection, sql.FieldConfig{})
			mapped.MigratableTypes = dialect.MigratableTypes

			require.Equal(t, tt.targetDDL, mapped.DDL)

			for _, check := range tt.checks {
				require.Equal(t, check.canMigrate, mapped.CanMigrate(check.existing), check.desc)
			}
		})
	}
}
