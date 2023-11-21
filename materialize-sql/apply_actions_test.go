package sql

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestFilterActions(t *testing.T) {
	table1 := Table{
		InfoLocation: InfoTableLocation{
			TableSchema: "schema",
			TableName:   "foo",
		},
	}

	table2 := Table{
		InfoLocation: InfoTableLocation{
			TableSchema: "schema",
			TableName:   "bar",
		},
	}

	tableCreate1 := TableCreate{
		Table:          table1,
		TableCreateSql: "CREATE TABLE foo",
	}

	tableCreate2 := TableCreate{
		Table:          table2,
		TableCreateSql: "CREATE TABLE foo",
	}

	newColumn := Column{
		Projection: Projection{
			Projection: pf.Projection{
				Field: "newColumn",
			},
		},
	}

	tests := []struct {
		name     string
		in       ApplyActions
		existing ExistingColumns
		want     ApplyActions
		wantErr  bool
	}{
		{
			name:     "empty input",
			in:       ApplyActions{},
			existing: ExistingColumns{},
			want:     ApplyActions{},
			wantErr:  false,
		},
		{
			name:     "nothing exists",
			in:       ApplyActions{CreateTables: []TableCreate{tableCreate1, tableCreate2}},
			existing: ExistingColumns{},
			want:     ApplyActions{CreateTables: []TableCreate{tableCreate1, tableCreate2}},
			wantErr:  false,
		},
		{
			name: "one table exists",
			in:   ApplyActions{CreateTables: []TableCreate{tableCreate1, tableCreate2}},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"foo": {}},
				},
			},
			want:    ApplyActions{CreateTables: []TableCreate{tableCreate2}},
			wantErr: false,
		},
		{
			name: "alter table add new column",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:      table1,
					AddColumns: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"foo": {}},
				},
			},
			want: ApplyActions{
				AlterTables: []TableAlter{{
					Table:      table1,
					AddColumns: []Column{newColumn},
				}},
			},
			wantErr: false,
		},
		{
			name: "alter table add new column that was already edded",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:      table1,
					AddColumns: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"foo": {{
						Name:     "newColumn",
						Nullable: true,
						Type:     "TEXT",
					}}},
				},
			},
			want:    ApplyActions{},
			wantErr: false,
		},
		{
			name: "alter table drop nullability constraint",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:        table1,
					DropNotNulls: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"foo": {{
						Name:     "newColumn",
						Nullable: false,
						Type:     "TEXT",
					}}},
				},
			},
			want: ApplyActions{
				AlterTables: []TableAlter{{
					Table:        table1,
					DropNotNulls: []Column{newColumn},
				}},
			},
			wantErr: false,
		},
		{
			name: "alter table drop nullability constraint but column is already nullable",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:        table1,
					DropNotNulls: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"foo": {{
						Name:     "newColumn",
						Nullable: true,
						Type:     "TEXT",
					}}},
				},
			},
			want:    ApplyActions{},
			wantErr: false,
		},
		{
			name: "alter table add new column to table that doesn't exist",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:      table1,
					AddColumns: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"bar": {}},
				},
			},
			want:    ApplyActions{},
			wantErr: true,
		},
		{
			name: "alter table drop nullability on a column that doesn't exist",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:        table1,
					DropNotNulls: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"foo": {}},
				},
			},
			want:    ApplyActions{},
			wantErr: true,
		},
		{
			name: "alter table drop nullability on a table that doesn't exist",
			in: ApplyActions{
				AlterTables: []TableAlter{{
					Table:        table1,
					DropNotNulls: []Column{newColumn},
				}},
			},
			existing: ExistingColumns{
				tables: map[string]map[string][]ExistingColumn{
					"schema": {"bar": {}},
				},
			},
			want:    ApplyActions{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FilterActions(tt.in, newTestDialect(), &tt.existing)
			require.Equal(t, tt.want, got)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestExistingColumns(t *testing.T) {
	// The tests for FilterActions verify the hasTable/hasColumn/nullable operations, so this test
	// is just making sure that adding a duplicate column is not allowed.
	e := ExistingColumns{}
	e.PushColumn("schema", "table", "column", true, "TEXT", 255)
	require.Panics(t, func() { e.PushColumn("schema", "table", "column", true, "TEXT", 255) })
}
