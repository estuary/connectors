package backfill

import (
	"testing"
	"time"

	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/stretchr/testify/require"
)

var textColumn = &datatypes.TextColumnType{
	Type:      "varchar",
	Collation: "SQL_Latin1_General_CP1_CI_AS",
	FullType:  "varchar(32)",
	MaxLength: 32,
}

// TestResumeKeyBinding checks what Go type each key column's resume value is bound as.
// Datetime keys must arrive as a time.Time: binding the serialized string instead leaves
// SQL Server comparing the column against an nvarchar parameter, and the implicit
// conversion that introduces costs the chunk query its index seek. See retypeResumeKey.
func TestResumeKeyBinding(t *testing.T) {
	// Whole seconds, so every encoding round-trips exactly and equality is meaningful.
	var reference = time.Date(2020, 3, 4, 5, 6, 7, 0, time.UTC)

	for _, tc := range []struct {
		name       string
		columnType any
		key        any
		wantBound  any
	}{
		{"datetime2 binds as a time", "datetime2", reference, reference},
		{"smalldatetime binds as a time", "smalldatetime", reference, reference},
		{"datetimeoffset binds as a time", "datetimeoffset", reference, reference},
		{"date binds as a time", "date", reference, reference},

		// Types left alone on purpose; see retypeResumeKey. DATETIME's key encoding
		// truncates to milliseconds while the column resolves to 1/300s, so binding a
		// precise value would place the resume key below the row it came from and the
		// backfill would re-read it forever.
		{"datetime binds unchanged", "datetime", reference, reference.Format(datatypes.DatetimeKeyEncoding)},
		{"time binds unchanged", "time", reference, reference.Format(datatypes.SortableRFC3339Nano)},

		// Types whose values already round-trip usefully.
		{"int binds unchanged", "int", int64(1723), int64(1723)},
		{"text binds unchanged", textColumn, "some value", "some value"},
		{"numeric binds unchanged", "numeric", "12345.12059", "12345.12059"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := datatypes.EncodeKeyFDB(tc.key, tc.columnType)
			require.NoError(t, err)
			packed, err := sqlcapture.PackTuple([]tuple.TupleElement{encoded, 7})
			require.NoError(t, err)

			_, args, err := BuildBackfillQuery(&sqlcapture.TableState{
				Mode:       sqlcapture.TableStatePreciseBackfill,
				KeyColumns: []string{"k", "node"},
				Scanned:    packed,
			}, "dbo", "events", map[string]any{"k": tc.columnType, "node": "int"}, 1000)
			require.NoError(t, err)
			require.Len(t, args, 2)
			require.Equal(t, tc.wantBound, args[0])
		})
	}
}
