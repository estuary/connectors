package sql

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestPublishedAtColumn(t *testing.T) {
	var column = func(field, ptr string, str *pf.Inference_String, userDDL bool) Column {
		return Column{
			Projection: Projection{Projection: pf.Projection{
				Field:     field,
				Ptr:       ptr,
				Inference: pf.Inference{String_: str},
			}},
			MappedType: MappedType{UserDefinedDDL: userDDL},
		}
	}
	var timestamp = &pf.Inference_String{Format: "date-time", ContentEncoding: "uuid"}

	for _, tc := range []struct {
		name    string
		values  []Column
		field   string
		wantErr string
	}{
		{
			name:   "qualifying column",
			values: []Column{column("other", "/other", nil, false), column("flow_published_at", "/_meta/uuid", timestamp, false)},
			field:  "flow_published_at",
		},
		{
			name:    "field excluded",
			values:  []Column{column("other", "/other", nil, false)},
			wantErr: "the binding excludes the flow_published_at field, which must be included to enable deletion after a backfill",
		},
		{
			name:    "user-defined DDL",
			values:  []Column{column("flow_published_at", "/_meta/uuid", timestamp, true)},
			wantErr: "field flow_published_at has a user-defined DDL type",
		},
		{
			name:    "not a string",
			values:  []Column{column("flow_published_at", "/_meta/uuid", nil, false)},
			wantErr: "field flow_published_at projects /_meta/uuid without a string type, where a date-time with uuid encoding is required",
		},
		{
			name:    "unexpected format",
			values:  []Column{column("flow_published_at", "/_meta/uuid", &pf.Inference_String{Format: "uuid"}, false)},
			wantErr: `field flow_published_at projects /_meta/uuid with the unexpected format "uuid" and content encoding "", where a date-time with uuid encoding is required`,
		},
		{
			name:   "later qualifying column wins over earlier mismatch",
			values: []Column{column("custom", "/_meta/uuid", timestamp, true), column("flow_published_at", "/_meta/uuid", timestamp, false)},
			field:  "flow_published_at",
		},
		{
			name:    "first mismatch is reported",
			values:  []Column{column("custom", "/_meta/uuid", timestamp, true), column("flow_published_at", "/_meta/uuid", nil, false)},
			wantErr: "field custom has a user-defined DDL type",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var table = Table{Values: tc.values}
			col, err := table.PublishedAtColumn()
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				require.Nil(t, col)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.field, col.Field)
		})
	}
}
