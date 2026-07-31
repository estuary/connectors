package sql

import (
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// Only a date-time projection of /_meta/uuid is a clock source: the raw UUID is
// not a timestamp, and a binding with neither reports no clock at all.
func TestMetaUUIDClockColumn(t *testing.T) {
	col := func(field, ptr, format string) Column {
		inf := pf.Inference{Types: []string{"string"}}
		if format != "" {
			inf.String_ = &pf.Inference_String{Format: format}
		}
		return Column{Projection: Projection{Projection: pf.Projection{Field: field, Ptr: ptr, Inference: inf}}}
	}

	for _, tc := range []struct {
		name  string
		vals  []Column
		found string
	}{
		{"flow_published_at is the clock", []Column{col("flow_published_at", "/_meta/uuid", "date-time")}, "flow_published_at"},
		{"a raw uuid field is not a clock", []Column{col("_meta/uuid", "/_meta/uuid", "uuid")}, ""},
		{"an unrelated _meta field is not a clock", []Column{col("_meta/op", "/_meta/op", "")}, ""},
		{"no projection of the location", []Column{col("id", "/id", "")}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl := Table{Values: tc.vals}
			got := tbl.MetaUUIDClockColumn()
			if tc.found == "" {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Equal(t, tc.found, got.Field)
			}
		})
	}
}
