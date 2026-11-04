package connector

import (
	"math"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestRequireMultiShardCapable(t *testing.T) {
	var full = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
	var keySplit = &pf.RangeSpec{KeyBegin: 0x80000000, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}
	var rClockSplit = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: 0x7fffffff}

	var bind = func(identifier string, configure func(*binding)) *binding {
		var b = &binding{target: sql.Table{Identifier: identifier}}
		configure(b)
		return b
	}
	var staged = bind("staged_table", func(*binding) {})
	var streamV2 = bind("v2_table", func(b *binding) { b.streamingV2 = true })
	var streamV1 = bind("v1_table", func(b *binding) { b.streaming = true })
	var pipe = bind("pipe_table", func(b *binding) { b.pipeName = "FLOW_PIPE_0" })

	for _, tc := range []struct {
		name     string
		r        *pf.RangeSpec
		bindings []*binding
		wantErr  []string
	}{
		{name: "one shard takes any binding", r: full, bindings: []*binding{staged, streamV2, streamV1, pipe}},
		{name: "shards take streaming v2 bindings", r: keySplit, bindings: []*binding{streamV2}},
		{name: "a key split refuses every other binding", r: keySplit,
			bindings: []*binding{streamV2, staged, streamV1, pipe}, wantErr: []string{"staged_table", "v1_table", "pipe_table"}},
		{name: "an r-clock split refuses a staged binding", r: rClockSplit,
			bindings: []*binding{staged}, wantErr: []string{"staged_table"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var err = requireMultiShardCapable(tc.r, tc.bindings)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, s := range tc.wantErr {
				require.ErrorContains(t, err, s)
			}
			require.NotContains(t, err.Error(), "v2_table")
		})
	}
}
