package boilerplate

import (
	"math"
	"testing"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/flow/go/labels"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	pb "go.gazette.dev/core/broker/protocol"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestIsMaterializationSpecRuntimeV2(t *testing.T) {
	var withLabels = func(nv ...string) *pf.MaterializationSpec {
		return &pf.MaterializationSpec{
			ShardTemplate: &pc.ShardSpec{LabelSet: pb.MustLabelSet(nv...)},
		}
	}

	for _, tt := range []struct {
		name string
		spec *pf.MaterializationSpec
		want bool
	}{
		{
			// Spelled out rather than composed from runtimeV2Flag: this label is
			// a wire contract with the reactor, so the test pins the exact name
			// the reactor reads.
			name: "flag set to true",
			spec: withLabels("estuary.dev/flag/enable-runtime-v2", "true"),
			want: true,
		},
		{
			name: "flag set to true alongside other flags",
			spec: withLabels(
				runtimeV2Flag, "true",
				labels.FlagPrefix+"something-else", "true",
			),
			want: true,
		},
		{
			name: "flag set to false",
			spec: withLabels(runtimeV2Flag, "false"),
			want: false,
		},
		{
			name: "flag absent",
			spec: withLabels(labels.FlagPrefix+"something-else", "true"),
			want: false,
		},
		{
			name: "no labels at all",
			spec: withLabels(),
			want: false,
		},
		{
			// The runtime compares the label value verbatim, so anything but an
			// exact "true" leaves the task on the V1 runtime.
			name: "flag set to a truthy value other than true",
			spec: withLabels(runtimeV2Flag, "TRUE"),
			want: false,
		},
		{
			name: "nil shard template",
			spec: &pf.MaterializationSpec{},
			want: false,
		},
		{
			name: "nil spec",
			spec: nil,
			want: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsMaterializationSpecRuntimeV2(tt.spec))
		})
	}
}

func TestRequireSingleShard(t *testing.T) {
	for _, tc := range []struct {
		name    string
		r       *pf.RangeSpec
		wantErr bool
	}{
		{name: "no range", r: nil},
		{name: "full key range", r: &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}},
		{name: "lower half", r: &pf.RangeSpec{KeyEnd: 0x7fffffff, RClockEnd: math.MaxUint32}, wantErr: true},
		{name: "upper half", r: &pf.RangeSpec{KeyBegin: 0x80000000, KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := RequireSingleShard(pm.Request_Open{Range: tc.r})
			if tc.wantErr {
				var userErr *cerrors.UserError
				require.ErrorAs(t, err, &userErr)
				require.ErrorContains(t, err, "does not support tasks split into multiple shards")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
