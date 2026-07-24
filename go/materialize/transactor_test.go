package materialize

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStateKeyFilter(t *testing.T) {
	// nil processes everything, including keys that can't be enumerated from
	// the active bindings.
	all := StateKeyFilter(nil)
	require.True(t, all("a_table.v1"))
	require.True(t, all("removed_table.v1"))

	// A non-nil list processes exactly those keys; an empty list processes
	// nothing.
	some := StateKeyFilter([]string{"a_table.v1"})
	require.True(t, some("a_table.v1"))
	require.False(t, some("b_table.v1"))
	require.False(t, StateKeyFilter([]string{})("a_table.v1"))
}

func TestSplitStatePatches(t *testing.T) {
	// Fixtures mirror the wire format produced by the runtime's patch
	// encoder: a JSON array whose elements are each followed by a tab.
	for _, tt := range []struct {
		name    string
		payload string
		want    []json.RawMessage
		wantErr bool
	}{
		{name: "empty payload", payload: "", want: nil},
		{name: "empty array", payload: "[]", want: nil},
		{
			name:    "single patch",
			payload: "[{\"a\":1}\t]",
			want:    []json.RawMessage{json.RawMessage(`{"a":1}`)},
		},
		{
			name:    "reset followed by document",
			payload: "[null\t,{\"a\":1}\t]",
			want:    []json.RawMessage{json.RawMessage(`null`), json.RawMessage(`{"a":1}`)},
		},
		{
			name:    "multiple shard patches",
			payload: "[{\"00000000-7fffffff\":{}}\t,{\"80000000-ffffffff\":{}}\t]",
			want: []json.RawMessage{
				json.RawMessage(`{"00000000-7fffffff":{}}`),
				json.RawMessage(`{"80000000-ffffffff":{}}`),
			},
		},
		{name: "malformed", payload: "[{\"a\":1}", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SplitStatePatches(json.RawMessage(tt.payload))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i := range tt.want {
				require.JSONEq(t, string(tt.want[i]), string(got[i]))
			}
		})
	}
}
