package materialize

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

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
