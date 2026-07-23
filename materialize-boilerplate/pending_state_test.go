package boilerplate

import (
	"encoding/json"
	"testing"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestStateUnchanged(t *testing.T) {
	prior := json.RawMessage(`{"a":{"q":"Q1"},"b":{"q":"Q2"}}`)

	for _, tt := range []struct {
		name   string
		update *pf.ConnectorState
		want   bool
	}{
		{
			name:   "merge patch removing an entry changes state",
			update: &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"a":null}`), MergePatch: true},
			want:   false,
		},
		{
			name:   "merge patch removing an absent entry is unchanged",
			update: &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"c":null}`), MergePatch: true},
			want:   true,
		},
		{
			name:   "empty merge patch is unchanged",
			update: &pf.ConnectorState{UpdatedJson: json.RawMessage(`{}`), MergePatch: true},
			want:   true,
		},
		{
			name:   "nested merge patch removing an entry changes state",
			update: &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"a":{"q":null}}`), MergePatch: true},
			want:   false,
		},
		{
			name:   "full replacement with identical document is unchanged",
			update: &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"b":{"q":"Q2"},"a":{"q":"Q1"}}`)},
			want:   true,
		},
		{
			name:   "full replacement with a subset changes state",
			update: &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"b":{"q":"Q2"}}`)},
			want:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, stateUnchanged(prior, tt.update))
		})
	}
}
