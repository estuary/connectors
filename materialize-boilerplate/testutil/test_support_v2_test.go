package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSanitizeCheckpointHashes(t *testing.T) {
	sanitize := SanitizeCheckpointHashes(`"(?:previous|current)Checkpoint":"([0-9a-f]{16})"`, "checkpoint")

	for _, tt := range []struct {
		name string
		in   string
		want string
	}{
		{
			name: "numbers by first appearance, preserving the hand-off",
			in: `{"currentCheckpoint":"85b7f18623e2219e"}` +
				`{"currentCheckpoint":"42439dc2b4c3f643","previousCheckpoint":"85b7f18623e2219e"}`,
			want: `{"currentCheckpoint":"<checkpoint-1>"}` +
				`{"currentCheckpoint":"<checkpoint-2>","previousCheckpoint":"<checkpoint-1>"}`,
		},
		{
			name: "distinct values stay distinct",
			in:   `"currentCheckpoint":"aaaaaaaaaaaaaaaa" "currentCheckpoint":"bbbbbbbbbbbbbbbb"`,
			want: `"currentCheckpoint":"<checkpoint-1>" "currentCheckpoint":"<checkpoint-2>"`,
		},
		{
			name: "numbering restarts per invocation",
			in:   `"currentCheckpoint":"cccccccccccccccc"`,
			want: `"currentCheckpoint":"<checkpoint-1>"`,
		},
		{
			name: "surrounding text and unmatched values are untouched",
			in:   `{"fileKeys":["x"],"currentCheckpoint":"85b7f18623e2219e","other":"85b7f18623e2219e"}`,
			want: `{"fileKeys":["x"],"currentCheckpoint":"<checkpoint-1>","other":"85b7f18623e2219e"}`,
		},
		{
			name: "no matches",
			in:   `{"fileKeys":[]}`,
			want: `{"fileKeys":[]}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, sanitize(tt.in))
		})
	}
}

// materialize-snowflake's offset tokens carry a deterministic ":N" suffix within
// a transaction, which must survive sanitization.
func TestSanitizeCheckpointHashesKeepsTrailingSuffix(t *testing.T) {
	sanitize := SanitizeCheckpointHashes(`"offset_token":"([0-9a-f]{16}):\d+"`, "offset-token")

	require.Equal(t,
		`"offset_token":"<offset-token-1>:0" "offset_token":"<offset-token-1>:1" "offset_token":"<offset-token-2>:0"`,
		sanitize(`"offset_token":"436ad5d41833f72d:0" "offset_token":"436ad5d41833f72d:1" "offset_token":"af89dd2fc6ccd4a6:0"`),
	)
}
