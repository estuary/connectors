package filesink

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextFilekey(t *testing.T) {
	sk := "val"

	tests := []struct {
		name      string
		prefix    string
		path      string
		backfill  uint32
		prevCount uint64
		want      string
	}{
		{
			prefix:    "",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			want:      "path/v0000000000/00000000000000000000.something.gz",
		},
		{
			prefix:    "prefix",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			want:      "prefix/path/v0000000000/00000000000000000000.something.gz",
		},
		{
			prefix:    "prefix/",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			want:      "prefix/path/v0000000000/00000000000000000000.something.gz",
		},
		{
			prefix:    "",
			path:      "path",
			backfill:  1,
			prevCount: 0,
			want:      "path/v0000000001/00000000000000000000.something.gz",
		},
		{
			prefix:    "",
			path:      "path",
			backfill:  0,
			prevCount: 1,
			want:      "path/v0000000000/00000000000000000002.something.gz",
		},
		{
			prefix:    "prefix/",
			path:      "path",
			backfill:  2,
			prevCount: 3,
			want:      "prefix/path/v0000000002/00000000000000000004.something.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			b := binding{
				stateKey: sk,
				backfill: tt.backfill,
				path:     tt.path,
			}

			ta := transactor{
				state: connectorState{
					FileCounts: make(map[string]uint64),
				},
			}
			ta.common.Extension = ".something.gz"

			if tt.prevCount != 0 {
				ta.state.FileCounts[sk] = tt.prevCount
			}

			if tt.prefix != "" {
				ta.common.Prefix = tt.prefix
			}

			require.Equal(t, tt.want, ta.nextFileKey(b))
		})
	}

}
