package filesink

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type nullStore struct {
}

func (*nullStore) SupportsPathPatternExpansion() bool {
	return true
}

func (*nullStore) StageObject(ctx context.Context, r io.Reader, key string) (*SinglePhase, error) {
	panic("not implemented")
}

func (*nullStore) CompleteObject(ctx context.Context, info *SinglePhase) error {
	panic("not implemented")
}

func TestNextFilekey(t *testing.T) {
	sk := "val"

	tests := []struct {
		name      string
		prefix    string
		path      string
		backfill  uint32
		prevCount uint64
		locString string
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
		{
			prefix:    "prefix/%Y/%m/%d",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			want:      "prefix/2026/02/11/path/v0000000000/00000000000000000000.something.gz",
		},
		{
			prefix:    "prefix/%Y-%m-%dT%H:%M:%S%z",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			want:      "prefix/2026-02-11T10:02:03+0000/path/v0000000000/00000000000000000000.something.gz",
		},
		{
			prefix:    "prefix/%Y-%m-%dT%H:%M:%S %Z",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			want:      "prefix/2026-02-11T10:02:03 UTC/path/v0000000000/00000000000000000000.something.gz",
		},
		{
			prefix:    "prefix/%Y-%m-%dT%H:%M:%S %Z",
			path:      "path",
			backfill:  0,
			prevCount: 0,
			locString: "Asia/Kathmandu",
			want:      "prefix/2026-02-11T15:47:03 +0545/path/v0000000000/00000000000000000000.something.gz",
		},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			b := binding{
				stateKey: sk,
				backfill: tt.backfill,
				path:     tt.path,
			}

			ta := transactor[*SinglePhase]{
				state: connectorState[*SinglePhase]{
					FileCounts: make(map[string]uint64),
				},
				store: &nullStore{},
			}
			ta.common.Extension = ".something.gz"

			if tt.prevCount != 0 {
				ta.state.FileCounts[sk] = tt.prevCount
			}

			if tt.prefix != "" {
				ta.common.Prefix = tt.prefix
			}

			tm := time.Date(2026, time.February, 11, 10, 2, 3, 4, time.UTC)
			if tt.locString != "" {
				loc, err := time.LoadLocation(tt.locString)
				require.NoError(t, err)
				tm = tm.In(loc)
			}
			require.Equal(t, tt.want, ta.nextFileKey(b, tm))
		})
	}
}
