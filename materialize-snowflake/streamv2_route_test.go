package connector

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// The routing hash itself is pinned to the runtime's implementations by the
// golden-vector test in go/keyhash, which this write path routes through.

func TestStreamV2TargetLayout(t *testing.T) {
	var full = streamV2Range{keyBegin: 0, keyEnd: math.MaxUint32}

	t.Run("full range quarters", func(t *testing.T) {
		layout, err := streamV2TargetLayout(0, math.MaxUint32)
		require.NoError(t, err)
		require.Equal(t, []streamV2Range{
			{keyBegin: 0x00000000, keyEnd: 0x3fffffff},
			{keyBegin: 0x40000000, keyEnd: 0x7fffffff},
			{keyBegin: 0x80000000, keyEnd: 0xbfffffff},
			{keyBegin: 0xc0000000, keyEnd: 0xffffffff},
		}, layout)
		require.True(t, streamV2LayoutCovers(layout, full))
	})

	t.Run("half range", func(t *testing.T) {
		layout, err := streamV2TargetLayout(0x80000000, math.MaxUint32)
		require.NoError(t, err)
		require.Equal(t, []streamV2Range{
			{keyBegin: 0x80000000, keyEnd: 0x9fffffff},
			{keyBegin: 0xa0000000, keyEnd: 0xbfffffff},
			{keyBegin: 0xc0000000, keyEnd: 0xdfffffff},
			{keyBegin: 0xe0000000, keyEnd: 0xffffffff},
		}, layout)
		require.True(t, streamV2LayoutCovers(layout, streamV2Range{keyBegin: 0x80000000, keyEnd: math.MaxUint32}))
	})

	t.Run("eighth of the range", func(t *testing.T) {
		layout, err := streamV2TargetLayout(0x20000000, 0x3fffffff)
		require.NoError(t, err)
		require.Equal(t, []streamV2Range{
			{keyBegin: 0x20000000, keyEnd: 0x27ffffff},
			{keyBegin: 0x28000000, keyEnd: 0x2fffffff},
			{keyBegin: 0x30000000, keyEnd: 0x37ffffff},
			{keyBegin: 0x38000000, keyEnd: 0x3fffffff},
		}, layout)
	})

	t.Run("uneven width is rejected", func(t *testing.T) {
		var _, err = streamV2TargetLayout(0, 5)
		require.ErrorContains(t, err, "cannot be subdivided evenly")
	})
}

func TestStreamV2LayoutCovers(t *testing.T) {
	var shard = streamV2Range{keyBegin: 0x40000000, keyEnd: 0x7fffffff}

	var cases = []struct {
		name   string
		layout []streamV2Range
		covers bool
	}{
		{"exact quarters", []streamV2Range{
			{keyBegin: 0x40000000, keyEnd: 0x4fffffff},
			{keyBegin: 0x50000000, keyEnd: 0x5fffffff},
			{keyBegin: 0x60000000, keyEnd: 0x6fffffff},
			{keyBegin: 0x70000000, keyEnd: 0x7fffffff},
		}, true},
		{"one range covering the shard", []streamV2Range{
			{keyBegin: 0x40000000, keyEnd: 0x7fffffff},
		}, true},
		{"gap between key ranges", []streamV2Range{
			{keyBegin: 0x40000000, keyEnd: 0x4ffffffe},
			{keyBegin: 0x50000000, keyEnd: 0x7fffffff},
		}, false},
		{"overlapping key ranges", []streamV2Range{
			{keyBegin: 0x40000000, keyEnd: 0x50000000},
			{keyBegin: 0x50000000, keyEnd: 0x7fffffff},
		}, false},
		{"short of the shard begin", []streamV2Range{
			{keyBegin: 0x40000001, keyEnd: 0x7fffffff},
		}, false},
		{"short of the shard end", []streamV2Range{
			{keyBegin: 0x40000000, keyEnd: 0x7ffffffe},
		}, false},
		{"empty layout", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.covers, streamV2LayoutCovers(tc.layout, shard))
		})
	}

	t.Run("key range ending at the top of the key space cannot wrap", func(t *testing.T) {
		// If the continuation check ran in uint32, 0xffffffff+1 would wrap to
		// zero and a layout circling past the top would read as contiguous.
		require.False(t, streamV2LayoutCovers([]streamV2Range{
			{keyBegin: 0x80000000, keyEnd: 0xffffffff},
			{keyBegin: 0x00000000, keyEnd: 0x7fffffff},
		}, streamV2Range{keyBegin: 0x80000000, keyEnd: 0x7fffffff}))
	})
}

func TestClassifyKeyRange(t *testing.T) {
	var shard = streamV2Range{keyBegin: 0x40000000, keyEnd: 0x7fffffff}
	targets, err := streamV2TargetLayout(shard.keyBegin, shard.keyEnd)
	require.NoError(t, err)

	var cases = []struct {
		name   string
		item   streamV2Range
		expect streamV2KeyRangeClass
	}{
		{"first target", targets[0], streamV2KeyRangeTarget},
		{"last target", targets[3], streamV2KeyRangeTarget},
		{"half of the shard", streamV2Range{keyBegin: 0x40000000, keyEnd: 0x5fffffff}, streamV2KeyRangeInherited},
		{"eighth of the shard", streamV2Range{keyBegin: 0x40000000, keyEnd: 0x47ffffff}, streamV2KeyRangeInherited},
		{"the whole shard range", shard, streamV2KeyRangeInherited},
		{"below the shard", streamV2Range{keyBegin: 0x00000000, keyEnd: 0x3fffffff}, streamV2KeyRangeSibling},
		{"above the shard", streamV2Range{keyBegin: 0x80000000, keyEnd: 0xffffffff}, streamV2KeyRangeSibling},
		{"adjacent below", streamV2Range{keyBegin: 0x30000000, keyEnd: 0x3fffffff}, streamV2KeyRangeSibling},
		{"straddling the low boundary", streamV2Range{keyBegin: 0x30000000, keyEnd: 0x4fffffff}, streamV2KeyRangeStraddling},
		{"straddling the high boundary", streamV2Range{keyBegin: 0x70000000, keyEnd: 0x8fffffff}, streamV2KeyRangeStraddling},
		{"covering the whole key space", streamV2Range{keyBegin: 0x00000000, keyEnd: 0xffffffff}, streamV2KeyRangeStraddling},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, classifyKeyRange(tc.item, shard, targets))
		})
	}
}

func TestStreamV2RouteHash(t *testing.T) {
	layout, err := streamV2TargetLayout(0x40000000, 0x7fffffff)
	require.NoError(t, err)

	var cases = []struct {
		name   string
		hash   uint32
		expect int
	}{
		{"exact begin of the first key range", 0x40000000, 0},
		{"exact end of the first key range", 0x4fffffff, 0},
		{"exact begin of the second key range", 0x50000000, 1},
		{"interior of the last key range", 0x7abcdef0, 3},
		{"exact end of the last key range", 0x7fffffff, 3},
		{"just below the shard", 0x3fffffff, -1},
		{"just above the shard", 0x80000000, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expect, streamV2RouteHash(layout, tc.hash))
		})
	}
}
