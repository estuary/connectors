package connector

import (
	"fmt"
	"slices"

	"github.com/estuary/connectors/go/keyhash"
)

// streamV2ChannelsPerShard is the number of channels a shard subdivides its key
// range into, per binding. It is a package var so that tests can exercise other
// values; it is not user-configurable.
var streamV2ChannelsPerShard = 4

// packedKeyHashHH64 routes documents to channels by the same hash the runtime
// routes them to shards by, so a channel's contents depend only on the data and
// a split or join inherits whole channels.
var packedKeyHashHH64 = keyhash.PackedKeyHash_HH64

// streamV2TargetLayout cuts the shard key range [keyBegin, keyEnd], inclusive on
// both ends, into streamV2ChannelsPerShard equal subranges. This is the layout a
// shard's channels converge to.
func streamV2TargetLayout(keyBegin, keyEnd uint32) ([]streamV2Range, error) {
	// Math uses uint64 because the full range spans 1<<32 keys.
	var width = uint64(keyEnd) - uint64(keyBegin) + 1
	var n = uint64(streamV2ChannelsPerShard)
	if width%n != 0 {
		return nil, fmt.Errorf(
			"shard key range [%08x, %08x] cannot be subdivided evenly into %d channels",
			keyBegin, keyEnd, streamV2ChannelsPerShard,
		)
	}

	var step = width / n
	var layout = make([]streamV2Range, 0, streamV2ChannelsPerShard)
	for i := range n {
		var begin = uint64(keyBegin) + i*step
		layout = append(layout, streamV2Range{
			keyBegin: uint32(begin),
			keyEnd:   uint32(begin + step - 1),
		})
	}
	return layout, nil
}

// contains reports whether the subrange covers a key hash. Bounds are inclusive
// on both ends, as RangeSpec bounds are.
func (r streamV2Range) contains(hash uint32) bool {
	return r.keyBegin <= hash && hash <= r.keyEnd
}

// streamV2Subrange classifies a checkpoint item's channel subrange against the
// range of the shard reading it.
type streamV2Subrange int

const (
	// streamV2SubrangeTarget is a subrange of the shard's own target layout.
	streamV2SubrangeTarget streamV2Subrange = iota
	// streamV2SubrangeInherited lies within the shard's range but is not a target
	// subrange: it belonged to a shard this topology replaced, and this shard
	// continues it until a rebalance retires it.
	streamV2SubrangeInherited
	// streamV2SubrangeSibling lies entirely outside the shard's range. It belongs
	// to a live sibling and is none of this shard's business.
	streamV2SubrangeSibling
	// streamV2SubrangeStraddling crosses the shard's boundary. Splits are
	// midpoint-only and channels subdivide evenly, so no channel can straddle a
	// boundary; this classification is a refusal.
	streamV2SubrangeStraddling
)

// classifySubrange places one channel subrange relative to a shard range and the
// shard's target layout.
func classifySubrange(item streamV2Range, shard streamV2Range, targets []streamV2Range) streamV2Subrange {
	if slices.Contains(targets, item) {
		return streamV2SubrangeTarget
	}
	if shard.keyBegin <= item.keyBegin && item.keyEnd <= shard.keyEnd {
		return streamV2SubrangeInherited
	}
	if item.keyEnd < shard.keyBegin || item.keyBegin > shard.keyEnd {
		return streamV2SubrangeSibling
	}
	return streamV2SubrangeStraddling
}

// streamV2LayoutTiles reports whether a layout, sorted by keyBegin, covers the
// shard range exactly.
func streamV2LayoutTiles(layout []streamV2Range, shard streamV2Range) bool {
	if len(layout) == 0 || layout[0].keyBegin != shard.keyBegin {
		return false
	}
	for i := 1; i < len(layout); i++ {
		// The comparison runs in uint64 so that a subrange ending at the top of
		// the key space cannot wrap to zero and fake a continuation.
		if uint64(layout[i].keyBegin) != uint64(layout[i-1].keyEnd)+1 {
			return false
		}
	}
	return layout[len(layout)-1].keyEnd == shard.keyEnd
}

// streamV2RouteHash reports the index of the layout subrange that covers a key
// hash, or -1 when none does. Under a layout that tiles the shard range, -1
// means the hash lies outside the shard entirely — a disagreement with the
// runtime's routing that the caller must refuse.
func streamV2RouteHash(layout []streamV2Range, hash uint32) int {
	for i, r := range layout {
		if r.contains(hash) {
			return i
		}
	}
	return -1
}
