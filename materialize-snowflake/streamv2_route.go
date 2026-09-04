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
// both ends, into streamV2ChannelsPerShard equal key ranges. This is the layout a
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

// contains reports whether the key range covers a key hash. Bounds are inclusive
// on both ends, as RangeSpec bounds are.
func (r streamV2Range) contains(hash uint32) bool {
	return r.keyBegin <= hash && hash <= r.keyEnd
}

// streamV2KeyRangeClass classifies a checkpoint item's channel key range against the
// range of the shard reading it.
type streamV2KeyRangeClass int

const (
	// streamV2KeyRangeTarget is a key range of the shard's own target layout.
	streamV2KeyRangeTarget streamV2KeyRangeClass = iota
	// streamV2KeyRangeInherited lies within the shard's range but is not a target
	// key range: it belonged to a shard this topology replaced, and this shard
	// continues it until a rebalance drops it.
	streamV2KeyRangeInherited
	// streamV2KeyRangeSibling lies entirely outside the shard's range. It belongs
	// to a live sibling and is none of this shard's business.
	streamV2KeyRangeSibling
	// streamV2KeyRangeStraddling crosses the shard's boundary. Splits are
	// midpoint-only and channels subdivide evenly, so no channel can straddle a
	// boundary; this classification is a rejection.
	streamV2KeyRangeStraddling
)

// classifyKeyRange places one channel key range relative to a shard range and the
// shard's target layout.
func classifyKeyRange(item streamV2Range, shard streamV2Range, targets []streamV2Range) streamV2KeyRangeClass {
	if slices.Contains(targets, item) {
		return streamV2KeyRangeTarget
	}
	if shard.keyBegin <= item.keyBegin && item.keyEnd <= shard.keyEnd {
		return streamV2KeyRangeInherited
	}
	if item.keyEnd < shard.keyBegin || item.keyBegin > shard.keyEnd {
		return streamV2KeyRangeSibling
	}
	return streamV2KeyRangeStraddling
}

// streamV2LayoutCovers reports whether a layout, sorted by keyBegin, covers the
// shard range with no gaps or overlaps.
func streamV2LayoutCovers(layout []streamV2Range, shard streamV2Range) bool {
	if len(layout) == 0 || layout[0].keyBegin != shard.keyBegin {
		return false
	}
	for i := 1; i < len(layout); i++ {
		// The comparison runs in uint64 so that a key range ending at the top of
		// the key space cannot wrap to zero and fake a continuation.
		if uint64(layout[i].keyBegin) != uint64(layout[i-1].keyEnd)+1 {
			return false
		}
	}
	return layout[len(layout)-1].keyEnd == shard.keyEnd
}

// streamV2RouteHash reports the index of the layout key range that covers a key
// hash, or -1 when none does. Under a layout that covers the shard range, -1
// means the hash lies outside the shard entirely — a disagreement with the
// runtime's routing that the caller must reject.
func streamV2RouteHash(layout []streamV2Range, hash uint32) int {
	for i, r := range layout {
		if r.contains(hash) {
			return i
		}
	}
	return -1
}
