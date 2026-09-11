package connector

import (
	"context"
	"math"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
)

// packKey packs a document key the way the runtime does, so a test row routes
// through the same hash the runtime would route its document by.
func packKey(key tuple.TupleElement) []byte {
	return tuple.Tuple{key}.Pack()
}

// testWriteRow stores one converted row under the key in its first value, which is
// the KEY column of every table these tests store into.
func testWriteRow(ctx context.Context, m *streamV2Manager, binding int, converted []any) error {
	return m.writeRow(ctx, binding, packKey(converted[0]), converted)
}

// soleItem returns the one non-nil checkpoint item a flush produced for a binding,
// for tests pinned to a single-channel layout.
func soleCheckpointItem(t *testing.T, entries map[int]streamV2Checkpoint, binding int) *streamV2ChannelCheckpointItem {
	t.Helper()
	var sv2Checkpoint []*streamV2ChannelCheckpointItem
	for _, sv2ChannelCheckpointItem := range entries[binding] {
		if sv2ChannelCheckpointItem != nil {
			sv2Checkpoint = append(sv2Checkpoint, sv2ChannelCheckpointItem)
		}
	}
	if len(sv2Checkpoint) != 1 {
		t.Fatalf("expected exactly one item for binding %d, got %d", binding, len(sv2Checkpoint))
	}
	return sv2Checkpoint[0]
}

// singleChannelLayout pins streamV2ChannelsPerShard to one for a test whose
// assertions follow a single channel's counter, committed index, or token. The write
// path's behavior per channel is identical at any depth; these tests are about that
// behavior, not about routing, which has coverage of its own.
func singleChannelLayout(t *testing.T) {
	var restore = streamV2ChannelsPerShard
	t.Cleanup(func() { streamV2ChannelsPerShard = restore })
	streamV2ChannelsPerShard = 1
}

// fullKeyRange is the key range of a single-channel layout on an unsplit task.
var fullKeyRange = streamV2Range{keyEnd: math.MaxUint32}
