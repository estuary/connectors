package connector

import (
	"context"
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
func soleItem(t *testing.T, entries map[int]map[string]*streamV2Item, binding int) *streamV2Item {
	t.Helper()
	var items []*streamV2Item
	for _, item := range entries[binding] {
		if item != nil {
			items = append(items, item)
		}
	}
	if len(items) != 1 {
		t.Fatalf("expected exactly one item for binding %d, got %d", binding, len(items))
	}
	return items[0]
}

// singleChannelLayout pins streamV2ChannelsPerShard to one for a test whose
// assertions follow a single channel's counter, skip threshold, or token. The write
// path's behavior per channel is identical at any depth; these tests are about that
// behavior, not about routing, which has coverage of its own.
func singleChannelLayout(t *testing.T) {
	var restore = streamV2ChannelsPerShard
	t.Cleanup(func() { streamV2ChannelsPerShard = restore })
	streamV2ChannelsPerShard = 1
}
