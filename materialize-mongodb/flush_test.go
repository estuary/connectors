package connector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestFlushFeatureFlags(t *testing.T) {
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		flags string
		want  map[int]time.Time
	}{
		{flags: "", want: map[int]time.Time{0: boundary}},
		{flags: "no_truncate_after_backfill", want: map[int]time.Time{}},
	} {
		t.Run(tc.flags, func(t *testing.T) {
			var flags = common.ResolveFlags(tc.flags, featureFlagDefaults, common.CreatedAt{})
			var d = &transactor{
				bindings:              []*binding{{path: []string{"db", "standard"}}, {path: []string{"db", "delta"}, deltaUpdates: true}},
				truncateAfterBackfill: flags["truncate_after_backfill"],
			}
			require.NoError(t, d.Flush(t.Context(), map[int]time.Time{0: boundary, 1: boundary}))
			require.Equal(t, tc.want, d.truncations)
		})
	}
}

func TestStoreDocumentPublishedAt(t *testing.T) {
	// The clock of this UUID is 1970-01-01T03:00:09Z.
	var raw = json.RawMessage(`{"id":1,"_meta":{"uuid":"3e2bc280-1deb-11b2-8000-071353030311"}}`)
	var want = primitive.NewDateTimeFromTime(time.Date(1970, 1, 1, 3, 0, 9, 0, time.UTC))

	t.Run("standard updates", func(t *testing.T) {
		doc, loadedJSON := storeAndReload(t, raw, "00", false, nil, nil)
		require.Equal(t, want, doc[publishedAtField])
		// The field is the connector's own, so a loaded document omits it.
		require.JSONEq(t, string(raw), string(loadedJSON))
	})

	t.Run("delta updates", func(t *testing.T) {
		doc, _ := storeAndReload(t, raw, "00", true, nil, nil)
		require.NotContains(t, doc, publishedAtField)
	})

	t.Run("no uuid", func(t *testing.T) {
		doc, _ := storeAndReload(t, json.RawMessage(`{"id":1}`), "00", false, nil, nil)
		require.NotContains(t, doc, publishedAtField)
	})

	t.Run("invalid uuid", func(t *testing.T) {
		_, err := storeDocument(json.RawMessage(`{"id":1,"_meta":{"uuid":"nope"}}`), "00", false, nil, nil)
		require.ErrorContains(t, err, "/_meta/uuid")
	})

	t.Run("field collision", func(t *testing.T) {
		_, err := storeDocument(json.RawMessage(`{"id":1,"_flow_published_at":1,"_meta":{"uuid":"3e2bc280-1deb-11b2-8000-071353030311"}}`), "00", false, nil, nil)
		require.ErrorContains(t, err, publishedAtField)
	})
}
