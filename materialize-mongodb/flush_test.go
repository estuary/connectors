package connector

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
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

// benchDoc is a document of typical shape for the storeDocument benchmarks
// below, so that the cost of setPublishedAt can be read against the JSON
// decode it follows.
var benchDoc = json.RawMessage(`{"id":12345,"_meta":{"op":"u","uuid":"3e2bc280-1deb-11b2-8000-071353030311"},"name":"some customer name","email":"someone@example.com","address":{"street":"1 Main St","city":"Springfield","zip":"12345"},"tags":["a","b","c"],"balance":123.45,"created_at":"2026-01-01T00:00:00Z","updated_at":"2026-02-01T00:00:00Z","notes":"lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor"}`)

// BenchmarkStoreDocumentStandard measures a standard-updates store, which
// sets the publication time.
func BenchmarkStoreDocumentStandard(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := storeDocument(benchDoc, "00", false, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoreDocumentDelta measures a delta-updates store of the same
// document, which omits the publication time.
func BenchmarkStoreDocumentDelta(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := storeDocument(benchDoc, "00", true, nil, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSetPublishedAt measures setPublishedAt alone on an already
// decoded document.
func BenchmarkSetPublishedAt(b *testing.B) {
	var doc bson.M
	require.NoError(b, json.Unmarshal(benchDoc, &doc))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		delete(doc, publishedAtField)
		if err := setPublishedAt(doc); err != nil {
			b.Fatal(err)
		}
	}
}
