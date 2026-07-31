package sql

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
)

// clockTime extracts the Gazette clock time from the /_meta/uuid of a document.
func clockTime(t *testing.T, doc json.RawMessage) time.Time {
	t.Helper()
	var parsed struct {
		Meta struct {
			UUID string `json:"uuid"`
		} `json:"_meta"`
	}
	require.NoError(t, json.Unmarshal(doc, &parsed))
	u, err := uuid.Parse(parsed.Meta.UUID)
	require.NoError(t, err)
	return message.GetClock(u).AsTime().UTC()
}

func micros(t time.Time) *int64 { m := t.UnixMicro(); return &m }

func TestSpliceMeta(t *testing.T) {
	published := time.Date(2024, 3, 4, 5, 6, 7, 891011000, time.UTC)

	t.Run("synthesizes a uuid whose clock is the publication time", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1,"canary":"a"}`), micros(published))
		require.NoError(t, err)
		require.WithinDuration(t, published, clockTime(t, out), time.Microsecond)
		require.JSONEq(t, `{"id":1,"canary":"a","_meta":{"uuid":"`+
			message.BuildUUID(zeroProducer, message.NewClock(published), message.Flag_OUTSIDE_TXN).String()+
			`"}}`, string(out))
	})

	// A NULL clock means the row predates the column, which is distinct from the
	// binding having no clock column at all: connectors skip SpliceMeta entirely
	// in that case rather than fabricating an epoch clock, see hasMetaClock.
	t.Run("a null clock uses the 1970 sentinel", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1}`), nil)
		require.NoError(t, err)
		require.WithinDuration(t, time.Unix(0, 0).UTC(), clockTime(t, out), time.Microsecond)
	})

	t.Run("an empty document object", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{}`), micros(published))
		require.NoError(t, err)
		require.WithinDuration(t, published, clockTime(t, out), time.Microsecond)
		require.NotContains(t, string(out), "{,")
	})

	t.Run("rejects a document that is not a JSON object", func(t *testing.T) {
		_, err := SpliceMeta([]byte(`[1]`), nil)
		require.ErrorContains(t, err, "not a JSON object")
	})

	t.Run("microsecond clocks round-trip across the supported range", func(t *testing.T) {
		for _, ts := range []time.Time{
			time.Unix(0, 0).UTC(),
			time.Date(2024, 3, 4, 5, 6, 7, 0, time.UTC),
			published,
			time.Date(2099, 12, 31, 23, 59, 59, 999999000, time.UTC),
		} {
			out, err := SpliceMeta([]byte(`{"id":1}`), micros(ts))
			require.NoError(t, err)
			require.WithinDuration(t, ts, clockTime(t, out), time.Microsecond)
		}
	})
}
