package sql

import (
	"encoding/json"
	"testing"
	"time"

	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/message"
)

func metaCol(field, format string) *Column {
	p := Projection{Projection: pf.Projection{
		Field: field,
		Ptr:   "/_meta/uuid",
		Inference: pf.Inference{
			Types:   []string{"string"},
			String_: &pf.Inference_String{Format: format},
		},
	}}
	return &Column{Projection: p}
}

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

func TestSynthesizeMetaUUID(t *testing.T) {
	t.Run("timestamp mode synthesizes clock from flow_published_at", func(t *testing.T) {
		ts := "2024-03-04T05:06:07.891011Z"
		want, err := time.Parse(time.RFC3339Nano, ts)
		require.NoError(t, err)

		out, err := SynthesizeMetaUUID(
			json.RawMessage(`{"id":1,"_meta":{"uuid":"`+ts+`"}}`),
			metaCol("flow_published_at", "date-time"),
		)
		require.NoError(t, err)
		// Clock resolves to 100ns; compare within a microsecond.
		require.WithinDuration(t, want, clockTime(t, out), time.Microsecond)
	})

	t.Run("timestamp mode with null uses 1970 sentinel", func(t *testing.T) {
		out, err := SynthesizeMetaUUID(
			json.RawMessage(`{"id":1,"_meta":{"uuid":null}}`),
			metaCol("flow_published_at", "date-time"),
		)
		require.NoError(t, err)
		require.WithinDuration(t, time.Unix(0, 0).UTC(), clockTime(t, out), time.Microsecond)
	})

	t.Run("raw uuid mode preserves an existing uuid", func(t *testing.T) {
		raw := message.BuildUUID(message.NewProducerID(), message.NewClock(time.Now()), message.Flag_OUTSIDE_TXN).String()
		in := json.RawMessage(`{"id":1,"_meta":{"uuid":"` + raw + `"}}`)
		out, err := SynthesizeMetaUUID(in, metaCol("_meta/uuid", "uuid"))
		require.NoError(t, err)
		require.JSONEq(t, string(in), string(out))
	})

	t.Run("raw uuid mode with null uses 1970 sentinel", func(t *testing.T) {
		out, err := SynthesizeMetaUUID(
			json.RawMessage(`{"id":1,"_meta":{"uuid":null}}`),
			metaCol("_meta/uuid", "uuid"),
		)
		require.NoError(t, err)
		require.WithinDuration(t, time.Unix(0, 0).UTC(), clockTime(t, out), time.Microsecond)
	})

	t.Run("missing _meta object uses sentinel", func(t *testing.T) {
		out, err := SynthesizeMetaUUID(
			json.RawMessage(`{"id":1}`),
			metaCol("flow_published_at", "date-time"),
		)
		require.NoError(t, err)
		require.WithinDuration(t, time.Unix(0, 0).UTC(), clockTime(t, out), time.Microsecond)
	})
}
