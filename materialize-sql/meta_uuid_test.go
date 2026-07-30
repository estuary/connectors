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

func micros(t time.Time) *int64 { m := t.UnixMicro(); return &m }

func TestSpliceMeta(t *testing.T) {
	published := time.Date(2024, 3, 4, 5, 6, 7, 891011000, time.UTC)

	t.Run("synthesizes uuid from clock micros", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1,"canary":"a"}`), nil, micros(published), true)
		require.NoError(t, err)
		require.WithinDuration(t, published, clockTime(t, out), time.Microsecond)
	})

	t.Run("merges synthesized uuid with materialized _meta fields", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1}`), []byte(`{"op":"u"}`), micros(published), true)
		require.NoError(t, err)
		require.WithinDuration(t, published, clockTime(t, out), time.Microsecond)
		// _meta/op must survive: it drives delete reduction.
		require.JSONEq(t, `{"id":1,"_meta":{"uuid":"`+
			message.BuildUUID(zeroProducer, message.NewClock(published), message.Flag_OUTSIDE_TXN).String()+
			`","op":"u"}}`, string(out))
	})

	t.Run("null clock uses the 1970 sentinel", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1}`), nil, nil, true)
		require.NoError(t, err)
		require.WithinDuration(t, time.Unix(0, 0).UTC(), clockTime(t, out), time.Microsecond)
	})

	t.Run("materialized raw uuid is spliced unchanged", func(t *testing.T) {
		raw := message.BuildUUID(message.NewProducerID(), message.NewClock(published), message.Flag_OUTSIDE_TXN).String()
		out, err := SpliceMeta([]byte(`{"id":1}`), []byte(`{"uuid":"`+raw+`","op":"d"}`), nil, false)
		require.NoError(t, err)
		require.JSONEq(t, `{"id":1,"_meta":{"uuid":"`+raw+`","op":"d"}}`, string(out))
	})

	t.Run("no clock and no _meta leaves the document alone", func(t *testing.T) {
		in := []byte(`{"id":1}`)
		out, err := SpliceMeta(in, nil, nil, false)
		require.NoError(t, err)
		require.Equal(t, in, out)
	})

	t.Run("_meta fields without a uuid", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1}`), []byte(`{"op":"u"}`), nil, false)
		require.NoError(t, err)
		require.JSONEq(t, `{"id":1,"_meta":{"op":"u"}}`, string(out))
	})

	t.Run("empty document object", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{}`), nil, micros(published), true)
		require.NoError(t, err)
		require.WithinDuration(t, published, clockTime(t, out), time.Microsecond)
		require.NotContains(t, string(out), "{,")
	})

	t.Run("empty _meta object", func(t *testing.T) {
		out, err := SpliceMeta([]byte(`{"id":1}`), []byte(`{}`), micros(published), true)
		require.NoError(t, err)
		require.WithinDuration(t, published, clockTime(t, out), time.Microsecond)
		require.NotContains(t, string(out), `",}`)
	})

	t.Run("rejects a non-object document", func(t *testing.T) {
		_, err := SpliceMeta([]byte(`[1]`), nil, nil, true)
		require.ErrorContains(t, err, "not a JSON object")
	})

	t.Run("rejects a non-object _meta", func(t *testing.T) {
		_, err := SpliceMeta([]byte(`{"id":1}`), []byte(`"nope"`), micros(published), true)
		require.ErrorContains(t, err, "_meta is not a JSON object")
	})
}
