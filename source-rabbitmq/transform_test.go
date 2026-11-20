package main

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestMakeDocumentBodies(t *testing.T) {
	const meta = `"_meta":{"queue":"orders","id":"m1","exchange":"","routingKey":"orders","messageId":"m1","redelivered":false,"priority":0,"deliveryMode":0}`
	for _, tc := range []struct {
		name string
		body string
		want string
	}{
		{"object", `{"id":1,"canary":"o1"}`, `{` + meta + `,"canary":"o1","id":1}`},
		{"text", "plain text o3", `{` + meta + `,"body":"plain text o3"}`},
		{"empty", "", `{` + meta + `,"body":""}`},
		{"unicode", "こんにちは", `{` + meta + `,"body":"こんにちは"}`},
		{"binary", "\x00\xff\x80", `{` + meta + `,"body":"AP+A"}`},
		{"binary in object", "{\"text\":\"\xff\"}", `{` + meta + `,"body":"eyJ0ZXh0Ijoi/yJ9"}`},
		{"array", `[1,2]`, `{` + meta + `,"body":"[1,2]"}`},
		{"string", `"hello"`, `{` + meta + `,"body":"\"hello\""}`},
		{"number", `123`, `{` + meta + `,"body":"123"}`},
		{"boolean", `true`, `{` + meta + `,"body":"true"}`},
		{"null", `null`, `{` + meta + `,"body":"null"}`},
		{"large integer", `{"id":9007199254740993}`, `{` + meta + `,"id":9007199254740993}`},
		{"number literals", `{"nested":{"big":1234567890123456789},"price":10.10}`, `{` + meta + `,"nested":{"big":1234567890123456789},"price":10.10}`},
		{"whitespace", " \n{\"id\":1}\n\t", `{` + meta + `,"id":1}`},
		{"trailing text", `{"id":1} extra`, `{` + meta + `,"body":"{\"id\":1} extra"}`},
		{"two objects", `{"id":1}{}`, `{` + meta + `,"body":"{\"id\":1}{}"}`},
		{"metadata overwritten", `{"_meta":{"id":"spoofed"},"id":1}`, `{` + meta + `,"id":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := makeDocument("orders", amqp.Delivery{
				MessageId: "m1", RoutingKey: "orders", Body: []byte(tc.body),
			}, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(doc))
		})
	}
}

func TestMakeDocumentMetadata(t *testing.T) {
	doc, err := makeDocument("orders", amqp.Delivery{
		Body:        []byte(`{"id":1}`),
		MessageId:   "m1",
		Exchange:    "sales",
		RoutingKey:  "orders.created",
		Redelivered: true,
		Timestamp:   time.Date(2026, 11, 10, 8, 30, 0, 0, time.FixedZone("test", -8*60*60)),
		ContentType: "application/json", ContentEncoding: "utf-8", CorrelationId: "request-1",
		ReplyTo: "replies", Type: "order.created", AppId: "sales", UserId: "test",
		Priority: 255, DeliveryMode: amqp.Persistent, Expiration: "60000",
		Headers: amqp.Table{
			"trace": "abc", "attempt": int32(2), "binary": []byte{0x01, 0xff},
			"nested": amqp.Table{"values": []any{true, "x", int64(7)}},
		},
	}, nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1,"_meta":{
		"queue":"orders","id":"m1","messageId":"m1","exchange":"sales",
		"routingKey":"orders.created","redelivered":true,"priority":255,"deliveryMode":2,"timestamp":"2026-11-10T16:30:00Z",
		"contentType":"application/json","contentEncoding":"utf-8","correlationId":"request-1",
		"replyTo":"replies","type":"order.created","appId":"sales","userId":"test","expiration":"60000",
		"headers":{"trace":"abc","attempt":2,"binary":"Af8=","nested":{"values":[true,"x",7]}}
	}}`, string(doc))
}

func TestMakeDocumentTimestampRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		time time.Time
		want string
	}{
		{"milliseconds", time.Unix(1704067200000, 0), ""},
		{"before year one", time.Unix(-70000000000, 0), ""},
		{"year zero", time.Date(0, 12, 31, 23, 59, 59, 0, time.UTC), ""},
		{"year one", time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC), "0001-01-01T00:00:01Z"},
		{"year 9999", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC), "9999-12-31T23:59:59Z"},
		{"UTC overflow", time.Date(9999, 12, 31, 23, 59, 59, 0, time.FixedZone("test", -3600)), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := makeDocument("orders", amqp.Delivery{Timestamp: tc.time}, nil)
			require.NoError(t, err)
			var got struct {
				Meta map[string]any `json:"_meta"`
			}
			require.NoError(t, json.Unmarshal(doc, &got))
			if tc.want == "" {
				require.NotContains(t, got.Meta, "timestamp")
			} else {
				require.Equal(t, tc.want, got.Meta["timestamp"])
				_, err := time.Parse(time.RFC3339Nano, got.Meta["timestamp"].(string))
				require.NoError(t, err)
			}
		})
	}
}

func TestMakeDocumentNormalizesHeaders(t *testing.T) {
	invalidTime := time.Unix(1704067200000, 0)
	validTime := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	nested := []any{amqp.Table{"value": math.NaN(), "binary": "\x01\xff", "text": "hello 🌈"}, "\x01\xff", "hello 🌈"}
	headers := amqp.Table{
		"binary": "\x01\xff", "text": "hello 🌈",
		"timestamp": invalidTime, "validTime": validTime,
		"nan32": float32(math.NaN()), "nan64": math.NaN(),
		"positive32": float32(math.Inf(1)), "positive64": math.Inf(1),
		"negative32": float32(math.Inf(-1)), "negative64": math.Inf(-1),
		"nested": amqp.Table{"array": nested}, "finite": float32(1.5),
	}
	for _, isStream := range []bool{false, true} {
		var offset *int64
		if isStream {
			offset = new(int64)
		}
		doc, err := makeDocument("events", amqp.Delivery{Headers: headers}, offset)
		require.NoError(t, err)
		var got struct {
			Meta struct {
				Headers json.RawMessage `json:"headers"`
			} `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(doc, &got))
		require.JSONEq(t, `{
			"timestamp":1704067200000,"validTime":"2026-11-11T12:00:00Z",
			"nan32":"NaN","nan64":"NaN",
			"positive32":"Infinity","positive64":"Infinity",
			"negative32":"-Infinity","negative64":"-Infinity",
			"binary":"~base64:Af8=","text":"hello 🌈",
			"nested":{"array":[{"value":"NaN","binary":"~base64:Af8=","text":"hello 🌈"},"~base64:Af8=","hello 🌈"]},"finite":1.5
		}`, string(got.Meta.Headers))
	}
	require.Equal(t, invalidTime, headers["timestamp"])
	require.True(t, math.IsNaN(headers["nan64"].(float64)))
	require.True(t, math.IsNaN(nested[0].(amqp.Table)["value"].(float64)), "normalization must not mutate nested headers")
}

func TestMakeDocumentStream(t *testing.T) {
	for _, offset := range []int64{0, 1, 9007199254740993} {
		doc, err := makeDocument("events", amqp.Delivery{
			Body: []byte(`{"id":101}`), MessageId: "m101", RoutingKey: "events",
			Headers: amqp.Table{"x-stream-offset": offset},
		}, &offset)
		require.NoError(t, err)
		var got struct {
			Meta documentMetadata `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(doc, &got))
		require.Equal(t, "m101", got.Meta.MessageID)
		require.Equal(t, &offset, got.Meta.Offset)
		require.Equal(t, "events:"+strconv.FormatInt(offset, 10), got.Meta.ID)
	}
}

func TestMakeDocumentInvalidUTF8(t *testing.T) {
	values := []string{"\xff\x01", "\xfe\x01", "�\x01", "~base64:/wE=", "~base64:~base64:/wE=", "hello 🌈"}
	seenIDs := make(map[string]bool)
	for _, value := range values {
		delivery := amqp.Delivery{
			MessageId: value, Exchange: value, RoutingKey: value, ContentType: value,
			ContentEncoding: value, CorrelationId: value, ReplyTo: value,
			Type: value, AppId: value, UserId: value, Expiration: value,
			Headers: amqp.Table{
				"k\xff": "\xff\x01", "k\xfe": "\xfe\x01", "k�": "�\x01",
				"~base64:a/8=": "~base64:/wE=",
				"nested":       amqp.Table{"k\xff": []any{value}},
			},
		}
		doc, err := makeDocument(value, delivery, nil)
		require.NoError(t, err)
		var got struct {
			Meta map[string]any `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(doc, &got))
		id := got.Meta["id"].(string)
		require.NotContains(t, seenIDs, id, "distinct raw message IDs must remain distinct collection keys")
		seenIDs[id] = true
		for _, property := range []string{"queue", "id", "messageId", "exchange", "routingKey", "contentType", "contentEncoding", "correlationId", "replyTo", "type", "appId", "userId", "expiration"} {
			require.Equal(t, value, decodeMetadataString(t, got.Meta[property].(string)), property)
		}
		headers := got.Meta["headers"].(map[string]any)
		require.Len(t, headers, 5, "non-UTF-8 header names must not collapse into duplicate JSON keys")
		for name, header := range headers {
			rawName := decodeMetadataString(t, name)
			if rawName == "nested" {
				nested := header.(map[string]any)
				require.Len(t, nested, 1)
				for name, array := range nested {
					require.Equal(t, "k\xff", decodeMetadataString(t, name))
					require.Equal(t, value, decodeMetadataString(t, array.([]any)[0].(string)))
				}
			} else {
				require.Equal(t, delivery.Headers[rawName], decodeMetadataString(t, header.(string)))
			}
		}
		require.Contains(t, delivery.Headers, "k\xff", "normalization must not mutate the original delivery")
		require.Equal(t, "\xff\x01", delivery.Headers["k\xff"])
	}
}

func decodeMetadataString(t *testing.T, value string) string {
	t.Helper()
	if strings.HasPrefix(value, "~base64:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "~base64:"))
		require.NoError(t, err)
		return string(decoded)
	}
	return value
}

func TestMakeDocumentMissingMessageID(t *testing.T) {
	var ids []string
	for range 2 {
		doc, err := makeDocument("orders", amqp.Delivery{}, nil)
		require.NoError(t, err)
		var got struct {
			Meta map[string]any `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(doc, &got))
		id := got.Meta["id"].(string)
		parsed, err := uuid.Parse(id)
		require.NoError(t, err)
		require.Equal(t, uuid.Version(4), parsed.Version())
		ids = append(ids, id)
		require.NotContains(t, got.Meta, "messageId")
		require.NotContains(t, got.Meta, "timestamp")
		require.NotContains(t, got.Meta, "headers")
		require.NotContains(t, got.Meta, "offset")
		for _, property := range []string{"contentType", "contentEncoding", "correlationId", "replyTo", "type", "appId", "userId", "expiration"} {
			require.NotContains(t, got.Meta, property)
		}
		require.Equal(t, float64(0), got.Meta["priority"])
		require.Equal(t, float64(0), got.Meta["deliveryMode"])
		require.Equal(t, false, got.Meta["redelivered"])
	}
	require.NotEqual(t, ids[0], ids[1])
}

func TestStreamOffset(t *testing.T) {
	offset, err := streamOffset(nil)
	require.NoError(t, err)
	require.Nil(t, offset)
	for _, value := range []int64{0, 42, math.MaxInt64 - 1} {
		offset, err := streamOffset(amqp.Table{"x-stream-offset": value})
		require.NoError(t, err)
		require.Equal(t, &value, offset)
	}
	for _, value := range []any{nil, "0", float64(0), int64(-1), int64(math.MaxInt64)} {
		_, err := streamOffset(amqp.Table{"x-stream-offset": value})
		require.ErrorContains(t, err, "invalid x-stream-offset")
	}
}

func mustMarshal(t *testing.T, value any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(value)
	require.NoError(t, err)
	return data
}
