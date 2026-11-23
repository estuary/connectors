package connector

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"unicode"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/stretchr/testify/require"
)

func TestEncodeKey(t *testing.T) {
	for _, tt := range []struct {
		name string
		key  tuple.Tuple
		want string
	}{
		{"integer", tuple.Tuple{int64(1)}, "1"},
		{"compound", tuple.Tuple{"eu/west", int64(8)}, "eu~2Fwest:8"},
		{"escaping", tuple.Tuple{`~%:/\?#`, "~3A"}, "~7E~25~3A~2F~5C~3F~23:~7E3A"},
		{"timestamp", tuple.Tuple{"2026-11-16T10:00:00Z"}, "2026-11-16T10~3A00~3A00Z"},
		{"control characters", tuple.Tuple{"a\tb\r\n\x7f"}, "a~09b~0D~0A~7F"},
		// C1 controls must escape every UTF-8 byte, including within compound keys.
		{"unicode control characters", tuple.Tuple{"a\u0081b"}, "a~C2~81b"},
		{"unicode controls in compound", tuple.Tuple{"x\u0080\u009fy", int64(1)}, "x~C2~80~C2~9Fy:1"},
		{"trailing whitespace", tuple.Tuple{" a b \u3000", "x "}, " a b~20~E3~80~80:x~20"},
		{"unicode", tuple.Tuple{"日本", true, false}, "日本:true:false"},
		{"large integers", tuple.Tuple{int64(9007199254740993), uint64(math.MaxUint64)}, "9007199254740993:18446744073709551615"},
		{"negative", tuple.Tuple{int64(math.MinInt64), -1.25}, "-9223372036854775808:-1.25"},
		{"dot", tuple.Tuple{"."}, "~2E"},
		{"dot dot", tuple.Tuple{".."}, "~2E~2E"},
		{"dots in compound", tuple.Tuple{".", "..", "..."}, "~2E:~2E~2E:..."},
		{"dots in text", tuple.Tuple{"a.b"}, "a.b"},
		{"empty string", tuple.Tuple{""}, "~"},
		{"empty strings in compound", tuple.Tuple{"", "~"}, ":~7E"},
		// Null must not collide with string keys, including the sentinel text.
		{"null", tuple.Tuple{nil}, "~N"},
		{"null text", tuple.Tuple{"null"}, "null"},
		{"null sentinel text", tuple.Tuple{"~N"}, "~7EN"},
		{"null in compound", tuple.Tuple{nil, int64(7)}, "~N:7"},
		{"null text in compound", tuple.Tuple{"null", int64(7)}, "null:7"},
		{"null sentinel text in compound", tuple.Tuple{"~N", int64(7)}, "~7EN:7"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeKey(tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
	// The routing gateway decodes ids once more and rejects control characters
	// and trailing whitespace, so no encoded id may contain them.
	for _, key := range []tuple.Tuple{{"100%"}, {"%2F"}, {"a\x00b"}, {"a\u0081b"}, {"x\u009fy", int64(1)}, {"tab\t"}, {"end "}, {"x", "y\u00a0"}, {"日本%:/\\?#"}} {
		got, err := encodeKey(key)
		require.NoError(t, err)
		require.NotContains(t, got, "%")
		require.Equal(t, strings.TrimRightFunc(got, unicode.IsSpace), got)
		require.False(t, strings.ContainsFunc(got, unicode.IsControl), got)
	}
	for _, key := range []tuple.Tuple{nil, {math.NaN()}, {[]byte("invalid")}} {
		_, err := encodeKey(key)
		require.Error(t, err)
	}
}

func TestDocumentRoundTrip(t *testing.T) {
	for _, raw := range []string{
		`{"id":1,"status":"new","canary":"o1","note":"keep"}`,
		`{"id":9007199254740993,"nested":{"n":18446744073709551615,"d":1.234567890123456789},"values":[9223372036854775807]}`,
		`{"region":"eu/west","seq":8}`,
		`{"id":{"nested":"original"},"value":true}`,
		`{"id":null,"other_id":"untouched"}`,
	} {
		t.Run(raw, func(t *testing.T) {
			stored, deleted, err := storeDocument(json.RawMessage(raw), "encoded:key")
			require.NoError(t, err)
			require.False(t, deleted)
			var item map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(stored, &item))
			require.Equal(t, json.RawMessage(`"encoded:key"`), item["id"])
			for _, field := range systemFields {
				item[field] = json.RawMessage(`"system"`)
			}
			stored, err = json.Marshal(item)
			require.NoError(t, err)
			loaded, err := loadDocument(stored)
			require.NoError(t, err)
			var got, want map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(loaded, &got))
			require.NoError(t, json.Unmarshal([]byte(raw), &want))
			require.Equal(t, want, got) // Comparing raw numbers also detects rounding beyond 2^53.
		})
	}
}

func TestDeletionMetadata(t *testing.T) {
	for _, tt := range []struct {
		raw     string
		deleted bool
	}{
		{`{"id":1,"_meta":{"op":"d"}}`, true},
		{`{"id":1,"_meta":{"op":"\u0064"}}`, true},
		{`{"id":1,"_meta":{"op":"u"}}`, false},
		{`{"id":1,"_meta":{"OP":"d"}}`, false},
		{`{"id":1,"_meta":{"op":true}}`, false},
		{`{"id":1,"_meta":null}`, false},
		{`{"id":1,"op":"d"}`, false},
	} {
		_, deleted, err := storeDocument(json.RawMessage(tt.raw), "1")
		require.NoError(t, err)
		require.Equal(t, tt.deleted, deleted)
	}
	for _, raw := range []string{`null`, `[]`, `{`} {
		_, _, err := storeDocument(json.RawMessage(raw), "1")
		require.Error(t, err)
		_, err = loadDocument(json.RawMessage(raw))
		require.Error(t, err)
	}
}

func TestUnmarshalSessionState(t *testing.T) {
	for _, tt := range []struct{ raw, want string }{
		{`{}`, ""},
		{`null`, ""},
		{`{"bindingStateV1":{"orders.v1":{"containerRid":"orders-1","sessionTokens":{"0":"0:1"}},"events.v1":{"containerRid":"events-1","sessionTokens":{"1":"1:2"}}}}`, "0:1"},
		// Tokens of a deleted container would name LSNs its replacement has not reached.
		{`{"bindingStateV1":{"orders.v1":{"containerRid":"orders-0","sessionTokens":{"0":"0:1"}}}}`, ""},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			tr := transactor{bindings: []binding{
				{stateKey: "orders.v1", containerRID: "orders-1", sessionTokens: make(map[string]string)},
				{stateKey: "events.v2", containerRID: "events-1", sessionTokens: make(map[string]string)},
			}}
			require.NoError(t, tr.UnmarshalState(json.RawMessage(tt.raw)))
			require.Empty(t, tr.bindings[1].sessionTokens, "a new backfill must not inherit old session tokens")
			if tt.want == "" {
				require.Nil(t, tr.bindings[0].readOptions())
			} else {
				require.Equal(t, tt.want, *tr.bindings[0].readOptions().SessionToken)
			}
		})
	}
	var tr transactor
	require.Error(t, tr.UnmarshalState(json.RawMessage(`{"bindingStateV1":[]}`)))
}

func TestPartitionHeader(t *testing.T) {
	for _, tt := range []struct {
		name string
		b    binding
		key  tuple.Tuple
		want string
	}{
		{"string partition", binding{partitionKey: "/region", partitionKeyIndex: 0}, tuple.Tuple{"München", int64(1)}, `["M\u00fcnchen"]`},
		{"default id", binding{partitionKey: "/id"}, tuple.Tuple{"日本"}, `["\u65e5\u672c"]`},
		{"supplementary", binding{partitionKey: "/id"}, tuple.Tuple{"😀"}, `["\ud83d\ude00"]`},
		{"exact integer", binding{partitionKey: "/seq", partitionKeyIndex: 1}, tuple.Tuple{"x", uint64(math.MaxUint64)}, `[18446744073709551615]`},
		{"delete character", binding{partitionKey: "/region", partitionKeyIndex: 0}, tuple.Tuple{"a\x7fb", int64(1)}, `["a\u007fb"]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key, err := tt.b.itemKey(tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.want, key.partitionKeyJSON)
			require.False(t, strings.ContainsFunc(key.partitionKeyJSON, unicode.IsControl), "header values must not contain control characters")
		})
	}
}
