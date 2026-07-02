package sql

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBase64Decoder(t *testing.T) {
	raw := []byte{0x00, 0x01, 0xff, 0xfe, 0x7f}
	encoded := base64.StdEncoding.EncodeToString(raw)

	for _, tc := range []struct {
		name  string
		input any
	}{
		{"string", encoded},
		{"[]byte", []byte(encoded)},
		{"json.RawMessage", json.RawMessage(`"` + encoded + `"`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Base64Decoder(tc.input)
			require.NoError(t, err)
			require.Equal(t, raw, got)
		})
	}

	t.Run("nil", func(t *testing.T) {
		got, err := Base64Decoder(nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})
}

func TestBase64ToHex(t *testing.T) {
	raw := []byte{0x00, 0x01, 0xff, 0xfe, 0x7f}
	encoded := base64.StdEncoding.EncodeToString(raw)
	wantHex := "0001fffe7f"

	for _, tc := range []struct {
		name  string
		input any
	}{
		{"string", encoded},
		{"[]byte", []byte(encoded)},
		{"json.RawMessage", json.RawMessage(`"` + encoded + `"`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Base64ToHex(tc.input)
			require.NoError(t, err)
			require.Equal(t, wantHex, got)
		})
	}

	t.Run("nil", func(t *testing.T) {
		got, err := Base64ToHex(nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})
}

func TestStdStrToInt(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  int64
	}{
		{
			input: "11.0",
			want:  11,
		},
		{
			input: "11.0000000",
			want:  11,
		},
		{
			input: "1",
			want:  1,
		},
		{
			input: "-3",
			want:  -3,
		},
		{
			input: "-14.0",
			want:  -14,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := StrToInt(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.(*big.Int).Int64())
		})
	}
}

func TestClampDatetime(t *testing.T) {
	for _, tt := range []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: "0000-01-01T00:00:00Z",
			want:  "0001-01-01T00:00:00Z",
		},
		{
			input: "0000-12-31T23:59:59Z",
			want:  "0001-01-01T00:00:00Z",
		},
		{
			input: "0001-01-01T00:00:00Z",
			want:  "0001-01-01T00:00:00Z",
		},
		{
			input: "0001-01-01T00:00:01Z",
			want:  "0001-01-01T00:00:01Z",
		},
		{
			input: "2023-08-29T16:17:18Z",
			want:  "2023-08-29T16:17:18Z",
		},
		// Already-valid RFC3339 values must pass through byte-identical.
		{
			input: "2023-08-29T16:17:18.123456789+05:00",
			want:  "2023-08-29T16:17:18.123456789+05:00",
		},
		{
			input: "2023-08-29T16:17:18z",
			want:  "2023-08-29T16:17:18Z",
		},
		// Near-RFC3339 variants (see issue #4725): space separator instead of
		// "T", and/or a missing offset (assumed UTC). All normalize to
		// canonical RFC3339.
		{
			input: "2025-11-29 01:05:28+00:00",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "2026-06-23 00:15:23.918411+00:00",
			want:  "2026-06-23T00:15:23.918411Z",
		},
		{
			input: "2025-11-29 01:05:28+05:30",
			want:  "2025-11-29T01:05:28+05:30",
		},
		{
			input: "2025-11-29 01:05:28Z",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "2025-11-29 01:05:28z",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "2025-11-29 01:05:28",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "2025-11-29T01:05:28",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "0000-01-01 00:00:00Z",
			want:  "0001-01-01T00:00:00Z",
		},
		{
			input:   "not a timestamp",
			wantErr: true,
		},
		{
			input:   "2025-11-29",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ClampDatetime(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseRFC3339Nano(t *testing.T) {
	for _, tt := range []struct {
		input         string
		wantCanonical string
		wantUnix      int64
		wantErr       bool
	}{
		{
			input:         "2025-11-29T01:05:28Z",
			wantCanonical: "2025-11-29T01:05:28Z",
			wantUnix:      1764378328,
		},
		// All variants of the same instant normalize to the same canonical
		// string and parsed time.
		{
			input:         "2025-11-29 01:05:28+00:00",
			wantCanonical: "2025-11-29T01:05:28Z",
			wantUnix:      1764378328,
		},
		{
			input:         "2025-11-29 01:05:28Z",
			wantCanonical: "2025-11-29T01:05:28Z",
			wantUnix:      1764378328,
		},
		{
			input:         "2025-11-29 01:05:28",
			wantCanonical: "2025-11-29T01:05:28Z",
			wantUnix:      1764378328,
		},
		{
			input:         "2025-11-29T01:05:28",
			wantCanonical: "2025-11-29T01:05:28Z",
			wantUnix:      1764378328,
		},
		{
			input:         "2025-11-29 06:35:28+05:30",
			wantCanonical: "2025-11-29T06:35:28+05:30",
			wantUnix:      1764378328,
		},
		{
			input:         "2026-06-23 00:15:23.918411+00:00",
			wantCanonical: "2026-06-23T00:15:23.918411Z",
			wantUnix:      1782173723,
		},
		{
			input:   "not a timestamp",
			wantErr: true,
		},
		{
			input:   "2025-11-29",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			parsed, canonical, err := ParseRFC3339Nano(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantCanonical, canonical)
			require.Equal(t, tt.wantUnix, parsed.Unix())
		})
	}
}

func TestNormalizeDatetimeString(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
	}{
		{
			input: "2023-08-29T16:17:18Z",
			want:  "2023-08-29T16:17:18Z",
		},
		{
			input: "2023-08-29T16:17:18.123456789+05:00",
			want:  "2023-08-29T16:17:18.123456789+05:00",
		},
		{
			input: "2023-08-29T16:17:18z",
			want:  "2023-08-29T16:17:18Z",
		},
		{
			input: "2025-11-29 01:05:28+00:00",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			input: "2026-06-23 00:15:23.918411+00:00",
			want:  "2026-06-23T00:15:23.918411Z",
		},
		{
			input: "2025-11-29 01:05:28",
			want:  "2025-11-29T01:05:28Z",
		},
		// Unparseable values pass through rather than erroring, since this
		// converter has always been a pass-through and some endpoints accept
		// formats we don't recognize.
		{
			input: "not a timestamp",
			want:  "not a timestamp",
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := NormalizeDatetimeString(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestClampDate(t *testing.T) {
	for _, tt := range []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: "0000-01-01",
			want:  "0001-01-01",
		},
		{
			input: "0000-12-31",
			want:  "0001-01-01",
		},
		{
			input: "0001-01-01",
			want:  "0001-01-01",
		},
		{
			input: "2023-08-29",
			want:  "2023-08-29",
		},
		{
			input:   "not a date",
			wantErr: true,
		},
		{
			input:   "2025-11-29 01:05:28+00:00",
			wantErr: true,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ClampDate(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestToJsonString(t *testing.T) {
	for _, tt := range []struct {
		input any
		want  any
	}{
		{
			input: []byte("{\"hello\": \"world\"}"),
			want:  "{\"hello\": \"world\"}",
		},
		{
			input: nil,
			want:  nil,
		},
		{
			input: true,
			want:  "true",
		},
		{
			input: 12.34,
			want:  "12.34",
		},
		{
			input: json.RawMessage([]byte("{\"hello\": \"world\"}")),
			want:  "{\"hello\": \"world\"}",
		},
	} {
		got, err := ToJsonString(tt.input)
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}
}
