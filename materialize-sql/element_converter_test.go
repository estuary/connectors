package sql

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		input string
		want  string
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
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ClampDatetime(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestClampDate(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  string
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
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ClampDate(tt.input)
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

func TestCheckedInt64(t *testing.T) {
	for _, tt := range []struct {
		input   any
		want    any
		wantErr bool
	}{
		{
			input:   nil,
			want:    nil,
			wantErr: false,
		},
		{
			input:   int64(12),
			want:    int64(12),
			wantErr: false,
		},
		{
			input:   "1234",
			want:    int64(1234),
			wantErr: false,
		},
		{
			input:   uint64(math.MaxUint64),
			want:    nil,
			wantErr: true,
		},
		{
			input:   float64(math.MaxFloat64),
			want:    nil,
			wantErr: true,
		},
		{
			input:   fmt.Sprintf("%.0f", math.MaxFloat64),
			want:    nil,
			wantErr: true,
		},
		{
			input:   true,
			want:    nil,
			wantErr: true,
		},
		{
			input:   "notanintegerstring",
			want:    nil,
			wantErr: true,
		},
	} {
		got, err := CheckedInt64(tt.input)
		if tt.wantErr {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, tt.want, got)
		if tt.want != nil {
			require.IsType(t, int64(0), tt.want)
		}
	}
}
