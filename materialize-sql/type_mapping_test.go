package sql

import (
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
		{
			input: "1_234",
			want:  1234,
		},
		{
			input: "123_",
			want:  123,
		},
		{
			input: "_12__34_",
			want:  1234,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := StdStrToInt()(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.(*big.Int).Int64())
		})
	}
}

func TestStdStrToFloat(t *testing.T) {
	for _, tt := range []struct {
		input string
		want  float64
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
		{
			input: "1_234.1",
			want:  1234.1,
		},
		{
			input: "12_34.2",
			want:  1234.2,
		},
		{
			input: "1.234_",
			want:  1.234,
		},
	} {
		t.Run(tt.input, func(t *testing.T) {
			got, err := StdStrToFloat(nil, nil, nil)(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got.(float64))
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
			got, err := ClampDatetime()(tt.input)
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
			got, err := ClampDate()(tt.input)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
