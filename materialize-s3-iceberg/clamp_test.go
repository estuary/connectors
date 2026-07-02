package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClampTimestamp(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    any
		wantErr bool
	}{
		{
			name:  "non-string passthrough",
			input: 42,
			want:  42,
		},
		{
			name:    "unparseable returns error",
			input:   "not-a-timestamp",
			wantErr: true,
		},
		{
			name:  "UTC year > 9999 clamps to MAXYEAR",
			input: "9999-12-31T23:59:59-14:00", // UTC normalizes to year 10000
			want:  "9999-12-31T23:59:59.999999Z",
		},
		{
			name:  "UTC year < 1 clamps to MINYEAR",
			input: "0001-01-01T00:00:00+14:00", // UTC normalizes to year 0
			want:  "0001-01-01T00:00:00Z",
		},
		{
			name:  "in-range value passes through unchanged",
			input: "2025-06-15T12:00:00Z",
			want:  "2025-06-15T12:00:00Z",
		},
		{
			name:  "space separator normalizes to RFC3339",
			input: "2025-11-29 01:05:28+00:00",
			want:  "2025-11-29T01:05:28Z",
		},
		{
			name:  "missing offset assumed UTC",
			input: "2025-11-29 01:05:28",
			want:  "2025-11-29T01:05:28Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := clampTimestamp(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestClampDate(t *testing.T) {
	tests := []struct {
		name    string
		input   any
		want    any
		wantErr bool
	}{
		{
			name:  "non-string passthrough",
			input: 42,
			want:  42,
		},
		{
			name:    "unparseable returns error",
			input:   "not-a-date",
			wantErr: true,
		},
		{
			name:  "year > 9999 clamps to MAXYEAR",
			input: "10000-01-01",
			want:  "9999-12-31",
		},
		{
			name:  "year < 1 clamps to MINYEAR",
			input: "0000-01-01",
			want:  "0001-01-01",
		},
		{
			name:  "in-range value passes through unchanged",
			input: "2025-06-15",
			want:  "2025-06-15",
		},
		{
			name:    "timestamp in a date column returns error",
			input:   "2025-11-29 01:05:28+00:00",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := clampDate(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
