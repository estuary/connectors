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

func TestClampTimestampNanos(t *testing.T) {
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
			name:  "after ns max clamps",
			input: "2500-01-01T00:00:00Z",
			want:  "2262-04-11T23:47:16.854775807Z",
		},
		{
			name:  "before ns min clamps",
			input: "1600-01-01T00:00:00Z",
			want:  "1677-09-21T00:12:43.145224192Z",
		},
		{
			name:  "exact ns min passes through",
			input: "1677-09-21T00:12:43.145224192Z",
			want:  "1677-09-21T00:12:43.145224192Z",
		},
		{
			name:  "exact ns max passes through",
			input: "2262-04-11T23:47:16.854775807Z",
			want:  "2262-04-11T23:47:16.854775807Z",
		},
		{
			name:  "in-range value passes through unchanged",
			input: "2025-06-15T12:00:00Z",
			want:  "2025-06-15T12:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := clampTimestampNanos(tt.input)
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
