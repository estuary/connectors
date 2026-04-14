package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelName(t *testing.T) {
	require.Equal(t, "testing_4E7A62CBF3428987_00000000", channelName("testing", 0))
	require.Equal(t, "testing_4E7A62CBF3428987_000004d2", channelName("testing", 1234))
	require.Equal(t, "some_other_stuff__816D5E80F9E632A7_000004d2", channelName("some/other-stuff!", 1234))
	require.Equal(t, "long_long_long_long_long_long_lo_05A28455EE5956EB_00000000", channelName(strings.Repeat("long/", 10), 0))
	require.Equal(t, "long_long_long_long_long_long_lo_135047533004DC65_00000000", channelName(strings.Repeat("long!", 10), 0))
}

func TestGetNextFileName(t *testing.T) {
	calendar, err := time.Parse(time.RFC3339Nano, "2025-04-03T13:40:36.434227277Z")
	require.NoError(t, err)

	clientPrefix := "asdfasdf_1234"

	sm := &streamManager{keyBegin: 5678, counter: -1}

	for idx := range 5 {
		require.Equal(t,
			fmt.Sprintf("2025/4/3/13/40/su59zo_asdfasdf_1234_5678_%d.bdec", idx),
			string(sm.getNextFileName(calendar, clientPrefix)),
		)
	}
}

func TestShouldWriteNextToken(t *testing.T) {
	strPtr := func(s string) *string { return &s }

	for _, tt := range []struct {
		name         string
		blobToken    string
		currentToken *string
		want         bool
		wantErr      bool
	}{
		{
			name:         "no current token",
			blobToken:    "the-token-1:0",
			currentToken: nil,
			want:         true,
			wantErr:      false,
		},
		{
			name:         "same token",
			blobToken:    "the-token-1:0",
			currentToken: strPtr("the-token-1:0"),
			want:         false,
			wantErr:      false,
		},
		{
			name:         "next in sequence",
			blobToken:    "the-token-1:2",
			currentToken: strPtr("the-token-1:1"),
			want:         true,
			wantErr:      false,
		},
		{
			name:         "prior in sequence",
			blobToken:    "the-token-1:1",
			currentToken: strPtr("the-token-1:2"),
			want:         false,
			wantErr:      false,
		},
		{
			name:         "start new token sequence",
			blobToken:    "the-token-2:0",
			currentToken: strPtr("the-token-1:2"),
			want:         true,
			wantErr:      false,
		},
		{
			name:         "wrong token",
			blobToken:    "the-token-2:1",
			currentToken: strPtr("the-token-1:2"),
			want:         false,
			wantErr:      true,
		},
		{
			name:         "malformed current",
			blobToken:    "the-token-2:1",
			currentToken: strPtr("wrong:asdf"),
			want:         false,
			wantErr:      true,
		},
		{
			name:         "malformed blob",
			blobToken:    "wrong",
			currentToken: strPtr("the-token-1:2"),
			want:         false,
			wantErr:      true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shouldWriteNextToken(tt.blobToken, tt.currentToken)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestValidWriteBlobs(t *testing.T) {
	makeChunkMeta := func(db, sch, tbl, ch, tok string) uploadChunkMetadata {
		return uploadChunkMetadata{
			Database: db,
			Schema:   sch,
			Table:    tbl,
			Channels: []uploadChunkChannelMetadata{{Channel: ch, OffsetToken: tok}},
		}
	}

	db := "db"
	sch := "sch"
	tbl := "tbl"
	ch := "ch"

	for _, tt := range []struct {
		name  string
		blobs []*blobMetadata
		valid bool
	}{
		{
			name: "single blob",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
			},
			valid: true,
		},
		{
			name: "multi blob",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: true,
		},
		{
			name: "wrong base token",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "other:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "out of order",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
			},
			valid: false,
		},
		{
			name: "more than one chunk in a single blob",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{
					makeChunkMeta(db, sch, tbl, ch, "token:1"),
					makeChunkMeta(db, sch, tbl, ch, "token:2"),
				}},
			},
			valid: false,
		},
		{
			name: "more than one channel in a single chunk",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{
					{
						Database: db,
						Schema:   sch,
						Table:    tbl,
						Channels: []uploadChunkChannelMetadata{
							{Channel: ch, OffsetToken: "token:1"},
							{Channel: ch, OffsetToken: "token:2"},
						},
					},
				}},
			},
			valid: false,
		},
		{
			name: "different databases",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db+"other", sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "different schemas",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch+"other", tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "different tables",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl+"other", ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "different channels",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch+"other", "token:1")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:2")}},
				{Chunks: []uploadChunkMetadata{makeChunkMeta(db, sch, tbl, ch, "token:3")}},
			},
			valid: false,
		},
		{
			name: "malformed token",
			blobs: []*blobMetadata{
				{Chunks: []uploadChunkMetadata{{Channels: []uploadChunkChannelMetadata{{OffsetToken: "asdf"}}}}},
			},
			valid: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validWriteBlobs(tt.blobs)
			if tt.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
