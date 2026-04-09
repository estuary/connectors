package main

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpecification(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestNormalizeIndexName(t *testing.T) {
	for _, tt := range []struct {
		name       string
		byteLength int
		input      string
		want       string
	}{
		{
			name:       "not normalized",
			byteLength: maxByteLength,
			input:      "some_collection",
			want:       "some_collection",
		},
		{
			name:       "dots",
			byteLength: maxByteLength,
			input:      ".some..collection",
			want:       "some..collection",
		},
		{
			name:       "normalized characters",
			byteLength: maxByteLength,
			input:      `??weird*<"\/,|>?:#index`,
			want:       "weird___________index",
		},
		{
			name:       "capitalized",
			byteLength: maxByteLength,
			input:      "SomeCollection",
			want:       "somecollection",
		},
		{
			name:       "strip prefixes",
			byteLength: maxByteLength,
			input:      "-_.collection",
			want:       "collection",
		},
		{
			name:       "truncate Ascii",
			byteLength: 3,
			input:      "__aaaa",
			want:       "aaa",
		},
		{
			name:       "truncate UTF-8",
			byteLength: 10,
			input:      "__中文内码",
			want:       "中文内",
		},
		{
			name:       "empty input",
			byteLength: maxByteLength,
			input:      "",
			want:       "",
		},
		{
			name:       "no valid characters",
			byteLength: maxByteLength,
			input:      "._-",
			want:       "",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeIndexName(tt.input, tt.byteLength))
		})
	}
}
