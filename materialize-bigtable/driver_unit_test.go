package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestSpec(t *testing.T) {
	driver := driver{}
	response, err := driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestNormalizeTableName(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "not normalized",
			input: "Some.collection-Name_v1",
			want:  "Some.collection-Name_v1",
		},
		{
			name:  "non-ascii sanitized",
			input: "café_table",
			want:  "caf__table",
		},
		{
			name:  "leading dashes and dots stripped",
			input: "-.-foo.bar",
			want:  "foo.bar",
		},
		{
			name:  "truncated",
			input: strings.Repeat("a", maxTableNameLength+5),
			want:  strings.Repeat("a", maxTableNameLength),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, normalizeTableName(tt.input))
		})
	}
}
