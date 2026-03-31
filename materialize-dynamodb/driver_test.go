package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"

	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func testConfig() *config {
	return &config{
		AWSAccessKeyID:     "anything",
		AWSSecretAccessKey: "anything",
		Region:             "anything",
		Advanced: advancedConfig{
			Endpoint:     "http://localhost:8000",
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func TestValidateAndApply(t *testing.T) {
	t.Skip("TODO: migrate to new test structure")
}

func TestSpec(t *testing.T) {
	t.Parallel()

	driver := driver{}
	response, err := driver.Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(response, "", "  ")
	require.NoError(t, err)
	cupaloy.SnapshotT(t, string(formatted))
}

func TestNormalizeTableName(t *testing.T) {
	for _, tt := range []struct {
		name    string
		input   string
		want    string
		wantErr error
	}{
		{
			name:    "not normalized",
			input:   ".-Som3_collectioN",
			want:    ".-Som3_collectioN",
			wantErr: nil,
		},
		{
			name:    "normalized",
			input:   "@hello 🇱🇮",
			want:    "_hello___",
			wantErr: nil,
		},
		{
			name:    "too short",
			input:   "a",
			want:    "",
			wantErr: errors.New("table name 'a' is invalid: must contain at least 3 alphanumeric, dash ('-'), dot ('.'), or underscore ('_') characters"),
		},
		{
			name:    "empty input",
			input:   "",
			want:    "",
			wantErr: errors.New("table name '' is invalid: must contain at least 3 alphanumeric, dash ('-'), dot ('.'), or underscore ('_') characters"),
		},
		{
			name:    "truncated",
			input:   strings.Repeat("a", maxTableNameLength+1),
			want:    strings.Repeat("a", maxTableNameLength),
			wantErr: nil,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeTableName(tt.input)
			require.Equal(t, tt.wantErr, err)
			require.Equal(t, tt.want, got)
		})
	}
}
