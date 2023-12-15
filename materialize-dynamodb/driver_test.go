package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bradleyjkemp/cupaloy"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func testConfig() *config {
	return &config{
		AWSAccessKeyID:     "anything",
		AWSSecretAccessKey: "anything",
		Region:             "anything",
		Advanced: advancedConfig{
			Endpoint: "http://localhost:8000",
		},
	}
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	flag.Parse()

	cfg := testConfig()

	resourceConfig := resource{
		Table: "target",
	}

	client, err := cfg.client(ctx)
	require.NoError(t, err)

	boilerplate.RunValidateAndApplyTestCases(
		t,
		driver{},
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			d, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(resourceConfig.Table),
			})
			require.NoError(t, err)

			slices.SortFunc(d.Table.AttributeDefinitions, func(i, j types.AttributeDefinition) int {
				return strings.Compare(*i.AttributeName, *j.AttributeName)
			})

			var out strings.Builder
			enc := json.NewEncoder(&out)
			for _, r := range d.Table.AttributeDefinitions {
				require.NoError(t, enc.Encode(r))
			}

			return out.String()
		},
		func(t *testing.T, materialization pf.Materialization) {
			t.Helper()

			for _, table := range []string{resourceConfig.Table, metaTableName} {
				_, err := client.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{
					TableName: aws.String(table),
				})

				var errNotFound *types.ResourceNotFoundException

				if err != nil {
					require.ErrorAs(t, err, &errNotFound)
					return
				}

				for {
					_, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
						TableName: aws.String(table),
					})
					if err != nil {
						require.ErrorAs(t, err, &errNotFound)
						return
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		},
	)
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
