package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bradleyjkemp/cupaloy"
	bp_test "github.com/estuary/connectors/materialize-boilerplate/testing"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

var (
	accessKeyId     = flag.String("awsAccessKeyId", "keyid", "AWS Access Key ID for materializing to DynamoDB.")
	secretAccessKey = flag.String("awsSecretAccessKey", "secret", "AWS Secret Access Key for materializing to DynamoDB.")
	region          = flag.String("region", "region", "Region of the materialized tables.")
	endpoint        = flag.String("endpoint", "http://localhost:8000", "The AWS endpoint URI to connect to. Use if you're materializing to a compatible API that isn't provided by AWS.")
)

func TestApply(t *testing.T) {
	ctx := context.Background()
	flag.Parse()

	cfg := config{
		AWSAccessKeyID:     *accessKeyId,
		AWSSecretAccessKey: *secretAccessKey,
		Region:             *region,
		Advanced: advancedConfig{
			Endpoint: *endpoint,
		},
	}

	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	firstTable := "first-table"
	secondTable := "second-table"

	firstResource := resource{
		Table:        firstTable,
		DeltaUpdates: false,
	}
	firstResourceJson, err := json.Marshal(firstResource)
	require.NoError(t, err)

	secondResource := resource{
		Table:        secondTable,
		DeltaUpdates: false,
	}
	secondResourceJson, err := json.Marshal(secondResource)
	require.NoError(t, err)

	client, err := cfg.client(ctx)
	require.NoError(t, err)

	bp_test.RunApplyTestCases(
		t,
		driver{},
		configJson,
		[2]json.RawMessage{firstResourceJson, secondResourceJson},
		[2][]string{{firstTable}, {secondTable}},
		func(t *testing.T) []string {
			listing, err := client.db.ListTables(ctx, &dynamodb.ListTablesInput{})
			require.NoError(t, err)

			return listing.TableNames
		},
		func(t *testing.T, resourcePath []string) string {
			t.Helper()

			d, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(resourcePath[0]),
			})
			require.NoError(t, err)

			b, err := json.MarshalIndent(d.Table.AttributeDefinitions, "", "  ")
			require.NoError(t, err)

			return string(b)
		},
		func(t *testing.T) {
			t.Helper()

			deleteTable := func(table string) {
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

			deleteTable(firstTable)
			deleteTable(secondTable)
			deleteTable(metaTableName)
		},
	)
}

func TestValidate(t *testing.T) {
	bp_test.RunValidateTestCases(t, ddbValidator, ".snapshots")
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
