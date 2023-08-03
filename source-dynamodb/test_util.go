package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

var (
	accessKeyId     = flag.String("accessKeyId", "id", "AWS Access Key ID")
	secretAccessKey = flag.String("secretAccessKey", "key", "AWS Secret Access Key")
	region          = flag.String("region", "region", "Region of the DynamoDB table")
	endpoint        = flag.String("endpoint", "http://localhost:8000", "The AWS endpoint URI to connect to")
)

func testClient(t *testing.T) (*client, config) {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil, config{}
	}

	ctx := context.Background()

	config := config{
		AWSAccessKeyID:     *accessKeyId,
		AWSSecretAccessKey: *secretAccessKey,
		Region:             *region,
		Advanced: advancedConfig{
			Endpoint:         *endpoint,
			BackfillSegments: 2, // Keep snapshotted checkpoints readable
		},
	}

	client, err := config.toClient(ctx)
	require.NoError(t, err)

	return client, config
}

type createTableParams struct {
	tableName string
	pkName    string
	skName    string

	pkType types.ScalarAttributeType
	skType types.ScalarAttributeType

	enableStream bool
}

func createTable(ctx context.Context, t *testing.T, client *client, params createTableParams) {
	t.Helper()

	input := &dynamodb.CreateTableInput{
		TableName: aws.String(params.tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(params.pkName),
				AttributeType: params.pkType,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(params.pkName),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	}

	if params.skName != "" {
		input.AttributeDefinitions = append(input.AttributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(params.skName),
			AttributeType: params.skType,
		})

		input.KeySchema = append(input.KeySchema, types.KeySchemaElement{
			AttributeName: aws.String(params.skName),
			KeyType:       types.KeyTypeRange,
		})
	}

	if params.enableStream {
		input.StreamSpecification = &types.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: types.StreamViewTypeNewAndOldImages,
		}
	}

	_, err := client.db.CreateTable(ctx, input)
	require.NoError(t, err)
	waitForTableActive(ctx, t, client, params.tableName)
}

func waitForTableActive(ctx context.Context, t *testing.T, client *client, tableName string) {
	t.Helper()

	for {
		d, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		require.NoError(t, err)
		if d.Table.TableStatus == types.TableStatusActive {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func deleteTable(ctx context.Context, t *testing.T, client *client, table string) {
	t.Helper()

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
		time.Sleep(500 * time.Millisecond)
	}
}

func addTestTableData(
	ctx context.Context,
	t *testing.T,
	c *client,
	tableName string,
	numItems int,
	startAtItem int,
	pkName string,
	pkData func(int) any,
	skName string,
	cols ...string,
) {
	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		item := make(map[string]any)
		item[pkName] = pkData(idx)
		if skName != "" {
			item[skName] = fmt.Sprintf("sk val %d", idx)
		}

		for _, col := range cols {
			item[col] = fmt.Sprintf("%s val %d", col, idx)
		}

		itemAttr, err := attributevalue.MarshalMap(item)
		require.NoError(t, err)

		_, err = c.db.PutItem(ctx, &dynamodb.PutItemInput{
			Item:      itemAttr,
			TableName: aws.String(tableName),
		})
		require.NoError(t, err)
	}
}
