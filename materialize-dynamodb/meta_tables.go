package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	pf "github.com/estuary/flow/go/protocols/flow"
)

var (
	metaTableName = "flow_materializations_v2"

	metaTableAttrs = []types.AttributeDefinition{
		{
			AttributeName: aws.String("materialization"),
			AttributeType: types.ScalarAttributeTypeS,
		},
	}

	metaTableSchema = []types.KeySchemaElement{
		{
			AttributeName: aws.String("materialization"),
			KeyType:       types.KeyTypeHash,
		},
	}

	errMetaTableNotFound = errors.New("metadata table not found")
)

func putSpec(ctx context.Context, client *client, spec *pf.MaterializationSpec, version string) error {
	specBytes, err := spec.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling spec: %w", err)
	}

	if _, err := client.db.PutItem(ctx, &dynamodb.PutItemInput{
		Item: map[string]types.AttributeValue{
			"materialization": &types.AttributeValueMemberS{Value: spec.Name.String()},
			"spec":            &types.AttributeValueMemberB{Value: specBytes},
			"version":         &types.AttributeValueMemberS{Value: version},
		},
		TableName: aws.String(metaTableName),
	}); err != nil {
		return fmt.Errorf("PutItem for updated spec: %w", err)
	}

	return nil
}

func getSpec(ctx context.Context, client *client, materialization string) (*pf.MaterializationSpec, error) {
	item, err := client.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(metaTableName),
		Key: map[string]types.AttributeValue{
			"materialization": &types.AttributeValueMemberS{Value: materialization},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		var errNotFound *types.ResourceNotFoundException
		if errors.As(err, &errNotFound) {
			return nil, errMetaTableNotFound
		}

		return nil, fmt.Errorf("getItem: %w", err)
	}

	ss := []byte{}
	if err := attributevalue.Unmarshal(item.Item["spec"], &ss); err != nil {
		return nil, err
	}

	spec := &pf.MaterializationSpec{}
	if err := spec.Unmarshal(ss); err != nil {
		return nil, fmt.Errorf("unmarshalling spec: %w", err)
	}

	return spec, nil
}
