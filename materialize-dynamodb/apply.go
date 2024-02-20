package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
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
)

type ddbApplier struct {
	client *client
	cfg    config
}

func (e *ddbApplier) CreateMetaTables(ctx context.Context, spec *pf.MaterializationSpec) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("create table %q", metaTableName), func(ctx context.Context) error {
		return createTable(ctx, e.client, metaTableName, metaTableAttrs, metaTableSchema)
	}, nil
}

func (e *ddbApplier) CreateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int) (string, boilerplate.ActionApplyFn, error) {
	binding := spec.Bindings[bindingIndex]

	tableName := binding.ResourcePath[0]
	attrs, schema := tableConfigFromBinding(binding.Collection.Projections)

	return fmt.Sprintf("create table %q", tableName), func(ctx context.Context) error {
		return createTable(ctx, e.client, tableName, attrs, schema)
	}, nil
}

func (e *ddbApplier) LoadSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	return getSpec(ctx, e.client, materialization.String())
}

func (e *ddbApplier) PutSpec(ctx context.Context, spec *pf.MaterializationSpec, version string, _ bool) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("update stored materialization spec and set version = %s", version), func(ctx context.Context) error {
		specBytes, err := spec.Marshal()
		if err != nil {
			return fmt.Errorf("marshalling spec: %w", err)
		}

		if _, err := e.client.db.PutItem(ctx, &dynamodb.PutItemInput{
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
	}, nil

}

func (e *ddbApplier) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return fmt.Sprintf("delete table %q", path[0]), func(ctx context.Context) error {
		return deleteTable(ctx, e.client, path[0])
	}, nil
}

func (e *ddbApplier) UpdateResource(ctx context.Context, spec *pf.MaterializationSpec, bindingIndex int, bindingUpdate boilerplate.BindingUpdate) (string, boilerplate.ActionApplyFn, error) {
	// No-op since DynamoDB only applies a schema to the key columns, and Flow doesn't allow you to
	// change the key of an established collection, and the Validation constraints don't allow
	// changing the type of a key field in a way that would change its materialized type.
	return "", nil, nil
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
			// Metadata table doesn't exist yet.
			return nil, nil
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

func createTable(
	ctx context.Context,
	client *client,
	name string,
	attrs []types.AttributeDefinition,
	keySchema []types.KeySchemaElement,
) error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: attrs,
		KeySchema:            keySchema,
		TableName:            aws.String(name),
		BillingMode:          types.BillingModePayPerRequest,
	}

	_, err := client.db.CreateTable(ctx, input)
	if err != nil {
		var errInUse *types.ResourceInUseException
		// Any error other than an "already exists" error is a more serious problem. Usually we
		// should not be trying to create tables that already exist, so emit a warning log if that
		// ever occurs.
		if !errors.As(err, &errInUse) {
			return fmt.Errorf("create table %s: %w", name, err)
		}
		log.WithField("table", name).Warn("table already exists")
	}

	// Wait for the table to be in an "active" state.
	maxAttempts := 30
	for attempt := 0; attempt < maxAttempts; attempt++ {
		d, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
		if err != nil {
			return err
		}

		if d.Table.TableStatus == types.TableStatusActive {
			return nil
		}

		log.WithFields(log.Fields{
			"table":      name,
			"lastStatus": d.Table.TableStatus,
		}).Debug("waiting for table to become ready")
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("table %s was created but did not become ready in time", name)
}

func deleteTable(ctx context.Context, client *client, name string) error {
	var errNotFound *types.ResourceNotFoundException

	if _, err := client.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	}); err != nil {
		return fmt.Errorf("deleting existing table: %w", err)
	}

	// Wait for the table to be fully deleted.
	attempts := 30
	for {
		if attempts < 0 {
			return fmt.Errorf("table %s did not finish deleting in time", name)
		}

		d, err := client.db.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(name),
		})
		if err != nil {
			if errors.As(err, &errNotFound) {
				return nil
			}
			return fmt.Errorf("waiting for table deletion to finish: %w", err)
		}

		log.WithFields(log.Fields{
			"table":      name,
			"lastStatus": d.Table.TableStatus,
		}).Debug("waiting for table deletion to complete")

		time.Sleep(1 * time.Second)
		attempts -= 1
	}
}

func tableConfigFromBinding(projections []pf.Projection) ([]types.AttributeDefinition, []types.KeySchemaElement) {
	mappedKeys := []mappedType{}
	for _, p := range projections {
		if p.IsPrimaryKey {
			mappedKeys = append(mappedKeys, mapType(&p))
		}
	}

	// The collection keys will be used as the partition key and sort key, respectively.
	keyTypes := [2]types.KeyType{types.KeyTypeHash, types.KeyTypeRange}

	attrs := []types.AttributeDefinition{}
	schema := []types.KeySchemaElement{}

	for idx, k := range mappedKeys {
		attrs = append(attrs, types.AttributeDefinition{
			AttributeName: aws.String(k.field),
			AttributeType: k.ddbScalarType,
		})
		schema = append(schema, types.KeySchemaElement{
			AttributeName: aws.String(k.field),
			KeyType:       keyTypes[idx],
		})
	}

	return attrs, schema
}
