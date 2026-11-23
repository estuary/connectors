package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	log "github.com/sirupsen/logrus"
)

// tablePollInterval and tableWaitTimeout pace and bound waiting for a created
// table to become active, or a deleted one to be gone.
const (
	tablePollInterval = time.Second
	tableWaitTimeout  = 30 * time.Second
)

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
		// Any error other than an "already exists" error is a more serious problem. Usually we
		// should not be trying to create tables that already exist, so emit a warning log if that
		// ever occurs.
		if _, ok := errors.AsType[*types.ResourceInUseException](err); !ok {
			return fmt.Errorf("create table %s: %w", name, err)
		}
		log.WithField("table", name).Warn("table already exists")
	}

	if err := dynamodb.NewTableExistsWaiter(client.db, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay, o.MaxDelay = tablePollInterval, tablePollInterval
	}).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}, tableWaitTimeout); err != nil {
		return fmt.Errorf("waiting for table %s to become active: %w", name, err)
	}
	return nil
}

func deleteTable(ctx context.Context, client *client, name string) error {
	if _, err := client.db.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(name),
	}); err != nil {
		return fmt.Errorf("deleting existing table: %w", err)
	}

	if err := dynamodb.NewTableNotExistsWaiter(client.db, func(o *dynamodb.TableNotExistsWaiterOptions) {
		o.MinDelay, o.MaxDelay = tablePollInterval, tablePollInterval
	}).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(name)}, tableWaitTimeout); err != nil {
		return fmt.Errorf("waiting for table %s to finish deleting: %w", name, err)
	}
	return nil
}
