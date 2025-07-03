package main

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
