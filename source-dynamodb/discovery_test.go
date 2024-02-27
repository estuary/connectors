package main

import (
	"context"
	"regexp"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	st "github.com/estuary/connectors/source-boilerplate/testing"
)

func TestDiscovery(t *testing.T) {
	ctx := context.Background()

	client, cfg := testClient(t)

	t.Run("single table with a partition key", func(t *testing.T) {
		tableName := "discoverTable"

		cleanup := func() {
			deleteTable(ctx, t, client, tableName)
		}
		cleanup()
		t.Cleanup(cleanup)

		createTable(ctx, t, client, createTableParams{
			tableName:    tableName,
			pkName:       "partitionKey",
			pkType:       types.ScalarAttributeTypeS,
			enableStream: true,
		})

		var sanitizers = make(map[string]*regexp.Regexp)
		sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`)
		cs := &st.CaptureSpec{
			Driver:       driver{},
			EndpointSpec: &cfg,
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   testSanitizers,
		}

		cs.VerifyDiscover(ctx, t)
	})

	t.Run("additional table without stream enabled", func(t *testing.T) {
		tableName := "discoverTable"
		otherTableName := "noStreamTable"

		cleanup := func() {
			deleteTable(ctx, t, client, tableName)
			deleteTable(ctx, t, client, otherTableName)
		}
		cleanup()
		t.Cleanup(cleanup)

		createTable(ctx, t, client, createTableParams{
			tableName:    tableName,
			pkName:       "partitionKey",
			pkType:       types.ScalarAttributeTypeS,
			enableStream: true,
		})
		createTable(ctx, t, client, createTableParams{
			tableName:    otherTableName,
			pkName:       "partitionKey",
			pkType:       types.ScalarAttributeTypeS,
			enableStream: false,
		})

		cs := &st.CaptureSpec{
			Driver:       driver{},
			EndpointSpec: &cfg,
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   testSanitizers,
		}

		cs.VerifyDiscover(ctx, t)
	})

	t.Run("multiple tables with various types", func(t *testing.T) {
		tableName := "discoverTable"
		otherTableName := "secondTable"
		lastTableName := "thirdTable"

		cleanup := func() {
			deleteTable(ctx, t, client, tableName)
			deleteTable(ctx, t, client, otherTableName)
			deleteTable(ctx, t, client, lastTableName)
		}
		cleanup()
		t.Cleanup(cleanup)

		createTable(ctx, t, client, createTableParams{
			tableName:    tableName,
			pkName:       "partitionKey",
			pkType:       types.ScalarAttributeTypeS,
			enableStream: true,
		})
		createTable(ctx, t, client, createTableParams{
			tableName:    otherTableName,
			pkName:       "partitionKeyString",
			pkType:       types.ScalarAttributeTypeS,
			skName:       "sortKeyNumber",
			skType:       types.ScalarAttributeTypeN,
			enableStream: true,
		})
		createTable(ctx, t, client, createTableParams{
			tableName:    lastTableName,
			pkName:       "partitionKeyBinary",
			pkType:       types.ScalarAttributeTypeB,
			skName:       "sortKeyBinary",
			skType:       types.ScalarAttributeTypeB,
			enableStream: true,
		})

		cs := &st.CaptureSpec{
			Driver:       driver{},
			EndpointSpec: &cfg,
			Validator:    &st.SortedCaptureValidator{},
			Sanitizers:   testSanitizers,
		}

		cs.VerifyDiscover(ctx, t)
	})
}

var testSanitizers = map[string]*regexp.Regexp{
	`"<TIMESTAMP>"`: regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`),
}
