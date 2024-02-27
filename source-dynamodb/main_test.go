package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/bradleyjkemp/cupaloy"
	st "github.com/estuary/connectors/source-boilerplate/testing"
	"github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestCapture(t *testing.T) {
	table1 := "tableOne"
	table2 := "tableTwo"
	table3 := "tableThree"

	ctx := context.Background()
	client, cfg := testClient(t)

	// Use artificially small chunks to exercise multiple required backfill requests.
	cfg.Advanced.ScanLimit = 3

	cleanup := func() {
		deleteTable(ctx, t, client, table1)
		deleteTable(ctx, t, client, table2)
		deleteTable(ctx, t, client, table3)
	}
	cleanup()
	t.Cleanup(cleanup)

	createTable(ctx, t, client, createTableParams{
		tableName:    table1,
		pkName:       "partitionKeyString",
		pkType:       types.ScalarAttributeTypeS,
		enableStream: true,
	})
	createTable(ctx, t, client, createTableParams{
		tableName:    table2,
		pkName:       "partitionKeyNumber",
		pkType:       types.ScalarAttributeTypeN,
		skName:       "sortKey",
		skType:       types.ScalarAttributeTypeS,
		enableStream: true,
	})
	createTable(ctx, t, client, createTableParams{
		tableName:    table3,
		pkName:       "partitionKeyBinary",
		pkType:       types.ScalarAttributeTypeB,
		skName:       "sortKey",
		skType:       types.ScalarAttributeTypeS,
		enableStream: true,
	})

	stringPkVals := func(idx int) any {
		return fmt.Sprintf("pk val %d", idx)
	}

	numberPkVals := func(idx int) any {
		if idx%2 == 0 {
			return idx
		}

		return float64(idx) + 0.5
	}

	binaryPkVals := func(idx int) any {
		return []byte(fmt.Sprintf("pk val %d", idx))
	}

	addTestTableData(ctx, t, client, table1, 5, 0, "partitionKeyString", stringPkVals, "", "onlyColumn")
	addTestTableData(ctx, t, client, table2, 5, 0, "partitionKeyNumber", numberPkVals, "sortKey", "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, table3, 5, 0, "partitionKeyBinary", binaryPkVals, "sortKey", "firstColumn", "secondColumn", "thirdColumn")

	cs := &st.CaptureSpec{
		Driver:       driver{},
		EndpointSpec: &cfg,
		Checkpoint:   []byte("{}"),
		// Values returned from individual segment queries will appear to be a random order, but
		// this is because of how DynamoDB hashes values from the partition key. The returned
		// orderings are deterministic per segment, but the concurrent processing of segments makes
		// the overall ordering non-deterministic.
		Validator:  &st.SortedCaptureValidator{},
		Sanitizers: commonSanitizers(),
		Bindings:   bindings(t, table1, table2, table3),
	}

	// Run the capture, stopping it before it has completed the entire backfill.
	count := 0
	captureCtx, cancel := context.WithCancel(context.Background())
	cs.Capture(captureCtx, t, func(msg json.RawMessage) {
		count++
		if count == 10 {
			cancel()
		}
	})

	// Run the capture again and let it finish the backfill, resuming from the previous checkpoint.
	advanceCapture(ctx, t, cs)

	// Run the capture one more time, first adding more data that will be picked up from stream
	// shards, to verify resumption from stream checkpoints.
	addTestTableData(ctx, t, client, table1, 5, 5, "partitionKeyString", stringPkVals, "", "onlyColumn")
	addTestTableData(ctx, t, client, table2, 5, 5, "partitionKeyNumber", numberPkVals, "sortKey", "firstColumn", "secondColumn")
	addTestTableData(ctx, t, client, table3, 5, 5, "partitionKeyBinary", binaryPkVals, "sortKey", "firstColumn", "secondColumn", "thirdColumn")

	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCaptureDataTypes(t *testing.T) {
	table := "datatypes"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		deleteTable(ctx, t, client, table)
	}
	cleanup()
	t.Cleanup(cleanup)

	createTable(ctx, t, client, createTableParams{
		tableName:    table,
		pkName:       "pk",
		pkType:       types.ScalarAttributeTypeS,
		enableStream: true,
	})

	// Ref:
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypeDescriptors
	av := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{
			Value: "pk",
		},
		"binary": &types.AttributeValueMemberB{
			Value: []byte{1, 2, 3},
		},
		"booleanTrue": &types.AttributeValueMemberBOOL{
			Value: true,
		},
		"booleanFalse": &types.AttributeValueMemberBOOL{
			Value: false,
		},
		"binarySet": &types.AttributeValueMemberBS{
			Value: [][]byte{{1, 2, 3}, {4, 5, 6}},
		},
		"list": &types.AttributeValueMemberL{
			Value: []types.AttributeValue{
				&types.AttributeValueMemberS{
					Value: "stringValue",
				},
				&types.AttributeValueMemberN{
					Value: "123",
				},
			},
		},
		"map": &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"stringKey": &types.AttributeValueMemberS{
					Value: "stringValue",
				},
				"numberKey": &types.AttributeValueMemberN{
					Value: "123",
				},
			},
		},
		"decimal": &types.AttributeValueMemberN{
			Value: "123.45",
		},
		"integer": &types.AttributeValueMemberN{
			Value: "123",
		},
		"numberSet": &types.AttributeValueMemberNS{
			Value: []string{"123", "123.45"},
		},
		"null": &types.AttributeValueMemberNULL{
			Value: true, // This can never be false.
		},
		"string": &types.AttributeValueMemberS{
			Value: "stringValue",
		},
		"stringSet": &types.AttributeValueMemberSS{
			Value: []string{"stringValue", "anotherStringValue"},
		},
	}

	_, err := client.db.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(table),
	})
	require.NoError(t, err)

	cs := &st.CaptureSpec{
		Driver:       driver{},
		EndpointSpec: &cfg,
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     bindings(t, table),
	}

	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func TestCaptureOperations(t *testing.T) {
	table := "operations"

	ctx := context.Background()
	client, cfg := testClient(t)

	cleanup := func() {
		deleteTable(ctx, t, client, table)
	}
	cleanup()
	t.Cleanup(cleanup)

	createTable(ctx, t, client, createTableParams{
		tableName:    table,
		pkName:       "pk",
		pkType:       types.ScalarAttributeTypeS,
		enableStream: true,
	})

	create := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{
			Value: "backfill",
		},
		"string": &types.AttributeValueMemberS{
			Value: "value",
		},
	}

	_, err := client.db.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      create,
		TableName: aws.String(table),
	})
	require.NoError(t, err)

	cs := &st.CaptureSpec{
		Driver:       driver{},
		EndpointSpec: &cfg,
		Validator:    &st.OrderedCaptureValidator{},
		Sanitizers:   commonSanitizers(),
		Bindings:     bindings(t, table),
	}

	// Capture documents from the backfill.
	advanceCapture(ctx, t, cs)

	streamCreate := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{
			Value: "stream",
		},
		"string": &types.AttributeValueMemberS{
			Value: "initial",
		},
	}
	_, err = client.db.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      streamCreate,
		TableName: aws.String(table),
	})
	require.NoError(t, err)

	streamUpdate := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{
			Value: "stream",
		},
		"string": &types.AttributeValueMemberS{
			Value: "updated",
		},
	}
	_, err = client.db.PutItem(ctx, &dynamodb.PutItemInput{
		Item:      streamUpdate,
		TableName: aws.String(table),
	})
	require.NoError(t, err)

	_, err = client.db.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"pk": streamCreate["pk"],
		},
		TableName: aws.String(table),
	})
	require.NoError(t, err)

	// Capture streamed change records.
	advanceCapture(ctx, t, cs)

	cupaloy.SnapshotT(t, cs.Summary())
}

func commonSanitizers() map[string]*regexp.Regexp {
	sanitizers := make(map[string]*regexp.Regexp)
	sanitizers[`"<TIMESTAMP>"`] = regexp.MustCompile(`"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?(Z|-[0-9]+:[0-9]+)"`)
	sanitizers[`"<SHARD_ID>"`] = regexp.MustCompile(`"shardId\-\d+\-\w+"`)
	sanitizers[`"<STREAM_ARN>"`] = regexp.MustCompile(`"arn:aws:dynamodb:\w+:\d+:.+?\.\d+"`)
	sanitizers[`<UUID>`] = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	sanitizers[`"lastReadSequence":"<SEQUENCE_NUM>"`] = regexp.MustCompile(`"lastReadSequence":"\d+"`)

	return sanitizers
}

func resourceSpecJson(t *testing.T, r resource) json.RawMessage {
	t.Helper()

	out, err := json.Marshal(r)
	require.NoError(t, err)

	return out
}

func bindings(t *testing.T, tableNames ...string) []*flow.CaptureSpec_Binding {
	t.Helper()

	out := []*flow.CaptureSpec_Binding{}
	for _, tbl := range tableNames {
		out = append(out, &flow.CaptureSpec_Binding{
			ResourceConfigJson: resourceSpecJson(t, resource{Table: tbl, RcuAllocation: 1000}),
			ResourcePath:       []string{tbl},
			Collection:         flow.CollectionSpec{Name: flow.Collection(fmt.Sprintf("acmeCo/test/%s", tbl))},
			Backfill:           0,
			StateKey:           tbl,
		})
	}

	return out
}

func advanceCapture(ctx context.Context, t testing.TB, cs *st.CaptureSpec) {
	t.Helper()
	captureCtx, cancelCapture := context.WithCancel(ctx)

	const shutdownDelay = 1000 * time.Millisecond
	var shutdownWatchdog *time.Timer
	cs.Capture(captureCtx, t, func(data json.RawMessage) {
		if shutdownWatchdog == nil {
			shutdownWatchdog = time.AfterFunc(shutdownDelay, func() {
				log.WithField("delay", shutdownDelay.String()).Debug("capture shutdown watchdog expired")
				cancelCapture()
			})
		}
		shutdownWatchdog.Reset(shutdownDelay)
	})

	for _, e := range cs.Errors {
		require.NoError(t, e)
	}
}
