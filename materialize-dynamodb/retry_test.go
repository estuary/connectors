package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/estuary/connectors/go/retry"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	require.Equal(t, retry.Retryable, classify(fmt.Errorf("storing: %w", &unprocessedError{operation: "BatchWriteItem", count: 1})))
	require.Equal(t, retry.Fatal, classify(&types.ProvisionedThroughputExceededException{}))
	require.Equal(t, retry.Fatal, classify(context.Canceled))
}

// fakeDynamoDB answers each request with the next of its responses, and keeps
// the request bodies it was sent.
type fakeDynamoDB struct {
	responses []string
	requests  []map[string]json.RawMessage
}

func (f *fakeDynamoDB) Do(req *http.Request) (*http.Response, error) {
	var body map[string]json.RawMessage
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
		return nil, err
	}
	f.requests = append(f.requests, body)

	var resp = f.responses[0]
	if len(f.responses) > 1 {
		f.responses = f.responses[1:]
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewBufferString(resp)),
	}, nil
}

func (f *fakeDynamoDB) transactor() *transactor {
	return &transactor{
		client: &client{db: dynamodb.New(dynamodb.Options{
			Region:       "us-east-1",
			BaseEndpoint: aws.String("http://dynamodb.test"),
			Credentials:  aws.AnonymousCredentials{},
			HTTPClient:   f,
			Retryer:      aws.NopRetryer{},
		})},
		tablesToBindings: map[string]int{"tbl": 0},
	}
}

func put(key string) types.WriteRequest {
	return types.WriteRequest{PutRequest: &types.PutRequest{
		Item: map[string]types.AttributeValue{"key": &types.AttributeValueMemberS{Value: key}},
	}}
}

func runStoreWorker(t *testing.T, fake *fakeDynamoDB, batch map[string][]types.WriteRequest) (int64, error) {
	var batches = make(chan map[string][]types.WriteRequest, 1)
	batches <- batch
	close(batches)

	var stored = make([]atomic.Int64, 1)
	var err = fake.transactor().storeWorker(t.Context(), batches, stored)
	return stored[0].Load(), err
}

func TestStoreWorkerResendsUnprocessedItems(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fake = &fakeDynamoDB{responses: []string{
			`{"UnprocessedItems":{"tbl":[{"PutRequest":{"Item":{"key":{"S":"b"}}}}]}}`,
			`{"UnprocessedItems":{}}`,
		}}

		stored, err := runStoreWorker(t, fake, map[string][]types.WriteRequest{"tbl": {put("a"), put("b"), put("c")}})
		require.NoError(t, err)
		require.Equal(t, int64(3), stored)
		require.Len(t, fake.requests, 2)
		require.JSONEq(t,
			`{"tbl":[{"PutRequest":{"Item":{"key":{"S":"b"}}}}]}`,
			string(fake.requests[1]["RequestItems"]),
			"the retry re-sends only the unprocessed item")
	})
}

func TestStoreWorkerGivesUpOnUnprocessedItems(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fake = &fakeDynamoDB{responses: []string{
			`{"UnprocessedItems":{"tbl":[{"PutRequest":{"Item":{"key":{"S":"a"}}}}]}}`,
		}}

		stored, err := runStoreWorker(t, fake, map[string][]types.WriteRequest{"tbl": {put("a")}})
		require.ErrorContains(t, err, "after 30 attempts: BatchWriteItem left 1 requests unprocessed")
		require.Equal(t, int64(0), stored)
		require.Len(t, fake.requests, unprocessedRetry.MaxAttempts)
	})
}

func TestLoadWorkerResendsUnprocessedKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fake = &fakeDynamoDB{responses: []string{
			`{"Responses":{"tbl":[{"doc":{"S":"a"}}]},"UnprocessedKeys":{"tbl":{"Keys":[{"key":{"S":"b"}}]}}}`,
			`{"Responses":{"tbl":[{"doc":{"S":"b"}}]},"UnprocessedKeys":{}}`,
		}}

		var batches = make(chan map[string]types.KeysAndAttributes, 1)
		batches <- map[string]types.KeysAndAttributes{"tbl": {Keys: []map[string]types.AttributeValue{
			{"key": &types.AttributeValueMemberS{Value: "a"}},
			{"key": &types.AttributeValueMemberS{Value: "b"}},
		}}}
		close(batches)

		var loaded []string
		require.NoError(t, fake.transactor().loadWorker(t.Context(), func(_ int, doc json.RawMessage) error {
			loaded = append(loaded, string(doc))
			return nil
		}, batches))

		require.Equal(t, []string{`"a"`, `"b"`}, loaded)
		require.Len(t, fake.requests, 2)
		require.JSONEq(t,
			`{"tbl":{"Keys":[{"key":{"S":"b"}}]}}`,
			string(fake.requests[1]["RequestItems"]),
			"the retry re-sends only the unprocessed key")
	})
}
