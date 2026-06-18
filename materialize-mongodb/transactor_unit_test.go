package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// bulkErr builds a mongo.BulkWriteException with one per-item WriteError per
// entry in codeByIndex (index -> MongoDB error code). It is returned by value,
// which satisfies the error interface.
func bulkErr(codeByIndex map[int]int) mongo.BulkWriteException {
	var bwe mongo.BulkWriteException
	for idx, code := range codeByIndex {
		bwe.WriteErrors = append(bwe.WriteErrors, mongo.BulkWriteError{
			WriteError: mongo.WriteError{Index: idx, Code: code, Message: fmt.Sprintf("code %d", code)},
		})
	}
	return bwe
}

func model(id string) mongo.WriteModel {
	return &mongo.InsertOneModel{Document: bson.M{idField: id}}
}

func noDelay(context.Context, int) error { return nil }

// TestStoreBulkRetryResendsOnlyFailedModels is the regression test for transient
// per-item bulk write errors: a retryable per-item error (e.g. PrimarySteppedDown,
// code 189) must be retried by re-sending only the failed model, not surfaced as
// fatal. Counts from each attempt accumulate so the caller's sanity check holds.
func TestStoreBulkRetryResendsOnlyFailedModels(t *testing.T) {
	models := []mongo.WriteModel{model("A"), model("B"), model("C")}

	responses := []struct {
		res *mongo.BulkWriteResult
		err error
	}{
		{&mongo.BulkWriteResult{InsertedCount: 2}, bulkErr(map[int]int{1: 189})}, // B fails transiently
		{&mongo.BulkWriteResult{MatchedCount: 1}, nil},                           // resent B succeeds
	}
	var sent [][]mongo.WriteModel
	send := func(models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
		sent = append(sent, models)
		r := responses[0]
		responses = responses[1:]
		return r.res, r.err
	}

	res, err := storeBulkWithRetry(context.Background(), models, maxStoreRetries, send, noDelay)
	require.NoError(t, err)
	require.Len(t, sent, 2, "should send the initial batch then a retry")
	require.Len(t, sent[0], 3, "first send is the whole batch")
	require.Len(t, sent[1], 1, "retry re-sends only the failed model")
	require.Same(t, models[1], sent[1][0], "retry re-sends exactly the failed model")
	require.Equal(t, int64(2), res.InsertedCount, "counts accumulate across attempts")
	require.Equal(t, int64(1), res.MatchedCount, "counts accumulate across attempts")
}

// TestStoreBulkRetryFailsFastOnDuplicateKey guards the strict-insert tripwire: an
// E11000 duplicate-key (code 11000) is non-retryable and must be returned
// immediately without retrying.
func TestStoreBulkRetryFailsFastOnDuplicateKey(t *testing.T) {
	models := []mongo.WriteModel{model("A")}

	var sends int
	send := func(models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
		sends++
		return &mongo.BulkWriteResult{}, bulkErr(map[int]int{0: 11000})
	}

	_, err := storeBulkWithRetry(context.Background(), models, maxStoreRetries, send, noDelay)
	require.Error(t, err)
	require.Equal(t, 1, sends, "a duplicate-key error must not be retried")
	var bwe mongo.BulkWriteException
	require.True(t, errors.As(err, &bwe), "terminal error should be the BulkWriteException")
	require.Equal(t, 11000, bwe.WriteErrors[0].Code)
}

// TestStoreBulkRetryExhaustsAttempts guards that a persistently retryable error
// gives up after maxAttempts rather than looping forever, surfacing the
// underlying error.
func TestStoreBulkRetryExhaustsAttempts(t *testing.T) {
	models := []mongo.WriteModel{model("A")}

	var sends int
	send := func(models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
		sends++
		return &mongo.BulkWriteResult{}, bulkErr(map[int]int{0: 10107}) // NotWritablePrimary
	}

	_, err := storeBulkWithRetry(context.Background(), models, 3 /* maxAttempts */, send, noDelay)
	require.Error(t, err)
	require.Equal(t, 3, sends, "should attempt exactly maxAttempts times")
}
