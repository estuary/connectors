package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestBatchStreamCoordinator(t *testing.T) {
	ctx := context.Background()

	getOpTime := func(_ context.Context) (primitive.Timestamp, error) {
		return primitive.Timestamp{T: 1000}, nil
	}

	bindings := []bindingInfo{
		{resource: resource{Database: "first"}},
		{resource: resource{Database: "second"}},
		{resource: resource{Database: "third"}},
	}

	coord := newBatchStreamCoordinator(bindings, getOpTime)

	select {
	case <-coord.streamsCaughtUp():
	default:
		require.Fail(t, "expected streams to be caught up on initialization")
	}

	require.NoError(t, coord.startCatchingUp(ctx))

	select {
	case <-coord.streamsCaughtUp():
		require.Fail(t, "expected streams to not be caught up after starting catch up")
	default:
	}

	require.False(t, coord.gotCaughtUp("first", primitive.Timestamp{T: 999}))
	require.True(t, coord.gotCaughtUp("first", primitive.Timestamp{T: 1000}))
	require.False(t, coord.gotCaughtUp("first", primitive.Timestamp{T: 1000}))

	select {
	case <-coord.streamsCaughtUp():
		require.Fail(t, "expected streams to not be caught up after only one stream got caught up")
	default:
	}

	require.True(t, coord.gotCaughtUp("second", primitive.Timestamp{T: 1001}))
	require.True(t, coord.gotCaughtUp("third", primitive.Timestamp{T: 1001}))

	select {
	case <-coord.streamsCaughtUp():
	default:
		require.Fail(t, "expected streams to be caught up after all streams got caught up")
	}

	require.NoError(t, coord.startCatchingUp(ctx))

	select {
	case <-coord.streamsCaughtUp():
		require.Fail(t, "expected streams to not be caught up after starting catch up for the second time")
	default:
	}
}

func TestBatchStreamCoordinatorWaitRace(t *testing.T) {
	ctx := context.Background()

	getOpTime := func(_ context.Context) (primitive.Timestamp, error) {
		return primitive.Timestamp{T: 1000}, nil
	}

	bindings := []bindingInfo{
		{resource: resource{Database: "first"}},
	}

	coord := newBatchStreamCoordinator(bindings, getOpTime)

	require.NoError(t, coord.startCatchingUp(ctx))
	doneChan := coord.streamsCaughtUp()
	require.NoError(t, coord.startCatchingUp(ctx))
	require.True(t, coord.gotCaughtUp("first", primitive.Timestamp{T: 1001}))

	select {
	case <-doneChan:
	default:
		require.FailNow(t, "channel not closed")
	}
}
