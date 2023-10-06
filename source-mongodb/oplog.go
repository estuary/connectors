package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type serverStatus struct {
	OplogTruncation oplogTruncation `bson:"oplogTruncation"`
}

type oplogTruncation struct {
	OplogMinRetentionHours int `bson:"oplogMinRetentionHours"`
}

func oplogMinRetentionHours(ctx context.Context, client *mongo.Client) (int, error) {
	var db = client.Database("admin")

	var result = db.RunCommand(ctx, bson.M{"serverStatus": "1"})

	var status serverStatus
	if err := result.Decode(&status); err != nil {
		return 0, fmt.Errorf("decoding server status for oplogMinRetentionHours: %w", err)
	}

	return status.OplogTruncation.OplogMinRetentionHours, nil
}

type oplogRecord struct {
	Ts primitive.Timestamp `bson:"ts"`
}

// Oplog time difference: the time difference is measured as the
// difference between the latest and the oldest record in a MongoDB oplog, and
// it is used as a measure of how long do oplog records stay in the oplog.
// Note that this value can fluctuate depending on the density of oplog
// records during a period, so continuously monitoring this value is necessary
// and a single value is not representative
func oplogTimeDifference(ctx context.Context, client *mongo.Client) (uint32, error) {
	var db = client.Database("local")
	var oplog = db.Collection("oplog.rs")

	// get latest record
	var opts = options.FindOne().SetSort(bson.D{{NaturalSort, SortDescending}})
	var latest oplogRecord
	if err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&latest); err != nil {
		return 0, fmt.Errorf("querying latest oplog record: %w", err)
	}

	// get oldest record
	opts = options.FindOne().SetSort(bson.D{{NaturalSort, SortAscending}})
	var oldest oplogRecord
	if err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&oldest); err != nil {
		return 0, fmt.Errorf("querying oldest oplog record: %w", err)
	}

	return latest.Ts.T - oldest.Ts.T, nil
}

// Check whether the timestamp ts is included in the oplog. This check is
// necessary to make sure that when we set StartAtOperationTime, that timestamp
// is actually available in the oplog. If it is not available, the driver will
// not error, and we will lose some events without knowing
func oplogHasTimestamp(ctx context.Context, client *mongo.Client, ts time.Time) error {
	var db = client.Database("local")
	var oplog = db.Collection("oplog.rs")

	// get oldest record
	var opts = options.FindOne().SetSort(bson.D{{NaturalSort, SortAscending}})
	var oldest oplogRecord
	if err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&oldest); err != nil {
		return fmt.Errorf("querying oldest oplog record: %w", err)
	}
	var oldestTs = time.Unix(int64(oldest.Ts.T), 0)

	if oldestTs.After(ts) {
		return fmt.Errorf("oplog's oldest record is for %s, but the last checkpoint requires reading changes since %s. This is usually due to a small oplog storage available. Please resize your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE. The connector will attempt to backfill all bindings.", oldestTs.String(), ts.String())
	}

	return nil
}
