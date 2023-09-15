package main

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type oplogRecord struct {
	Ts primitive.Timestamp `bson:"ts"`
}

const tsProperty = "ts"

// Oplog time difference: the time difference is measured as the
// difference between the latest and the oldest record in a MongoDB oplog, and
// it is used as a measure of how long do oplog records stay in the oplog.
// Note that this value can fluctuate depending on the density of oplog
// records during a period, so continuously monitoring this value is necessary
// and a single value is not representative
func OplogTimeDifference(ctx context.Context, client *mongo.Client) (uint32, error) {
	var db = client.Database("local")
	var oplog = db.Collection("oplog.rs")

	// get latest record
	var opts = options.FindOne().SetSort(bson.D{{tsProperty, SortDescending}})
	var latest oplogRecord
	if err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&latest); err != nil {
		return 0, fmt.Errorf("querying latest oplog record: %w", err)
	}

	// get oldest record
	opts = options.FindOne().SetSort(bson.D{{tsProperty, SortAscending}})
	var oldest oplogRecord
	if err := oplog.FindOne(ctx, bson.D{}, opts).Decode(&oldest); err != nil {
		return 0, fmt.Errorf("querying oldest oplog record: %w", err)
	}

	return latest.Ts.T - oldest.Ts.T, nil
}
