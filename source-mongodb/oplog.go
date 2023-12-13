package main

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type serverStatus struct {
	OplogTruncation oplogTruncation `bson:"oplogTruncation"`
}

type oplogTruncation struct {
	OplogMinRetentionHours int `bson:"oplogMinRetentionHours"`
}

// oplogMinRetentionHours queries for the configured minimum retention period of the oplog.
// Note that this can fail due to lack of permissions, even when we have sufficient permissions
// to read the oplog.
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

// checkOplog ensures that we can read from the oplog, and does some best-effort checks of
// the oplog retention.
func checkOplog(ctx context.Context, client *mongo.Client) (time.Time, error) {
	var retention, err = oplogMinRetentionHours(ctx, client)
	if err != nil {
		// We may not have permission to check the oplog retention period, and that's ok.
		// We want to avoid the Info log being interpreted as an error by users
		log.WithField("error", err).Debug("oplog retention check failed (this is usually OK)")
		log.Infof("unable to determine oplog retention period, so assuming it is sufficient")
	} else if retention == 0 {
		// Not all users will have the power to change this setting, so trying not to be too
		// forcefull in recommending that it is set.
		log.Infof("oplog does not have a minimum retention period set. It is recommended that production servers use a minimum retention period of at least 24 hours")
	} else if retention < minOplogTimediffHours {
		log.Infof("oplog retention period is lower than 24 hours, please consider setting your oplog minimum retention period to a minimum of 24 hours, and ideally more: https://go.estuary.dev/NurkrE")
	}

	// Try to determine the timestamp of the oldest entry in the oplog. This serves two purposes:
	// One, it ensures that we have sufficient permissions to read the oplog. Two, we can use it
	// as a "goodenuf" proxy for the configured retention period in case we weren't able to
	// determine that.
	oldestEntryTime, err := oldestOplogEntry(ctx, client)
	if err != nil && err != mongo.ErrNoDocuments {
		return oldestEntryTime, fmt.Errorf("unable to query the MongoDB oplog. Please ensure that the user has access to read the 'local' database and query the 'oplog.rs' collection: %w", err)
	} else if err == mongo.ErrNoDocuments {
		// If this is a shared instance, such as an entry-level Atlas server, this is normal.
		log.Warn("no readable entries found in the MongoDB oplog (this may be normal if capturing from a shared server such as Atlas M0)")
	} else if retention == 0 && time.Now().UTC().Sub(oldestEntryTime).Seconds() < minOplogTimediffSeconds {
		log.Warn(fmt.Sprintf("the age of the oldest readable entry in the oplog is %s, which may indicate that the oplog size is too small to ensure data consistency. This is an approximation and might not be representative of your oplog size. Please ensure your oplog is sufficiently large to be able to safely capture data from your database: https://go.estuary.dev/NurkrE", time.Now().UTC().Sub(oldestEntryTime)))
	}

	return oldestEntryTime, nil
}

// oldestOplogEntry queries the oplog for the oldest entry it contains. It returns
// a zero-valued time and `mongo.ErrNoDocuments` if the oplog contains no entries.
func oldestOplogEntry(ctx context.Context, client *mongo.Client) (time.Time, error) {
	var db = client.Database("local")
	var oplog = db.Collection("oplog.rs")

	var opts = options.FindOne().SetSort(bson.D{{NaturalSort, SortAscending}})
	var oldest oplogRecord
	var err = oplog.FindOne(ctx, bson.D{}, opts).Decode(&oldest)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(oldest.Ts.T), 0).UTC(), err
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
		return fmt.Errorf("oplog's oldest record is for %s, but the last checkpoint requires reading changes since %s. This is usually due to a small oplog storage available. Please resize your oplog to be able to safely capture data from your database: https://go.estuary.dev/NurkrE. The connector will now attempt to backfill all bindings again.", oldestTs.String(), ts.String())
	}

	return nil
}
