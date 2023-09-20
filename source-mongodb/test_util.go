package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
)

var (
	address   = flag.String("address", "mongodb://localhost", "MongoDB instance address")
	user      = flag.String("user", "flow", "Username")
	password  = flag.String("password", "flow", "Password")
	database  = flag.String("database", "test", "Database name")
)

func testClient(t *testing.T) (*mongo.Client, config) {
	t.Helper()
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return nil, config{}
	}

	ctx := context.Background()

	config := config{
		Address: *address,
		User: *user,
		Password: *password,
		Database: *database,
	}

	var d = driver{}
	client, err := d.Connect(ctx, config)
	require.NoError(t, err)

	return client, config
}

func dropCollection(ctx context.Context, t *testing.T, client *mongo.Client, database string, collection string) {
	t.Helper()

	var db = client.Database(database)
	var col = db.Collection(collection)
	err := col.Drop(ctx)

	require.NoError(t, err)
}

func addTestTableData(
	ctx context.Context,
	t *testing.T,
	c *mongo.Client,
	database string,
	collection string,
	numItems int,
	startAtItem int,
	pkData func(int) any,
	cols ...string,
) {
	var db = c.Database(database)
	var col = db.Collection(collection)

	for idx := startAtItem; idx < startAtItem+numItems; idx++ {
		item := make(map[string]any)
		item["_id"] = pkData(idx)

		for _, col := range cols {
			item[col] = fmt.Sprintf("%s val %d", col, idx)
		}

		log.WithField("data", item).WithField("col", collection).Info("inserting data")
		_, err := col.InsertOne(ctx, item)
		require.NoError(t, err)
	}
}

func deleteData(
	ctx context.Context,
	t *testing.T,
	c *mongo.Client,
	database string,
	collection string,
	id any,
) {
	var db = c.Database(database)
	var col = db.Collection(collection)

	_, err := col.DeleteOne(ctx, map[string]any{"_id": id})

	require.NoError(t, err)
}

func updateData(
	ctx context.Context,
	t *testing.T,
	c *mongo.Client,
	database string,
	collection string,
	id any,
	cols ...string,
) {
	var db = c.Database(database)
	var col = db.Collection(collection)

	var item = make(map[string]any)
	for _, col := range cols {
		item[col] = fmt.Sprintf("%s val %d", col, 0)
	}

	_, err := col.UpdateOne(ctx, map[string]any{"_id": id}, map[string]any{ "$set": item })

	require.NoError(t, err)
}
