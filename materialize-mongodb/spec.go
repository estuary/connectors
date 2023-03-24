package main

import (
	"fmt"
	"context"

	pf "github.com/estuary/flow/go/protocols/flow"
	proto "github.com/gogo/protobuf/proto"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo"
)

const specCollection = "flow_materializations"

type SavedSpec struct {
	Id   string `bson:"uuid"`
	Spec []byte `bson:"spec"`
}

// LoadSpec loads existing spec from spec collection and create a map of it by table name
func (d driver) LoadSpec(ctx context.Context, cfg config, materialization string) (*pf.MaterializationSpec, error) {
	var client, err = d.connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	var collection = client.Database(cfg.Database).Collection(specCollection)

	var s SavedSpec

	err = collection.FindOne(ctx, bson.D{{idField, bson.D{{"$eq", materialization}}}}).Decode(&s)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("could not find spec: %w", err)
	}

	var existing pf.MaterializationSpec
	err = proto.Unmarshal(s.Spec, &existing)
	if err != nil {
		return nil, fmt.Errorf("parsing existing materialization spec: %w", err)
	}

	return &existing, nil
}

func (d driver) WriteSpec(ctx context.Context, cfg config, materialization *pf.MaterializationSpec, version string) error {
	var client, err = d.connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	var collection = client.Database(cfg.Database).Collection(specCollection)

	bs, err := proto.Marshal(materialization)
	if err != nil {
		return fmt.Errorf("encoding existing materialization spec: %w", err)
	}

	opts := options.Replace().SetUpsert(true)
	_, err = collection.ReplaceOne(ctx, bson.D{{idField, string(materialization.Materialization)}}, bson.D{{"spec", bs}}, opts)
	if err != nil {
		return fmt.Errorf("upserting spec: %w", err)
	}

	return nil
}

func (d driver) CleanSpec(ctx context.Context, cfg config, materialization string) error {
	var client, err = d.connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	var collection = client.Database(cfg.Database).Collection(specCollection)

	_, err = collection.DeleteOne(ctx, bson.D{{idField, materialization}})
	if err != nil {
		return fmt.Errorf("cleaning up spec: %w", err)
	}

	return nil
}
