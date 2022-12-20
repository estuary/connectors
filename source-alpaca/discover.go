package main

import (
	"context"
	"encoding/json"
	"fmt"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	pc "github.com/estuary/flow/go/protocols/capture"
)

func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	documentSchema, err := schemagen.GenerateSchema("", &tickDocument{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating document schema: %w", err)
	}

	return &pc.DiscoverResponse{
		Bindings: []*pc.DiscoverResponse_Binding{{
			RecommendedName:    "trades",
			ResourceSpecJson:   json.RawMessage(`{"name": "trades"}`),
			DocumentSchemaJson: documentSchema,
			KeyPtrs:            []string{"/ID", "/Symbol", "/Exchange", "/Timestamp"},
		}},
	}, nil
}
