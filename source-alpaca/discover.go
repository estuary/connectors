package main

import (
	"context"
	"encoding/json"
	"fmt"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	documentSchema, err := schemagen.GenerateSchema("", &tickDocument{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating document schema: %w", err)
	}

	resourceJSON, err := json.Marshal(resource{
		Name:    "trades",
		Feed:    cfg.Feed,
		Symbols: cfg.Symbols,
	})
	if err != nil {
		return nil, fmt.Errorf("serializing resource json: %w", err)
	}

	return &pc.DiscoverResponse{
		Bindings: []*pc.DiscoverResponse_Binding{{
			RecommendedName:    "trades",
			ResourceSpecJson:   resourceJSON,
			DocumentSchemaJson: documentSchema,
			KeyPtrs:            []string{"/ID", "/Symbol", "/Exchange", "/Timestamp"},
		}},
	}, nil
}
