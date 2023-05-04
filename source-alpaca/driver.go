package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
)

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Source Alpaca Spec", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Resource", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-alpaca",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Name},
		})
	}

	// Validate that a connection can be made to Alpaca API for the specified feed. We need to
	// provide a symbol for the test request, so just use the first one from the list of symbols for
	// the capture. Validation ensures that there will be at least one symbol in the list.

	// Query an arbitrary time in the past so that we don't have to worry about the "free plan"
	// option which cannot query within the last 15 minutes.
	testTime := time.Now().Add(-1 * time.Hour)
	_, err := marketdata.NewClient(marketdata.ClientOpts{
		ApiKey:    cfg.ApiKeyID,
		ApiSecret: cfg.ApiSecretKey,
	}).GetTrades(cfg.GetSymbols()[0], marketdata.GetTradesParams{Feed: cfg.Feed, Start: testTime, End: testTime})

	if err != nil {
		return nil, fmt.Errorf("error when connecting to feed %s: %w", cfg.Feed, err)
	}

	return &pc.Response_Validated{Bindings: out}, nil
}

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	documentSchema, err := schemagen.GenerateSchema("", &tradeDocument{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating document schema: %w", err)
	}

	resourceJSON, err := json.Marshal(resource{
		Name: cfg.Feed,
	})
	if err != nil {
		return nil, fmt.Errorf("serializing resource json: %w", err)
	}

	return &pc.Response_Discovered{
		Bindings: []*pc.Response_Discovered_Binding{{
			RecommendedName:    pf.Collection(cfg.Feed),
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: documentSchema,
			Key:                []string{"/ID", "/Symbol", "/Exchange", "/Timestamp"},
		}},
	}, nil
}

func (d driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{
		ActionDescription: "",
	}, nil
}
