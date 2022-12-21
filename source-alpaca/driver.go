package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata"
	marketdataStream "github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type captureState struct {
	// Mapping of binding names to how far along they have read.
	BackfilledUntil map[string]time.Time `json:"backfilledUntil,omitempty"`
}

type driver struct{}

func (driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Source Alpaca Spec", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Trade Document", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/source-alpaca",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pc.ValidateResponse_Binding
	feeds := make(map[string][]string)
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.ValidateResponse_Binding{
			ResourcePath: []string{res.Name},
		})

		feeds[res.Feed] = res.GetSymbols()
	}

	// Validate that a connection can be made to Alpaca API for all of the specified feeds. We need
	// to provide a symbol for the test request, so just use the first one from the list of symbols
	// for the binding. Validation ensures that there will be at least one symbol in the list.

	// Query an arbitrary time in the past so that we don't have to worry about the "free plan"
	// option which cannot query within the last 15 minutes.
	testTime := time.Now().Add(-1 * time.Hour)
	for feed, symbols := range feeds {
		_, err := marketdata.NewClient(marketdata.ClientOpts{
			ApiKey:    cfg.ApiKey,
			ApiSecret: cfg.ApiSecret,
		}).GetTrades(symbols[0], marketdata.GetTradesParams{Feed: feed, Start: testTime, End: testTime})

		if err != nil {
			return nil, fmt.Errorf("error when connecting to feed %s: %w", feed, err)
		}
	}

	return &pc.ValidateResponse{Bindings: out}, nil
}

func (driver) Pull(stream pc.Driver_PullServer) error {
	log.Debug("connector started")

	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("error reading PullRequest: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected PullRequest.Open, got %#v", open)
	}

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Capture.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint captureState
	if open.Open.DriverCheckpointJson != nil {
		if err := json.Unmarshal(open.Open.DriverCheckpointJson, &checkpoint); err != nil {
			return fmt.Errorf("parsing driver checkpoint: %w", err)
		}
	}

	var resources []resource
	for _, binding := range open.Open.Capture.Bindings {
		// Default values from the endpoint config will be overridden by anything from the resource
		// spec.
		res := resource{
			Feed:    cfg.Feed,
			Symbols: cfg.Symbols,
		}
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		res.startDate = cfg.StartDate
		// If we have persisted a checkpoint indicating progress for this resource, use that instead
		// of the configured startDate.
		if got, ok := checkpoint.BackfilledUntil[res.Name]; ok {
			res.startDate = got
			log.WithFields(log.Fields{
				"Name":      res.Name,
				"StartDate": res.startDate,
			}).Info("set resource StartDate from checkpoint")
		}

		resources = append(resources, res)
	}

	var capture = &capture{
		Stream:   stream,
		Bindings: resources,
		Config:   cfg,
		State:    checkpoint,
		Output:   &boilerplate.PullOutput{Stream: stream},
	}
	return capture.Run()
}

type capture struct {
	Stream   pc.Driver_PullServer
	Bindings []resource
	Config   config
	State    captureState
	Output   *boilerplate.PullOutput
}

func (c *capture) Run() error {
	// Notify Flow that we're starting.
	if err := c.Output.Ready(); err != nil {
		return err
	}

	var eg, ctx = errgroup.WithContext(c.Stream.Context())

	// Capture each binding.
	for idx, r := range c.Bindings {
		idx, r := idx, r // Copy the loop variables for each closure
		eg.Go(func() error {
			return c.Capture(ctx, idx, r)
		})
	}

	return eg.Wait()
}

func (c *capture) Capture(ctx context.Context, bindingIdx int, r resource) error {
	dataClient := marketdata.NewClient(marketdata.ClientOpts{
		ApiKey:    c.Config.ApiKey,
		ApiSecret: c.Config.ApiSecret,
	})

	streamClient := marketdataStream.NewStocksClient(
		r.Feed,
		marketdataStream.WithCredentials(c.Config.ApiKey, c.Config.ApiSecret),
	)

	client := alpacaClient{
		output:       c.Output,
		bindingIdx:   bindingIdx,
		dataClient:   dataClient,
		streamClient: streamClient,
		resourceName: r.Name,
		symbols:      r.GetSymbols(),
		feed:         r.Feed,
		freePlan:     c.Config.Advanced.IsFreePlan,
	}

	eg, ctx := errgroup.WithContext(ctx)

	caughtUp := make(chan struct{})

	if !c.Config.Advanced.DisableBackfill {
		eg.Go(func() error {
			return client.doBackfill(ctx, r.startDate, c.Config.Advanced.StopDate, c.Config.Advanced.MaxBackfillInterval, c.Config.Advanced.MinBackfillInterval, caughtUp)
		})
	} else {
		// If backfilling is disabled, there's nothing to catch up on. Streaming can start right away.
		close(caughtUp)
	}

	if !c.Config.Advanced.DisableRealTime {
		eg.Go(func() error {
			// Wait until the backfilling is caught up (or disabled) before starting streaming.
			// Throughput will most likely be limited by the network or journal append limits. It
			// may be possible that there is such a large amount of data that the backfilling will
			// never catch up. This would be unfortunate, but we wouldn't want to add on event
			// streaming to that as well since it would only make matters worse.
			<-caughtUp
			return client.doStream(ctx)
		})
	}

	return eg.Wait()
}
