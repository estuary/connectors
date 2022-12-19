package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	BackfilledUntil map[string]string `json:"backfilledUntil,omitempty"`
}

type driver struct{}

func (driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Alpaca", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Alpaca Resource Spec", &resource{}).MarshalJSON()
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
	// TODO: Validate credentials via connection to Alpaca.

	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pc.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		out = append(out, &pc.ValidateResponse_Binding{
			ResourcePath: []string{res.Name},
		})
	}
	return &pc.ValidateResponse{Bindings: out}, nil
}

// TODO: Discover
type message struct {
	Timestamp time.Time `json:"ts" jsonschema:"title=Timestamp,description=The time at which this message was generated"`
	Message   string    `json:"message" jsonschema:"title=Message,description=A human-readable message"`
}

func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	messageSchema, err := schemagen.GenerateSchema("Example Output Record", &message{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating message schema: %w", err)
	}

	resourceJSON, err := json.Marshal(resource{})
	if err != nil {
		return nil, fmt.Errorf("serializing resource json: %w", err)
	}

	return &pc.DiscoverResponse{
		Bindings: []*pc.DiscoverResponse_Binding{{
			RecommendedName:    "events",
			ResourceSpecJson:   resourceJSON,
			DocumentSchemaJson: messageSchema,
			KeyPtrs:            []string{"/ts"},
		}},
	}, nil
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
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		// If we have persisted a checkpoint indicating progress for this resource, use that instead
		// of the configured startDate.
		if got, ok := checkpoint.BackfilledUntil[res.Name]; ok {
			res.StartDate = got
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
	Acks     <-chan *pc.Acknowledge
	Output   *boilerplate.PullOutput
}

func (c *capture) Run() error {
	// Spawn a goroutine which will translate gRPC PullRequest.Acknowledge messages
	// into equivalent messages on a channel. Since the ServerStream interface doesn't
	// have any sort of polling receive, this is necessary to allow captures to handle
	// acknowledgements without blocking their own capture progress.

	// TODO: I don't know if this is still necessary. Can we opt out of acks yet?
	var acks = make(chan *pc.Acknowledge)
	var eg, ctx = errgroup.WithContext(c.Stream.Context())
	eg.Go(func() error {
		defer close(acks)
		for {
			var msg, err = c.Stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return fmt.Errorf("error reading PullRequest: %w", err)
			} else if msg.Acknowledge == nil {
				return fmt.Errorf("expected PullRequest.Acknowledge, got %#v", msg)
			}
			select {
			case <-ctx.Done():
				return nil
			case acks <- msg.Acknowledge:
				// Loop and receive another acknowledgement
			}
		}
	})
	c.Acks = acks

	// Notify Flow that we're starting.
	if err := c.Output.Ready(); err != nil {
		return err
	}

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
	// Here we need to start up the backfiller as well as the streamer.

	// Initialize the backfiller.
	dataClient := marketdata.NewClient(marketdata.ClientOpts{
		ApiKey:    c.Config.ApiKey,
		ApiSecret: c.Config.ApiSecret,
	})

	// Initialize the streamer.
	streamClient := marketdataStream.NewStocksClient(
		r.Feed,
		marketdataStream.WithCredentials(c.Config.ApiKey, c.Config.ApiSecret),
	)

	// Make the client.
	client := alpacaClient{
		output:       c.Output,
		bindingIdx:   bindingIdx,
		dataClient:   dataClient,
		streamClient: streamClient,
		symbols:      r.GetSymbols(),
		feed:         r.Feed,
		currency:     r.Currency,
		freePlan:     c.Config.IsFreePlan,
	}

	// Run it.
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		// TODO: This could probably be moved up.
		// TODO: Time format.
		parsed, err := time.Parse(time.RFC3339Nano, r.StartDate)
		if err != nil {
			return err
		}
		return client.doBackfill(ctx, parsed, maxInterval, minInterval)
	})

	eg.Go(func() error {
		return client.doStream(ctx)
	})

	return eg.Wait()
}
