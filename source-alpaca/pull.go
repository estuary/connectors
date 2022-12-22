package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alpacahq/alpaca-trade-api-go/v2/marketdata"
	marketdataStream "github.com/alpacahq/alpaca-trade-api-go/v2/marketdata/stream"
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

type resourceState struct {
	startDate    time.Time
	bindingIndex uint32
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

	resourceStates := make(map[string]*resourceState)
	for idx, binding := range open.Open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		resourceState := &resourceState{
			bindingIndex: uint32(idx),
			startDate:    cfg.StartDate,
		}

		// If we have persisted a checkpoint indicating progress for this resource, use that instead
		// of the configured startDate.
		if got, ok := checkpoint.BackfilledUntil[res.Name]; ok {
			resourceState.startDate = got
			log.WithFields(log.Fields{
				"Name":      res.Name,
				"StartDate": resourceState.startDate,
			}).Info("set resource StartDate from checkpoint")
		}

		resourceStates[res.Name] = resourceState
	}

	var capture = &capture{
		stream:         stream,
		config:         cfg,
		state:          checkpoint,
		output:         &boilerplate.PullOutput{Stream: stream},
		resourceStates: resourceStates,
	}
	return capture.Run()
}

type capture struct {
	stream         pc.Driver_PullServer
	config         config
	state          captureState
	output         *boilerplate.PullOutput
	resourceStates map[string]*resourceState
}

func (c *capture) Run() error {
	// Notify Flow that we're starting.
	if err := c.output.Ready(); err != nil {
		return err
	}

	var eg, ctx = errgroup.WithContext(c.stream.Context())

	// Capture each resource.
	for name, res := range c.resourceStates {
		name, res := name, res // Copy the loop variables for each closure
		eg.Go(func() error {
			return c.captureResource(ctx, name, res)
		})
	}

	return eg.Wait()
}

func (c *capture) captureResource(ctx context.Context, name string, r *resourceState) error {
	dataClient := marketdata.NewClient(marketdata.ClientOpts{
		ApiKey:    c.config.ApiKeyID,
		ApiSecret: c.config.ApiSecretKey,
	})

	streamClient := marketdataStream.NewStocksClient(
		c.config.Feed,
		marketdataStream.WithCredentials(c.config.ApiKeyID, c.config.ApiSecretKey),
	)

	worker := alpacaWorker{
		output:       c.output,
		bindingIdx:   r.bindingIndex,
		dataClient:   dataClient,
		streamClient: streamClient,
		resourceName: name,
		symbols:      c.config.GetSymbols(),
		feed:         c.config.Feed,
		freePlan:     c.config.Advanced.IsFreePlan,
	}

	eg, ctx := errgroup.WithContext(ctx)

	caughtUp := make(chan struct{})

	if !c.config.Advanced.DisableBackfill {
		eg.Go(func() error {
			return worker.backfillTrades(ctx, r.startDate, c.config.Advanced.StopDate, c.config.effectiveMaxBackfillInterval, c.config.effectiveMinBackfillInterval, caughtUp)
		})
	} else {
		// If backfilling is disabled, there's nothing to catch up on. Streaming can start right away.
		close(caughtUp)
	}

	if !c.config.Advanced.DisableRealTime {
		eg.Go(func() error {
			// Wait until the backfilling is caught up (or disabled) before starting streaming.
			// Throughput will most likely be limited by the network or journal append limits. It
			// may be possible that there is such a large amount of data that the backfilling will
			// never catch up. This would be unfortunate, but we wouldn't want to add on event
			// streaming to that as well since it would only make matters worse.
			select {
			case <-caughtUp:
				return worker.streamTrades(ctx)
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	return eg.Wait()
}
