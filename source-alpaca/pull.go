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
		res = resourceWithConfigDefaults(res, cfg)

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
			return c.CaptureBinding(ctx, idx, r)
		})
	}

	return eg.Wait()
}

func (c *capture) CaptureBinding(ctx context.Context, bindingIdx int, r resource) error {
	dataClient := marketdata.NewClient(marketdata.ClientOpts{
		ApiKey:    c.Config.ApiKeyID,
		ApiSecret: c.Config.ApiSecretKey,
	})

	streamClient := marketdataStream.NewStocksClient(
		r.Feed,
		marketdataStream.WithCredentials(c.Config.ApiKeyID, c.Config.ApiSecretKey),
	)

	worker := alpacaWorker{
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
			return worker.backfillTrades(ctx, r.startDate, c.Config.Advanced.StopDate, c.Config.effectiveMaxBackfillInterval, c.Config.effectiveMinBackfillInterval, caughtUp)
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
