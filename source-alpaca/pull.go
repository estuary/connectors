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
	BackfilledUntil    map[boilerplate.StateKey]time.Time `json:"bindingStateV1,omitempty"`
	OldBackfilledUntil map[string]time.Time               `json:"backfilledUntil,omitempty"` // TODO(whb): Remove once all captures have migrated.
}

func migrateState(state *captureState, bindings []*pf.CaptureSpec_Binding) (bool, error) {
	if state.BackfilledUntil != nil && state.OldBackfilledUntil != nil {
		return false, fmt.Errorf("application error: both BackfilledUntil and OldBackfilledUntil were non-nil")
	} else if state.BackfilledUntil != nil {
		log.Info("skipping state migration since it's already done")
		return false, nil
	}

	state.BackfilledUntil = make(map[boilerplate.StateKey]time.Time)

	for _, b := range bindings {
		if b.StateKey == "" {
			return false, fmt.Errorf("state key was empty for binding %s", b.ResourcePath)
		}

		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return false, fmt.Errorf("parsing resource config: %w", err)
		}

		ll := log.WithFields(log.Fields{
			"stateKey": b.StateKey,
			"name":     res.Name,
		})

		stateFromOld, ok := state.OldBackfilledUntil[res.Name]
		if !ok {
			// This may happen if the connector has never emitted any checkpoints.
			ll.Warn("no state found for binding while migrating state")
			continue
		}

		state.BackfilledUntil[boilerplate.StateKey(b.StateKey)] = stateFromOld
		ll.Info("migrated binding state")
	}

	state.OldBackfilledUntil = nil

	return true, nil
}

type resourceState struct {
	stateKey     boilerplate.StateKey
	startDate    time.Time
	bindingIndex int
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	log.Debug("connector started")

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint captureState
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &checkpoint); err != nil {
			return fmt.Errorf("parsing driver checkpoint: %w", err)
		}
	}

	migrated, err := migrateState(&checkpoint, open.Capture.Bindings)
	if err != nil {
		return err
	}

	resourceStates := make(map[string]*resourceState)
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		resourceState := &resourceState{
			stateKey:     boilerplate.StateKey(binding.StateKey),
			bindingIndex: idx,
			startDate:    cfg.StartDate,
		}

		// If we have persisted a checkpoint indicating progress for this resource, use that instead
		// of the configured startDate.
		if got, ok := checkpoint.BackfilledUntil[boilerplate.StateKey(binding.StateKey)]; ok {
			resourceState.startDate = got
			log.WithFields(log.Fields{
				"Name":      res.Name,
				"StartDate": resourceState.startDate,
			}).Info("set resource StartDate from checkpoint")
		}

		resourceStates[res.Name] = resourceState
	}

	if err := stream.Ready(false); err != nil {
		return err
	}

	if migrated {
		if cp, err := json.Marshal(checkpoint); err != nil {
			return fmt.Errorf("error serializing checkpoint %q: %w", checkpoint, err)
		} else if err := stream.Checkpoint(cp, false); err != nil {
			return fmt.Errorf("updating migrated checkpoint: %w", err)
		}
	}

	var capture = &capture{
		stream:         stream,
		config:         cfg,
		state:          checkpoint,
		resourceStates: resourceStates,
	}
	return capture.Run()
}

type capture struct {
	stream         *boilerplate.PullOutput
	config         config
	state          captureState
	resourceStates map[string]*resourceState
}

func (c *capture) Run() error {
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
		flowStream:   c.stream,
		bindingIdx:   r.bindingIndex,
		dataClient:   dataClient,
		streamClient: streamClient,
		resourceName: name,
		symbols:      c.config.GetSymbols(),
		feed:         c.config.Feed,
		freePlan:     c.config.Advanced.IsFreePlan,
		stateKey:     r.stateKey,
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
