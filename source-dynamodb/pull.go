package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// Limit on how recent a change event can be per its ApproximateCreationDateTime to allow it to
	// be captured while streaming during a backfill.
	backfillStreamingHorizon = 1 * time.Hour

	// Default number of items to evaluate for each backfill scan request for tables with relatively
	// low provisioned read capacity units.
	lowRcuScanLimit = 100

	// Delay between checks of the shard topology for the streaming process.
	defaultShardMonitorDelay = 5 * time.Second

	// Number of backfill segments that will be used per table.
	defaultBackfillSegments = 100

	// Time to spend for each period of backfilling a table.
	backfillDuration = 1 * time.Hour

	// Number of concurrent workers to use when backfilling a table.
	backfillConcurrency = 3
)

type table struct {
	tableName      string
	bindingIdx     int
	keyFields      []string // Ordered as partition key followed by sort key (if there is a sort key)
	activeSegments chan int

	effectiveRcus       int
	scanLimitFromConfig int
	shardMonitorDelay   time.Duration

	totalBackfillSegments int
	streamArn             string
	backfillComplete      bool
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	ctx := stream.Context()

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.toClient(ctx)
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}

	var checkpoint captureState
	if open.StateJson != nil && !reflect.DeepEqual(open.StateJson, json.RawMessage("{}")) {
		if err := json.Unmarshal(open.StateJson, &checkpoint); err != nil {
			return fmt.Errorf("parsing driver checkpoint: %w", err)
		}
	} else {
		// Capture has never emitted a checkpoint.
		checkpoint.Tables = make(map[string]tableState)
	}

	c := capture{
		client: client,
		stream: stream,
		config: cfg,
		state:  checkpoint,
	}

	activeBindings := make(map[string]struct{})
	tables := []*table{}
	for idx, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}

		activeBindings[res.Table] = struct{}{}

		t, err := c.initializeTable(ctx, idx, res.Table, res.RcuAllocation)
		if err != nil {
			return fmt.Errorf("initializing table: %w", err)
		}

		tables = append(tables, t)
	}

	if err := stream.Ready(false); err != nil {
		return err
	}

	eg, groupCtx := errgroup.WithContext(ctx)

	// Remove tables from the persisted state that are no longer included as bindings in the
	// catalog. This allows for removing and re-adding an individual binding to restart a backfill
	// of a table from scratch.
	removedBindings := false
	for checkpointTable := range c.state.Tables {
		if _, ok := activeBindings[checkpointTable]; !ok {
			log.WithField("table", checkpointTable).Info("table removed from catalog")
			delete(c.state.Tables, checkpointTable)
			removedBindings = true
		}
	}
	if removedBindings {
		if err := c.checkpoint(); err != nil {
			return fmt.Errorf("updating checkpoint for removed tables: %w", err)
		}
	}

	for _, table := range tables {
		table := table
		eg.Go(func() error {
			return c.captureTable(groupCtx, table)
		})
	}

	// TODO: If we error here, do a catch-up read on all streams?
	return eg.Wait()
}
