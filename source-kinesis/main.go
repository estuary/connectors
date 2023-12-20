package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"sync"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

type driver struct{}

type resource struct {
	Stream   string `json:"stream" jsonschema:"title=Stream Name"`
	SyncMode string `json:"syncMode" jsonschema:"-"`
}

func (r resource) Validate() error {
	if r.Stream == "" {
		return fmt.Errorf("stream is required")
	}
	return nil
}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Kinesis", &Config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("Kinesis Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-kinesis",
		ResourcePathPointers:     []string{"/stream"},
	}, nil
}

// Validate that store resources and proposed collection bindings are compatible.
func (d *driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var config Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &config); err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

	client, err := connect(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	streamNames, err := listAllStreams(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}

	var bindings = []*pc.Response_Validated_Binding{}

	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		if !slices.Contains(streamNames, res.Stream) {
			return nil, fmt.Errorf("stream %s does not exist", res.Stream)
		}

		bindings = append(bindings, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Stream},
		})
	}

	return &pc.Response_Validated{Bindings: bindings}, nil
}

// Discover returns the set of resources available from this Driver.
func (d *driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var config Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &config); err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

	client, err := connect(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	streamNames, err := listAllStreams(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}

	bindings := discoverStreams(ctx, client, streamNames)

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var config Config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &config); err != nil {
		return fmt.Errorf("parsing config json: %w", err)
	}

	var state captureState
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &state); err != nil {
			return fmt.Errorf("parsing state file: %w", err)
		}
	}
	if state.Streams == nil {
		state.Streams = make(map[boilerplate.StateKey]map[string]string)
	}

	client, err := connect(&config)
	if err != nil {
		err = fmt.Errorf("failed to connect: %w", err)
	}

	var dataCh = make(chan readResult, 8)
	var ctx = stream.Context()
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	log.WithField("bindings count", open.Capture.Bindings).Info("Starting to read stream(s)")

	var shardRange = open.Range
	if shardRange.KeyBegin == 0 && shardRange.KeyEnd == 0 {
		log.Info("using full shard range since no range was given in the catalog")
		shardRange.KeyEnd = math.MaxUint32
	}

	stateKeys := make([]boilerplate.StateKey, len(open.Capture.Bindings))
	var waitGroup = new(sync.WaitGroup)
	for i, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("error parsing resource config: %w", err)
		}

		sk := boilerplate.StateKey(binding.StateKey)
		if _, ok := state.Streams[sk]; !ok {
			state.Streams[sk] = make(map[string]string)
		}
		stateKeys[i] = sk

		waitGroup.Add(1)
		go readStream(ctx, shardRange, client, i, res.Stream, maps.Clone(state.Streams[sk]), dataCh, waitGroup)
	}

	go closeChannelWhenDone(dataCh, waitGroup)

	if err := stream.Ready(false); err != nil {
		return err
	}

	// We're all set to start printing data to stdout
	for next := range dataCh {
		if next.err != nil {
			// time to bail
			log.WithField("error", next.err).Error("read failed due to error")
			err = next.err
			break
		}
		for _, record := range next.records {
			if err = stream.Documents(next.source.bindingIndex, record); err != nil {
				break
			}
		}

		sk := stateKeys[next.source.bindingIndex]
		state.Streams[sk][next.source.shardID] = next.sequenceNumber

		stateRaw, err := json.Marshal(state)
		if err != nil {
			break
		}
		if err = stream.Checkpoint(json.RawMessage(stateRaw), false); err != nil {
			break
		}

		if err != nil {
			break
		}
	}

	return err
}

// ApplyUpsert applies a new or updated capture to the store.
func (d *driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

func main() {
	boilerplate.RunMain(new(driver))
}

func closeChannelWhenDone(dataCh chan readResult, waitGroup *sync.WaitGroup) {
	waitGroup.Wait()
	log.Info("All reads have completed")
	close(dataCh)
}

type captureState struct {
	Streams map[boilerplate.StateKey]map[string]string `json:"bindingStateV1,omitempty"`
}
