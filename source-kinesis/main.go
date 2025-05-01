package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	"golang.org/x/sync/errgroup"
)

type driver struct{}

type resource struct {
	Stream string `json:"stream" jsonschema:"title=Stream Name"`
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

func (d *driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var config Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &config); err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

	client, err := connect(ctx, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	streams, err := listStreams(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}

	var bindings = []*pc.Response_Validated_Binding{}

	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		if !slices.ContainsFunc(streams, func(s kinesisStream) bool { return s.name == res.Stream }) {
			return nil, fmt.Errorf("stream %s does not exist", res.Stream)
		}

		bindings = append(bindings, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Stream},
		})
	}

	return &pc.Response_Validated{Bindings: bindings}, nil
}

func (d *driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var config Config
	if err := pf.UnmarshalStrict(req.ConfigJson, &config); err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

	client, err := connect(ctx, &config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	streams, err := listStreams(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("listing streams: %w", err)
	}

	bindings, err := discoverStreams(streams)
	if err != nil {
		return nil, err
	}

	return &pc.Response_Discovered{Bindings: bindings}, nil
}

func (d *driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	var ctx = stream.Context()
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
		state.Streams = make(map[boilerplate.StateKey]map[string]*string)
	}

	client, err := connect(ctx, &config)
	if err != nil {
		return err
	}

	streams, err := listStreams(ctx, client)
	if err != nil {
		return err
	}

	var c = &capture{
		client:      client,
		stream:      stream,
		updateState: make(map[boilerplate.StateKey]map[string]*string),
		stats:       make(map[string]map[string]shardStats),
	}

	if err := stream.Ready(false); err != nil {
		return err
	}

	group, groupCtx := errgroup.WithContext(ctx)

	for i, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("error parsing resource config: %w", err)
		}

		sk := boilerplate.StateKey(binding.StateKey)
		if _, ok := state.Streams[sk]; !ok {
			state.Streams[sk] = make(map[string]*string)
		}

		streamIdx := slices.IndexFunc(streams, func(s kinesisStream) bool {
			return s.name == res.Stream
		})
		if streamIdx == -1 {
			return fmt.Errorf("stream %s does not exist", res.Stream)
		}

		group.Go(func() error {
			return c.readStream(groupCtx, streams[streamIdx], sk, i, maps.Clone(state.Streams[sk]))
		})
	}
	group.Go(func() error { return c.statsLogger(groupCtx) })

	return group.Wait()
}

func (d *driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

func main() {
	boilerplate.RunMain(new(driver))
}

type captureState struct {
	Streams map[boilerplate.StateKey]map[string]*string `json:"bindingStateV1,omitempty"`
}
