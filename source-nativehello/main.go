package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func main() {
	boilerplate.RunMain(new(driver))
}

type config struct {
	Rate float32 `json:"rate" jsonschema:"title=Message Rate,description=Message rate in documents per second,default=1"`
}

func (c config) Validate() error {
	if c.Rate <= 0 {
		return fmt.Errorf("message rate must be positive (got %f)", c.Rate)
	}
	return nil
}

type resource struct {
	Name   string `json:"name" jsonschema:"title=Stream Name,description=Stream name to include in generated documents,default=greetings"`
	Prefix string `json:"prefix" jsonschema:"title=Message Prefix,description=Constant prefix to add to every message,default=Hello {}"`
}

func (r resource) Validate() error {
	return nil
}

type message struct {
	Timestamp time.Time `json:"ts" jsonschema:"title=Timestamp,description=The time at which this message was generated"`
	Message   string    `json:"message" jsonschema:"title=Message,description=A human-readable message"`
}

type state struct {
	Cursor int `json:"cursor"` // The last message generated
}

type capture struct {
	Stream   pc.Driver_PullServer
	Bindings []resource
	Config   config
	State    state
	Acks     <-chan *pc.Acknowledge
}

// driver implements the pc.DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Example Source Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Example Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/source-nativehello",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
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

func (driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	messageSchema, err := schemagen.GenerateSchema("Example Output Record", &message{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating message schema: %w", err)
	}

	resourceJSON, err := json.Marshal(resource{Name: "greetings", Prefix: "Hello {}!"})
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

func (d driver) ApplyUpsert(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return d.apply(ctx, req, false)
}

func (d driver) ApplyDelete(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return d.apply(ctx, req, true)
}

func (driver) apply(ctx context.Context, req *pc.ApplyRequest, isDelete bool) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{
		ActionDescription: "this is a dummy capture so nothing happened",
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

	var checkpoint state
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
		resources = append(resources, res)
	}

	var capture = &capture{
		Stream:   stream,
		Bindings: resources,
		Config:   cfg,
		State:    checkpoint,
	}
	return capture.Run()
}

func (c *capture) Run() error {
	// Spawn a goroutine which will translate gRPC PullRequest.Acknowledge messages
	// into equivalent messages on a channel. Since the ServerStream interface doesn't
	// have any sort of polling receive, this is necessary to allow captures to handle
	// acknowledgements without blocking their own capture progress.
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
	log.Debug("sending PullResponse.Opened")
	if err := c.Stream.Send(&pc.PullResponse{Opened: &pc.PullResponse_Opened{}}); err != nil {
		return fmt.Errorf("error sending PullResponse.Opened: %w", err)
	}

	eg.Go(func() error {
		return c.Capture(ctx)
	})
	return eg.Wait()
}

func (c *capture) EmitDocuments(binding int, docs []*message) error {
	var arena []byte
	var ranges []pf.Slice
	for _, doc := range docs {
		bs, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("error serializing document: %w", err)
		}

		var begin, end = len(arena), len(arena) + len(bs)
		arena = append(arena, bs...)
		ranges = append(ranges, pf.Slice{Begin: uint32(begin), End: uint32(end)})
	}

	var msg = &pc.PullResponse{
		Documents: &pc.Documents{
			Binding:  uint32(binding),
			Arena:    arena,
			DocsJson: ranges,
		},
	}
	log.WithField("count", len(docs)).Debug("emitting documents")
	if err := c.Stream.Send(msg); err != nil {
		return fmt.Errorf("error sending PullResponse.Documents: %w", err)
	}
	return nil
}

func (c *capture) EmitCheckpoint() error {
	checkpointJSON, err := json.Marshal(c.State)
	if err != nil {
		return fmt.Errorf("error serializing state: %w", err)
	}
	var checkpoint = &pc.PullResponse{
		Checkpoint: &pf.DriverCheckpoint{
			DriverCheckpointJson: checkpointJSON,
			Rfc7396MergePatch:    true,
		},
	}
	log.WithField("checkpoint", string(checkpointJSON)).Debug("emitting checkpoint")
	if err := c.Stream.Send(checkpoint); err != nil {
		return fmt.Errorf("error sending PullResponse.Checkpoint: %w", err)
	}
	return nil
}

func (c *capture) Capture(ctx context.Context) error {
	var interval = time.Duration(float64(time.Second) / float64(c.Config.Rate))
	for {
		select {
		case <-ctx.Done():
			log.Info("shutting down due to context cancellation")
			return nil
		case <-c.Acks:
			log.Debug("received acknowledgement")
		default:
		}

		for idx, binding := range c.Bindings {
			var text = strings.Replace(binding.Prefix, "{}", strconv.Itoa(c.State.Cursor), -1)
			var doc = &message{
				Timestamp: time.Now(),
				Message:   text,
			}
			if err := c.EmitDocuments(idx, []*message{doc}); err != nil {
				return err
			}
		}
		c.State.Cursor++
		if err := c.EmitCheckpoint(); err != nil {
			return err
		}
		time.Sleep(interval)
	}
}
