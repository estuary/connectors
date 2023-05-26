package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
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
	Stream   *boilerplate.PullOutput
	Bindings []resource
	Config   config
	State    state
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("Hello World", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Example Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-hello-world",
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
	return &pc.Response_Validated{Bindings: out}, nil
}

func (driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	messageSchema, err := schemagen.GenerateSchema("Example Output Record", &message{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating message schema: %w", err)
	}

	resourceJSON, err := json.Marshal(resource{Name: "greetings", Prefix: "Hello {}!"})
	if err != nil {
		return nil, fmt.Errorf("serializing resource json: %w", err)
	}

	return &pc.Response_Discovered{
		Bindings: []*pc.Response_Discovered_Binding{{
			RecommendedName:    "events",
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: messageSchema,
			Key:                []string{"/ts"},
		}},
	}, nil
}

func (d driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{
		ActionDescription: "this is a dummy capture so nothing happened",
	}, nil
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	log.Debug("connector started")

	var cfg config
	if err := pf.UnmarshalStrict(open.Capture.ConfigJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint state
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &checkpoint); err != nil {
			return fmt.Errorf("parsing driver checkpoint: %w", err)
		}
	}

	var resources []resource
	for _, binding := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
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
	if err := c.Stream.Ready(false); err != nil {
		return err
	}

	var interval = time.Duration(float64(time.Second) / float64(c.Config.Rate))
	for {
		select {
		case <-c.Stream.Context().Done():
			log.Info("shutting down due to context cancellation")
			return nil
		default:
		}

		for idx, binding := range c.Bindings {
			var text = strings.Replace(binding.Prefix, "{}", strconv.Itoa(c.State.Cursor), -1)
			var doc = &message{
				Timestamp: time.Now(),
				Message:   text,
			}
			bs, err := json.Marshal(doc)
			if err != nil {
				return fmt.Errorf("error serializing document: %w", err)
			} else if err := c.Stream.Documents(idx, json.RawMessage(bs)); err != nil {
				return err
			}
		}
		c.State.Cursor++

		checkpointJsonBytes, err := json.Marshal(c.State)
		if err != nil {
			return fmt.Errorf("error serializing state: %w", err)
		} else if err := c.Stream.Checkpoint(checkpointJsonBytes, true); err != nil {
			return err
		}
		time.Sleep(interval)
	}
}
