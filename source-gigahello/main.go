package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/encoding/json"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

var (
	// TestShutdownAfter is a test flag which, if set to a positive value, will cause the
	// capture to shut down after that many messages have been sent.
	TestShutdownAfter = 0

	// FinalStateCheckpoint is the final state checkpoint when the capture shuts down.
	FinalStateCheckpoint json.RawMessage
)

func main() {
	boilerplate.RunMain(new(driver))
}

type config struct {
	Rate float32 `json:"rate" jsonschema:"title=Message Rate,description=Message rate in MBps,default=1.0"`
}

func (c config) Validate() error {
	if c.Rate <= 0 && c.Rate != -1 {
		return fmt.Errorf("message rate must be positive or -1 (got %f)", c.Rate)
	}
	return nil
}

type resource struct {
	Name    string `json:"name" jsonschema:"title=Stream Name,description=Stream name to include in generated documents,default=greetings"`
	Message string `json:"message" jsonschema:"title=Message,description=A text payload to include in each document,default=Hello world!"`
	RawJSON string `json:"raw,omitempty" jsonschema:"title=Raw JSON Payload,description=An optional JSON value to include in each document"`
}

func (r resource) Validate() error {
	return nil
}

type message struct {
	Sequence int64  `json:"seq" jsonschema:"title=Sequence Number,description=The sequence number of this message"`
	Message  string `json:"message" jsonschema:"title=Message,description=A human-readable message"`

	RawJSON string `json:"raw" jsonschema:"totle=Raw JSON Payload,description=A JSON payload"`

	escaped json.RawMessage // Escaped JSON representation of the message.
}

func (m *message) AppendJSON(buf []byte) ([]byte, error) {
	buf = append(buf, `{"seq":`...)
	buf = strconv.AppendInt(buf, m.Sequence, 10)
	buf = append(buf, `,"message":`...)
	if m.escaped == nil {
		m.escaped = json.Escape(m.Message)
	}
	buf = append(buf, m.escaped...)
	if m.RawJSON != "" {
		buf = append(buf, `,"raw":`...)
		buf = append(buf, m.RawJSON...)
	}
	return append(buf, '}'), nil
}

type capture struct {
	Stream   *boilerplate.PullOutput
	Bindings []binding
	Config   config
	State    state
}

type state struct {
	Streams    map[boilerplate.StateKey]*bindingState `json:"bindingStateV1,omitempty"` // The last message (sequence number) generated for each binding.
	StartTime  int64
	LatestTime int64
	TotalBytes uint64
}

type bindingState struct {
	Counter int64 `json:"counter"` // The last message (sequence number) generated for this binding.
}

type binding struct {
	res      resource
	stateKey boilerplate.StateKey
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("GigaHello", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("GigaHello Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-gigahello",
		ResourcePathPointers:     []string{"/name"},
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
	messageSchema, err := schemagen.GenerateSchema("GigaHello Output Document", &message{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating message schema: %w", err)
	}

	var resourceName = "greetings"
	resourceJSON, err := json.Marshal(resource{Name: resourceName, Message: "Hello, world!"})
	if err != nil {
		return nil, fmt.Errorf("serializing resource json: %w", err)
	}

	return &pc.Response_Discovered{
		Bindings: []*pc.Response_Discovered_Binding{{
			RecommendedName:    "messages",
			ResourceConfigJson: resourceJSON,
			DocumentSchemaJson: messageSchema,
			Key:                []string{"/seq"},
		}},
	}, nil
}

func (d driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{
		ActionDescription: "this is a dummy capture so nothing happened",
	}, nil
}

func (driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
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
	if checkpoint.Streams == nil {
		checkpoint.Streams = make(map[boilerplate.StateKey]*bindingState)
	}

	var bindings []binding
	for _, b := range open.Capture.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		var stateKey = boilerplate.StateKey(b.StateKey)
		bindings = append(bindings, binding{
			res:      res,
			stateKey: stateKey,
		})
		if checkpoint.Streams[stateKey] == nil {
			checkpoint.Streams[stateKey] = &bindingState{Counter: 0}
		}
	}

	var capture = &capture{
		Stream:   stream,
		Bindings: bindings,
		Config:   cfg,
		State:    checkpoint,
	}
	return capture.Run()
}

func (c *capture) Run() error {
	if c.State.StartTime == 0 {
		c.State.StartTime = time.Now().UnixNano()
	}
	if err := c.Stream.Ready(false); err != nil {
		return err
	}

	// Rate limiter in bytes per second, with a 1MiB burst size.
	var limiter *rate.Limiter
	if c.Config.Rate > 0 {
		limiter = rate.NewLimiter(rate.Limit(c.Config.Rate)*1024*1024, 1024*1024)
	}

	// Reusable output allocations
	var reused struct {
		msg message              // Reusable object for generating change events.
		buf []byte               // Reusable buffer for serializing documents.
		out pc.Response          // Reusable object for emitting change events.
		doc pc.Response_Captured // Reusable object for emitting change events.
	}
	reused.buf = make([]byte, 0, 1024)

	const checkpointInterval = 50 * time.Millisecond
	var checkpointAfter = time.Now().Add(checkpointInterval)

	// State pointers in binding index order, to skip a map lookup
	var orderedStates = make([]*bindingState, len(c.Bindings))
	for i, binding := range c.Bindings {
		orderedStates[i] = c.State.Streams[binding.stateKey]
	}

	var nextBinding = 0 // Index of the next binding to emit.
	var messageCount int
	var totalBytes = c.State.TotalBytes

	log.WithField("eventType", "connectorStatus").Info("Sending messages")
	var ctx = c.Stream.Context()
	var err error
	for ctx.Err() == nil {
		// Fetch the next binding and its state.
		var binding = c.Bindings[nextBinding]
		var state = orderedStates[nextBinding]
		var seq = state.Counter
		state.Counter++

		// Construct another message and append it to the batch
		if len(reused.buf) > 0 {
			reused.buf = append(reused.buf, '\n')
		}
		reused.msg.Sequence = seq
		reused.msg.Message = binding.res.Message
		reused.msg.RawJSON = binding.res.RawJSON
		reused.buf, err = reused.msg.AppendJSON(reused.buf)
		if err != nil {
			return fmt.Errorf("error serializing message: %w", err)
		}
		messageCount++

		// Check if we've hit the (test only) shutdown threshold
		var shuttingDown = false
		if TestShutdownAfter > 0 && messageCount >= TestShutdownAfter {
			log.WithField("eventType", "shutdown").Infof("Shutting down after %d messages", messageCount)
			shuttingDown = true
		}

		// Determine whether a checkpoint is warranted
		var shouldCheckpoint bool
		if messageCount%1000 == 0 {
			shouldCheckpoint = time.Now().After(checkpointAfter)
		}

		// This is kind of hacky and won't work right if there are
		// multiple bindings, but for a quick experiment it'll be fine.
		if len(reused.buf) > 256*1024 || shuttingDown {
			// Wait for rate limiter
			if limiter != nil {
				if err = limiter.WaitN(c.Stream.Context(), len(reused.buf)); err != nil {
					return fmt.Errorf("error waiting for rate limiter: %w", err)
				}
			}

			// Emit document
			reused.doc.Binding = uint32(nextBinding)
			reused.doc.DocJson = reused.buf
			reused.out.Captured = &reused.doc
			if err = c.Stream.Send(&reused.out); err != nil {
				return fmt.Errorf("error sending document: %w", err)
			}
			totalBytes += uint64(len(reused.buf))

			// Clear buffer for reuse
			reused.buf = reused.buf[:0]
		}

		// Emit checkpoints periodically
		if shouldCheckpoint || shuttingDown {
			c.State.TotalBytes = totalBytes
			c.State.LatestTime = time.Now().UnixNano()
			checkpointJsonBytes, err := json.Marshal(c.State)
			if err != nil {
				return fmt.Errorf("error serializing state: %w", err)
			} else if err := c.Stream.Checkpoint(checkpointJsonBytes, true); err != nil {
				return err
			}
			FinalStateCheckpoint = checkpointJsonBytes
			checkpointAfter = time.Now().Add(checkpointInterval)
		}

		// If we're shutting down, return early here
		if shuttingDown {
			return nil
		}

		// Cycle to the next binding
		nextBinding++
		if nextBinding >= len(c.Bindings) {
			nextBinding = 0
		}
	}
	return nil
}
