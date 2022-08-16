package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

type config struct {
	BearerToken string `json:"token" jsonschema:"title=Bearer Token,description=The Slack API authentication token."`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if c.BearerToken == "" {
		return fmt.Errorf("missing required Slack API authentication token")
	}
	return nil
}

func (c config) buildAPI() (*slackAPI, error) {
	var api = &slackAPI{Token: c.BearerToken}
	if err := api.AuthTest(); err != nil {
		return nil, fmt.Errorf("error verifying authentication: %w", err)
	}
	return api, nil
}

type resource struct {
	Channel string `json:"channel" jsonschema:"title=Channel,description=The name of the channel to post messages to (or a raw channel ID like \"id:C123456\")"`
}

func (r resource) Validate() error {
	if r.Channel == "" {
		return fmt.Errorf("missing required channel name/id")
	}
	return nil
}

type driverCheckpoint struct {
	LastMessage time.Time `json:"last_message"`
}

func (c driverCheckpoint) Validate() error {
	return nil
}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	log.Debug("handling Spec request")

	endpointSchema, err := schemagen.GenerateSchema("Slack Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Slack Channel", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/materialize-slack",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	log.Debug("handling Validate request")

	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, err
	}

	var api, err = cfg.buildAPI()
	if err != nil {
		return nil, err
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		if _, err := api.GetChannelID(res.Channel); err != nil {
			return nil, fmt.Errorf("")
		}

		var constraints = make(map[string]*pm.Constraint)
		for _, projection := range binding.Collection.Projections {
			constraints[projection.Field] = &pm.Constraint{
				Type:   pm.Constraint_FIELD_OPTIONAL,
				Reason: "All fields other than 'ts' and 'message' will be ignored",
			}
		}
		constraints["ts"] = &pm.Constraint{
			Type:   pm.Constraint_LOCATION_REQUIRED,
			Reason: "The Slack materialization requires a message timestamp",
		}
		constraints["message"] = &pm.Constraint{
			Type:   pm.Constraint_LOCATION_REQUIRED,
			Reason: "The Slack materialization requires message text",
		}

		out = append(out, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.Channel},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	log.Debug("handling ApplyUpsert request")
	return &pm.ApplyResponse{ActionDescription: "materialize-slack does not create channels"}, nil
}

func (d driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	log.Debug("handling ApplyDelete request")
	return &pm.ApplyResponse{ActionDescription: "materialize-slack does not delete channels"}, nil
}

// Transactions implements the DriverServer interface.
func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	log.Debug("handling Transactions request")

	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	var cfg config
	if err = pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint driverCheckpoint
	if err = pf.UnmarshalStrict(open.Open.DriverCheckpointJson, &checkpoint); err != nil {
		return fmt.Errorf("parsing driver checkpoint: %w", err)
	}
	if checkpoint.LastMessage.IsZero() {
		checkpoint.LastMessage = time.Now()
	}

	api, err := cfg.buildAPI()
	if err != nil {
		return err
	}

	var bindings []*binding
	for _, b := range open.Open.Materialization.Bindings {
		var fields = append(append([]string{}, b.FieldSelection.Keys...), b.FieldSelection.Values...)
		var fieldIndices = make(map[string]int)
		for idx, field := range fields {
			fieldIndices[field] = idx
		}

		tsIndex, ok := fieldIndices["ts"]
		if !ok {
			return fmt.Errorf("no index found for field 'ts' in %q binding", b.ResourcePath[0])
		}
		msgIndex, ok := fieldIndices["message"]
		if !ok {
			return fmt.Errorf("no index found for field 'message' in %q binding", b.ResourcePath[0])
		}
		bindings = append(bindings, &binding{
			channel:  b.ResourcePath[0],
			tsIndex:  tsIndex,
			msgIndex: msgIndex,
		})
	}

	var transactor = &transactor{
		api:         api,
		lastMessage: checkpoint.LastMessage,
		bindings:    bindings,
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: nil},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	var logEntry = log.WithField("materialization", "slack")
	logEntry.Debug("running transactions")
	return pm.RunTransactions(stream, transactor, logEntry)
}

type transactor struct {
	api         *slackAPI
	bindings    []*binding
	lastMessage time.Time
}

type binding struct {
	channel  string
	tsIndex  int
	msgIndex int
}

func (transactor) Load(it *pm.LoadIterator, priorCommittedCh <-chan struct{}, priorAcknowledgedCh <-chan struct{}, loaded func(binding int, doc json.RawMessage) error) error {
	log.Debug("handling Load operation")
	return fmt.Errorf("this materialization only supports delta-updates")
}

func (t *transactor) Prepare(context.Context, pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	log.Debug("handling Prepare operation")
	var checkpoint = driverCheckpoint{LastMessage: t.lastMessage}
	var bs, err = json.Marshal(&checkpoint)
	if err != nil {
		return pf.DriverCheckpoint{}, fmt.Errorf("error marshalling driver checkpoint: %w", err)
	}
	return pf.DriverCheckpoint{
		DriverCheckpointJson: json.RawMessage(bs),
		Rfc7396MergePatch:    true,
	}, nil
}

func (t *transactor) Store(it *pm.StoreIterator) error {
	log.Debug("handling Store operation")

	var vals []tuple.TupleElement
	for it.Next() {
		// Concatenate key and non-key fields because we don't care
		vals = append(append(vals[:0], it.Key...), it.Values...)

		var b = t.bindings[it.Binding]

		log.WithField("binding", fmt.Sprintf("%#v", b)).WithField("vals", fmt.Sprintf("%#v", vals)).Debug("storing document")

		// Extract timestamp and message fields from the document
		tsStr, ok := vals[b.tsIndex].(string)
		if !ok {
			return fmt.Errorf("timestamp field is not a string, instead got %#v", vals[b.tsIndex])
		}
		msg, ok := vals[b.msgIndex].(string)
		if !ok {
			return fmt.Errorf("message field is not a string, instead got %#v", vals[b.msgIndex])
		}

		// Parse the timstamp as a time.Time
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			return fmt.Errorf("invalid timestamp %q", tsStr)
		}

		if ts.After(t.lastMessage) {
			if err := t.api.PostMessage(b.channel, msg); err != nil {
				return fmt.Errorf("error sending message: %w", err)
			}
			t.lastMessage = ts
		}
	}
	return nil
}

func (transactor) Commit(context.Context) error {
	log.Debug("handling Commit operation")
	return nil
}

func (transactor) Acknowledge(context.Context) error {
	log.Debug("handling Acknowledge operation")
	return nil
}

func (transactor) Destroy() {
	log.Debug("handling Destroy operation")
}

func main() {
	log.SetLevel(log.DebugLevel)
	log.Info("connector starting")
	boilerplate.RunMain(new(driver))
}
