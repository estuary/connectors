package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/slack-go/slack"
	"go.gazette.dev/core/consumer/protocol"
)

// driver implements the pm.DriverServer interface.
type driver struct{}

type config struct {
	Credentials CredentialConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"x-oauth2-provider=slack"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if err := c.Credentials.validateClientCreds(); err != nil {
		return err
	}
	return nil
}

func (c config) buildAPI() (*SlackAPI, error) {
	var api = c.Credentials.SlackAPI()
	if err := api.AuthTest(); err != nil {
		return nil, fmt.Errorf("error verifying authentication: %w", err)
	}
	return api, nil
}

type resource struct {
	Channel      string            `json:"channel" jsonschema:"title=Channel,description=The name of the channel to post messages to (or a raw channel ID like \"id:C123456\")"`
	SenderConfig SlackSenderConfig `json:"sender_config" jsonschema:"title=Configure Appearance"`
}

func (r resource) Validate() error {
	if r.Channel == "" {
		return fmt.Errorf("missing required channel name/id")
	}
	return nil
}

type driverCheckpoint struct {
	LastMessage        map[string]time.Time `json:"last_message"`
	BindingCollections []string             `json:"binding_collections"`
}

func (c driverCheckpoint) Validate() error {
	return nil
}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	log.Debug("handling Spec request")

	endpointSchema, err := schemagen.GenerateSchema("Slack Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Slack Channel", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-slack",
		Oauth2:                   Spec("channels:read", "groups:read", "im:read", "channels:join", "chat:write", "chat:write.customize"),
		//	                      Spec("channels:history", "channels:join", "channels:read", "files:read", "groups:read", "links:read", "reactions:read", "remote_files:read", "team:read", "usergroups:read", "users.profile:read", "users:read"),
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	log.Debug("handling Validate request")

	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, err
	}

	var api, err = cfg.buildAPI()
	if err != nil {
		return nil, err
	}

	var out []*pm.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		var channelInfo, err = api.ConversationInfo(res.Channel)
		if err != nil {
			return nil, fmt.Errorf("error getting channel: %s, %w", res.Channel, err)
		}

		if !channelInfo.IsMember {
			if err := api.JoinChannel(res.Channel); err != nil {
				return nil, fmt.Errorf("error joining channel: %s, %w", res.Channel, err)
			}
		}

		/*
			projections:
				blocks: /slack_blocks

			`blocks` is a FIELD
			`/slack_blocks` is a LOCATION
		*/

		var constraints = make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range binding.Collection.Projections {
			constraints[projection.Field] = &pm.Response_Validated_Constraint{
				Type:   pm.Response_Validated_Constraint_FIELD_OPTIONAL,
				Reason: "All fields other than 'ts', 'text', and 'blocks' will be ignored",
			}
		}
		constraints["ts"] = &pm.Response_Validated_Constraint{
			Type:   pm.Response_Validated_Constraint_FIELD_REQUIRED,
			Reason: "The Slack materialization requires a message timestamp",
		}

		for _, v := range binding.Collection.Key {
			if v != "ts" {
				constraints[v] = &pm.Response_Validated_Constraint{
					Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
					Reason: "All fields other than 'ts', 'text', and 'blocks' will be ignored",
				}
			}
		}

		constraints["text"] = &pm.Response_Validated_Constraint{
			Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
			Reason: "The Slack materialization requires either text or blocks",
		}
		constraints["blocks"] = &pm.Response_Validated_Constraint{
			Type:   pm.Response_Validated_Constraint_LOCATION_RECOMMENDED,
			Reason: "The Slack materialization requires either text or blocks",
		}
		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.Channel},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	log.Debug("handling Apply request")
	return &pm.Response_Applied{ActionDescription: "materialize-slack does not modify channels"}, nil
}

// Transactions implements the DriverServer interface.
func (driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	log.Debug("handling Transactions request")

	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint driverCheckpoint
	if err := pf.UnmarshalStrict(open.StateJson, &checkpoint); err != nil {
		return nil, nil, fmt.Errorf("parsing driver checkpoint: %w", err)
	}
	if checkpoint.LastMessage == nil {
		checkpoint.LastMessage = make(map[string]time.Time)
	}

	api, err := cfg.buildAPI()
	if err != nil {
		return nil, nil, err
	}

	var bindings []*binding
	for _, b := range open.Materialization.Bindings {
		//Keys/Values are the NAMES
		var fields = append(append([]string{}, b.FieldSelection.Keys...), b.FieldSelection.Values...)

		if checkpoint.LastMessage[string(b.Collection.Name)+b.ResourcePath[0]].IsZero() {
			checkpoint.LastMessage[string(b.Collection.Name)+b.ResourcePath[0]] = time.Now()
		}

		var fieldIndices = make(map[string]int)
		for idx, field := range fields {
			fieldIndices[field] = idx
		}

		tsIndex, ok := fieldIndices["ts"]
		if !ok {
			return nil, nil, fmt.Errorf("no index found for field 'ts' in %q binding", b.ResourcePath[0])
		}
		var textIndex, textOk = fieldIndices["text"]
		var blocksIndex, blocksOk = fieldIndices["blocks"]
		if !(textOk || blocksOk) {
			return nil, nil, fmt.Errorf("no index found for fields 'text' or 'blocks' in %q binding", b.ResourcePath[0])
		}
		var res resource
		err = json.Unmarshal(b.ResourceConfigJson, &res)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to parse resource config: %w", err)
		}
		bindings = append(bindings, &binding{
			resource:    res,
			collection:  string(b.Collection.Name),
			tsIndex:     tsIndex,
			textIndex:   textIndex,
			blocksIndex: blocksIndex,
		})
	}

	var transactor = &transactor{
		api:         api,
		lastMessage: checkpoint.LastMessage,
		bindings:    bindings,
	}

	return transactor, &pm.Response_Opened{}, nil
}

type transactor struct {
	api         *SlackAPI
	bindings    []*binding
	lastMessage map[string]time.Time
}

type binding struct {
	resource    resource
	collection  string
	tsIndex     int
	textIndex   int
	blocksIndex int
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("Load should never be called for materialize-slack")
	}
	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	log.Debug("handling Store operation")

	var vals []tuple.TupleElement
	for it.Next() {
		// Key and Values are the resolved values of the pointers defined by projections
		// Theoretically in the same order as FieldSelection.Keys and .Values
		vals = append(append(vals[:0], it.Key...), it.Values...)

		var b = t.bindings[it.Binding]

		log.WithField("binding", fmt.Sprintf("%#v", b)).WithField("vals", fmt.Sprintf("%#v", vals)).Debug("storing document")

		// Extract timestamp and message fields from the document
		tsStr, ok := vals[b.tsIndex].(string)
		if !ok {
			return nil, fmt.Errorf("timestamp field is not a string, instead got %#v", vals[b.tsIndex])
		}
		var text, textExists = vals[b.textIndex].(string)
		var blocks, blocksExists = vals[b.blocksIndex].([]byte)
		if !(textExists || blocksExists) {
			return nil, fmt.Errorf("text and blocks fields missing, instead got text:%#v, blocks:%#v", vals[b.textIndex], vals[b.blocksIndex])
		}

		// Parse the timstamp as a time.Time
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp %q", tsStr)
		}

		var blocksParsed slack.Blocks
		if blocksExists {
			// Is this jank? This should really be taken care of by the
			// serialization logic in slack.Block, but it can't be _that_
			// bad to do it here... right? see below:
			// https://api.slack.com/reference/surfaces/formatting#escaping
			var escaped_blocks = string(blocks)
			escaped_blocks = strings.ReplaceAll(escaped_blocks, "&", "&amp;")
			escaped_blocks = strings.ReplaceAll(escaped_blocks, "<", "&lt;")
			escaped_blocks = strings.ReplaceAll(escaped_blocks, ">", "&gt;")

			err = json.Unmarshal([]byte(escaped_blocks), &blocksParsed)

			if err != nil {
				log.Warn(fmt.Sprintf("Here is the field ordering: %#v", vals))
				return nil, fmt.Errorf("invalid blocks value %q: %w", string(blocks), err)
			}

		}

		if err := t.api.PostMessage(b.resource.Channel, text, blocksParsed.BlockSet, b.resource.SenderConfig); err != nil {
			serializedBlocks, marshal_err := json.Marshal(blocksParsed)
			if marshal_err == nil {
				log.Warn(fmt.Sprintf("Parse Blocks: %+v", string(serializedBlocks)))
			}
			log.Warn(fmt.Errorf("error sending message: %w", err))
		}
		t.lastMessage[b.collection+b.resource.Channel] = ts
		// Mom can we get rate limiting?
		// we have rate limiting at home
		time.Sleep(time.Second)
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		log.Debug("handling Prepare operation")
		var checkpoint = driverCheckpoint{LastMessage: t.lastMessage}
		var bs, err = json.Marshal(&checkpoint)
		if err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("error marshalling driver checkpoint: %w", err))
		}

		return &pf.ConnectorState{
			UpdatedJson: json.RawMessage(bs),
			MergePatch:  true,
		}, nil
	}, nil
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
