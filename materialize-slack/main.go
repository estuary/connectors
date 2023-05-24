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
	Credentials CredentialConfig `json:"credentials" jsonschema:"required,title=Authentication" jsonschema_extras:"x-oauth2-provider=slack"`
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
	BindingCollections []string `json:"binding_collections"`
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
			Type:   pm.Response_Validated_Constraint_LOCATION_REQUIRED,
			Reason: "The Slack materialization requires a message timestamp",
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

	api, err := cfg.buildAPI()
	if err != nil {
		return nil, nil, err
	}

	var bindings []*binding
	for _, b := range open.Materialization.Bindings {
		var res resource
		err = json.Unmarshal(b.ResourceConfigJson, &res)
		if err != nil {
			return nil, nil, fmt.Errorf("unable to parse resource config: %w", err)
		}
		bindings = append(bindings, &binding{
			resource:   res,
			collection: string(b.Collection.Name),
			spec:       b,
		})
	}

	var transactor = &transactor{
		api:      api,
		bindings: bindings,
	}

	return transactor, &pm.Response_Opened{}, nil
}

type transactor struct {
	api      *SlackAPI
	bindings []*binding
}

type binding struct {
	spec       *pf.MaterializationSpec_Binding
	resource   resource
	collection string
}

func buildDocument(b *binding, keys, values tuple.Tuple) map[string]interface{} {
	var document = make(map[string]interface{})

	// Add the keys to the document.
	for i, value := range keys {
		if i < len(b.spec.FieldSelection.Keys) {
			var propName = b.spec.FieldSelection.Keys[i]
			document[propName] = value
		}
	}

	// Add the non-keys to the document.
	for i, value := range values {
		if i < len(b.spec.FieldSelection.Values) {
			var propName = b.spec.FieldSelection.Values[i]

			if raw, ok := value.([]byte); ok {
				document[propName] = json.RawMessage(raw)
			} else {
				document[propName] = value
			}
		}
	}
	return document
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
		var b = t.bindings[it.Binding]
		var parsed = buildDocument(b, it.Key, it.Values)
		var tsStr, tsOk = parsed["ts"].(string)
		var text, textOk = parsed["text"].(string)
		var blocks, blocksOk = parsed["blocks"].([]byte)

		if !tsOk {
			return nil, fmt.Errorf("missing timestamp")
		}

		if !textOk {
			return nil, fmt.Errorf("missing text")
		}

		// Parse the timstamp as a time.Time
		ts, err := time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp %q", tsStr)
		}

		var blocksParsed slack.Blocks
		if blocksOk {
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

		// Accept messages from at most 30 minutes in the past
		if time.Since(ts).Minutes() < 30 {
			if err := t.api.PostMessage(b.resource.Channel, text, blocksParsed.BlockSet, b.resource.SenderConfig); err != nil {
				serializedBlocks, marshal_err := json.Marshal(blocksParsed)
				if marshal_err == nil {
					log.Warn(fmt.Sprintf("Parse Blocks: %+v", string(serializedBlocks)))
				}
				log.Warn(fmt.Errorf("error sending message: %w", err))
			}
			// Mom can we get rate limiting?
			// we have rate limiting at home
			// rate limiting at home:
			time.Sleep(time.Second * 10)
		} else {
			log.Warn(fmt.Sprintf("Ignoring message from the past: %q", ts))
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		log.Debug("handling Prepare operation")
		var checkpoint = driverCheckpoint{}
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
