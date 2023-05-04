package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/pubsub"
	google_auth "github.com/estuary/connectors/go/auth/google"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"google.golang.org/api/option"
)

const (
	IDENTIFIER_ATTRIBUTE_KEY = "identifier"
)

type config struct {
	ProjectID   string                        `json:"project_id" jsonschema:"title=Google Cloud Project ID"`
	Credentials *google_auth.CredentialConfig `json:"credentials" jsonschema:"title=Authentication"`
}

func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "ProjectID":
		return "Name of the project containing the PubSub topics for this materialization."
	default:
		return ""
	}
}

func (c *config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("missing project ID")
	}

	return c.Credentials.Validate()
}

func (c *config) client(ctx context.Context) (*pubsub.Client, error) {
	creds, err := c.Credentials.GoogleCredentials(ctx, pubsub.ScopePubSub)
	if err != nil {
		return nil, fmt.Errorf("creating pubsub client: %w", err)
	}

	client, err := pubsub.NewClient(
		ctx,
		c.ProjectID,
		option.WithCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("creating pubsub client: %w", err)
	}

	return client, err
}

type resource struct {
	TopicName                 string `json:"topic" jsonschema:"title=Topic Name" jsonschema_extras:"x-collection-name=true"`
	Identifier                string `json:"identifier,omitempty" jsonschema:"title=Resource Binding Identifier"`
	CreateDefaultSubscription bool   `json:"create_default_subscription" jsonschema:"title=Create with Default Subscription,default=true"`
}

func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Identifier":
		return "Optional identifier for the resource binding. Each binding must have a unique topic & identifier pair. " +
			fmt.Sprintf("Included as %q attribute in published messages if specified.", IDENTIFIER_ATTRIBUTE_KEY)
	case "TopicName":
		return "Name of the topic to publish materialized results to."
	case "CreateDefaultSubscription":
		return "Create a default subscription when creating the topic. Will be created as \"<topic>-sub\". Has no effect if the topic already exists."
	default:
		return ""
	}
}

func (r resource) Validate() error {
	if r.TopicName == "" {
		return fmt.Errorf("missing topic name")
	}
	return nil
}

func Driver() driver {
	return driver{}
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	es := schemagen.GenerateSchema("Materialize Google PubSub Spec", &config{})
	endpointSchema, err := es.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Google PubSub Topic", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-google-pubsub",
		Oauth2:                   google_auth.Spec(pubsub.ScopePubSub),
	}, nil
}

// Validate verifies that the provided credentials are valid for authentication and specifies the
// constraints for the connector.
func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	// Bindings are uniquely identified by their topic & identifier, but may have duplicated topic
	// names among bindings. Topic names are collected here in a set to be later verified.
	topicNames := make(map[string]struct{})
	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}

		topicNames[res.TopicName] = struct{}{}

		constraints := make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range b.Collection.Projections {
			var constraint = new(pm.Response_Validated_Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				constraint.Reason = "PubSub only materializes the full document"
			}
			constraints[projection.Field] = constraint
		}

		// Include identifier in the resource path if configured.
		resourcePath := []string{res.TopicName}
		if res.Identifier != "" {
			resourcePath = append(resourcePath, res.Identifier)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: resourcePath,
		})
	}

	for t := range topicNames {
		// The topic may or may not exist yet, but we want to make sure this configuration can check
		// without error. This confirms that the provided authentication credentials are valid to
		// check for the existence of a topic.
		_, err = client.Topic(t).Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("pubsub validation error: %w", err)
		}
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

// Apply creates any new topics for the materialization, leaving existing topics as-is.
func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	checkedTopics := make(map[string]struct{})
	var newTopics []resource
	for _, b := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}

		// Do not check a topic that has already been checked.
		if _, ok := checkedTopics[res.TopicName]; ok {
			continue
		}
		checkedTopics[res.TopicName] = struct{}{}

		exists, err := client.Topic(res.TopicName).Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("pubsub apply upsert topic check error: %w", err)
		}

		if !exists {
			newTopics = append(newTopics, res)
		}
	}

	actions := []string{}
	for _, topic := range newTopics {
		var t *pubsub.Topic
		var s *pubsub.Subscription

		if req.DryRun {
			// No-op, but record the action that would have been taken.
			actions = append(actions, fmt.Sprintf("to create topic %s", t.ID()))
			if topic.CreateDefaultSubscription {
				actions = append(actions, fmt.Sprintf("to create subscription %s for topic %s", s.ID(), t.ID()))
			}
		} else {
			t, err = client.CreateTopic(ctx, topic.TopicName)
			if err != nil {
				return nil, fmt.Errorf("pubsub apply create topic error: %w", err)
			}
			actions = append(actions, fmt.Sprintf("created topic %s", t.ID()))

			if topic.CreateDefaultSubscription {
				s, err = client.CreateSubscription(ctx, fmt.Sprintf("%s-sub", topic.TopicName), pubsub.SubscriptionConfig{Topic: t})
				if err != nil {
					return nil, fmt.Errorf("pubsub apply create default subscription: %w", err)
				}
				actions = append(actions, fmt.Sprintf("created subscription %s for topic %s", s.ID(), t.ID()))
			}
		}
	}

	return &pm.Response_Applied{
		ActionDescription: strings.Join(actions, ", "),
	}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg, err = resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, nil, err
	}

	var topicBindings []*topicBinding
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, err
		}
		t := client.Topic(res.TopicName)

		// Have calls to publish block when the number of queued messages exceeds the default limit
		// of 1000, per topic. This bounds the connector memory usage when the store iterator is
		// read from faster than the rate of publishing.
		t.PublishSettings.FlowControlSettings.LimitExceededBehavior = pubsub.FlowControlBlock

		// Allows for the reading of messages in-order with a provided ordering key. See
		// https://cloud.google.com/pubsub/docs/ordering
		t.EnableMessageOrdering = true
		topicBindings = append(topicBindings, &topicBinding{
			identifier: res.Identifier,
			topic:      t,
		})
	}

	return &transactor{
		bindings: topicBindings,
	}, &pm.Response_Opened{}, nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing PubSub config: %w", err)
	}

	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var res = resource{}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing resource config: %w", err)
	}

	return res, nil
}
