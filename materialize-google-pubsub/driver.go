package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/pubsub"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	google_auth "github.com/estuary/connectors/pkg/auth/google"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
	"google.golang.org/api/option"
)

type config struct {
	ProjectID   string                        `json:"project_id" jsonschema:"title=Google Cloud Project ID"`
	Credentials *google_auth.CredentialConfig `json:"credentials" jsonschema:"title=Authentication"`
}

func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "ProjectID":
		return "Name of the project containing the topics for this materialization."
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
	TopicName string `json:"topic" jsonschema:"title=Topic Name"`
}

func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "TopicName":
		return "Name of the topic to publish materialized results to."
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

func (d driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	es := schemagen.GenerateSchema("Materialize Google PubSub Spec", &config{})
	es.ID = "" // Needed for config-encryption to work.
	endpointSchema, err := es.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Google PubSub Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/materialize-google-pubsub",
		Oauth2Spec:             google_auth.Spec(pubsub.ScopePubSub),
	}, nil
}

// Validate verifies that the provided credentials are valid for authentication and specifies the
// constraints for the connector.
func (d driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	cfg, err := resolveEndpointConfig(req.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	var out []*pm.ValidateResponse_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceSpecJson)
		if err != nil {
			return nil, err
		}

		// The topic may or may not exist yet, but we want to make sure this configuration can check
		// without error. This confirms that the provided authentication credentials are valid.
		_, err = client.Topic(res.TopicName).Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("pubsub validation error: %w", err)
		}

		constraints := make(map[string]*pm.Constraint)
		for _, projection := range b.Collection.Projections {
			var constraint = new(pm.Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			default:
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "PubSub only materializes the full document"
			}
			constraints[projection.Field] = constraint
		}

		out = append(out, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.TopicName},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

// ApplyUpsert creates any new topics for the materialization, leaving existing topics as-is.
func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	var newTopics []string
	for _, b := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceSpecJson)
		if err != nil {
			return nil, err
		}

		exists, err := client.Topic(res.TopicName).Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("pubsub apply upsert topic check error: %w", err)
		}

		if !exists {
			newTopics = append(newTopics, res.TopicName)
		}
	}

	action := ""
	if len(newTopics) > 0 {
		action = fmt.Sprintf("created topics: %s", strings.Join(newTopics, ","))
	}

	if !req.DryRun {
		for _, topicName := range newTopics {
			_, err := client.CreateTopic(ctx, topicName)
			if err != nil {
				return nil, fmt.Errorf("pubsub apply create topic error: %w", err)
			}
		}
	}

	return &pm.ApplyResponse{
		ActionDescription: action,
	}, nil
}

// ApplyDelete deletes topics for the materialization. It is idempotent - topics listed in the
// materialization that do not exist do not result in error.
func (d driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	var topicsToDelete []string
	for _, b := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceSpecJson)
		if err != nil {
			return nil, err
		}

		exists, err := client.Topic(res.TopicName).Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("pubsub apply upsert topic check error: %w", err)
		}

		if exists {
			topicsToDelete = append(topicsToDelete, res.TopicName)
		}
	}

	action := ""
	if len(topicsToDelete) > 0 {
		action = fmt.Sprintf("deleted topics: %s", strings.Join(topicsToDelete, ","))
	}

	if !req.DryRun {
		for _, topicName := range topicsToDelete {
			if err := client.Topic(topicName).Delete(ctx); err != nil {
				return nil, fmt.Errorf("pubsub apply delete topic error: %w", err)
			}
		}
	}

	return &pm.ApplyResponse{
		ActionDescription: action,
	}, nil
}

func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	req, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("stream recv: %w", err)
	} else if req.Open == nil {
		return fmt.Errorf("expected Open, got %#v", req)
	}

	cfg, err := resolveEndpointConfig(req.Open.Materialization.EndpointSpecJson)
	if err != nil {
		return err
	}

	client, err := cfg.client(stream.Context())
	if err != nil {
		return err
	}

	var topics []*pubsub.Topic
	for _, b := range req.Open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceSpecJson)
		if err != nil {
			return err
		}
		t := client.Topic(res.TopicName)

		// Allows for the reading of messages in-order with a provided ordering key. See
		// https://cloud.google.com/pubsub/docs/ordering
		t.EnableMessageOrdering = true
		topics = append(topics, t)
	}

	t := &transactor{
		topics: topics,
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: nil},
	}); err != nil {
		logrus.Info("driver transactions - send error: ", err)
		return fmt.Errorf("sending Opened: %w", err)
	}

	log := logrus.WithField("materialization", "google pubsub")
	return pm.RunTransactions(stream, t, log)
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
