package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	embeddingclient "github.com/estuary/connectors/materialize-qdrant/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	qdrant "github.com/qdrant/go-client/qdrant"
)

const (
	defaultEmbeddingModel     = "text-embedding-3-small"
	defaultEmbeddingDimension = 1536
)

type config struct {
	QdrantHost     string         `json:"qdrantHost" jsonschema:"title=Qdrant Host" jsonschema_extras:"order=0"`
	QdrantPort     int            `json:"qdrantPort,omitempty" jsonschema:"title=Qdrant gRPC Port,default=6334" jsonschema_extras:"order=1"`
	QdrantUseTLS   bool           `json:"qdrantUseTls,omitempty" jsonschema:"title=Use TLS" jsonschema_extras:"order=2"`
	QdrantAPIKey   string         `json:"qdrantApiKey,omitempty" jsonschema:"title=Qdrant API Key" jsonschema_extras:"secret=true,order=3"`
	OpenAIAPIKey   string         `json:"openAiApiKey" jsonschema:"title=OpenAI API Key" jsonschema_extras:"secret=true,order=4"`
	VectorSize     int            `json:"vectorSize,omitempty" jsonschema:"title=Vector Size,default=1536,minimum=1" jsonschema_extras:"order=5"`
	DistanceMetric string         `json:"distanceMetric,omitempty" jsonschema:"title=Distance Metric,default=Cosine,enum=Cosine,enum=Euclid,enum=Dot,enum=Manhattan" jsonschema_extras:"order=6"`
	EmbeddingModel string         `json:"embeddingModel,omitempty" jsonschema:"title=Embedding Model ID,default=text-embedding-3-small" jsonschema_extras:"order=7"`
	Advanced       advancedConfig `json:"advanced,omitempty" jsonschema_extras:"advanced=true"`
}

func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "QdrantHost":
		return "Hostname of the Qdrant server. For Qdrant Cloud, use the cluster hostname without a scheme or port."
	case "QdrantPort":
		return "Qdrant gRPC port. The default is 6334."
	case "QdrantUseTLS":
		return "Use TLS for the Qdrant gRPC connection. Enable this for Qdrant Cloud."
	case "QdrantAPIKey":
		return "Optional Qdrant API key used for authentication."
	case "OpenAIAPIKey":
		return "OpenAI API key used for authentication."
	case "VectorSize":
		return "Number of dimensions in each vector. This value must match the output of the embedding model."
	case "DistanceMetric":
		return "Metric that Qdrant uses to compare vectors."
	case "EmbeddingModel":
		return "OpenAI embedding model ID. The default is text-embedding-3-small."
	case "Advanced":
		return "Optional OpenAI configuration."
	default:
		return ""
	}
}

type advancedConfig struct {
	OpenAIOrg string `json:"openAiOrg,omitempty" jsonschema:"title=OpenAI Organization"`
}

func (advancedConfig) GetFieldDocString(fieldName string) string {
	if fieldName == "OpenAIOrg" {
		return "OpenAI organization for API requests."
	}
	return ""
}

func (c *config) Validate() error {
	if strings.TrimSpace(c.QdrantHost) == "" {
		return fmt.Errorf("endpoint config missing required property 'qdrantHost'")
	}
	if strings.TrimSpace(c.OpenAIAPIKey) == "" {
		return fmt.Errorf("endpoint config missing required property 'openAiApiKey'")
	}

	if c.QdrantPort < 0 || c.QdrantPort > 65535 {
		return fmt.Errorf("endpoint config property 'qdrantPort' must be 0 or between 1 and 65535")
	}
	if c.VectorSize < 0 {
		return fmt.Errorf("endpoint config property 'vectorSize' must be greater than 0")
	}
	if c.embeddingModel() == defaultEmbeddingModel && c.vectorSize() != defaultEmbeddingDimension {
		return fmt.Errorf("endpoint config property 'vectorSize' must be %d for embedding model '%s'", defaultEmbeddingDimension, defaultEmbeddingModel)
	}
	if _, err := c.distance(); err != nil {
		return err
	}
	return nil
}

func (c *config) distance() (qdrant.Distance, error) {
	switch c.DistanceMetric {
	case "", "Cosine":
		return qdrant.Distance_Cosine, nil
	case "Euclid":
		return qdrant.Distance_Euclid, nil
	case "Dot":
		return qdrant.Distance_Dot, nil
	case "Manhattan":
		return qdrant.Distance_Manhattan, nil
	default:
		return qdrant.Distance_UnknownDistance, fmt.Errorf("endpoint config property 'distanceMetric' must be one of Cosine, Euclid, Dot, or Manhattan")
	}
}

func (c *config) vectorSize() int {
	if c.VectorSize == 0 {
		return defaultEmbeddingDimension
	}
	return c.VectorSize
}

func (c *config) qdrantClient() (*qdrant.Client, error) {
	return qdrant.NewClient(&qdrant.Config{
		Host:                   c.QdrantHost,
		Port:                   c.QdrantPort,
		APIKey:                 c.QdrantAPIKey,
		UseTLS:                 c.QdrantUseTLS,
		PoolSize:               uint(concurrentWorkers),
		RetryConfig:            &qdrant.RetryConfig{MaxRetries: 10},
		SkipCompatibilityCheck: true,
	})
}

func (c *config) embeddingModel() string {
	if c.EmbeddingModel == "" {
		return defaultEmbeddingModel
	}
	return c.EmbeddingModel
}

func (c *config) openAIClient() *embeddingclient.OpenAIClient {
	return embeddingclient.NewOpenAIClient(c.embeddingModel(), c.Advanced.OpenAIOrg, c.OpenAIAPIKey)
}

type resource struct {
	Collection string `json:"collection" jsonschema:"title=Qdrant Collection" jsonschema_extras:"x-collection-name=true"`
}

func (resource) GetFieldDocString(fieldName string) string {
	if fieldName == "Collection" {
		return "Name of the Qdrant collection. The connector creates the collection if it does not exist."
	}
	return ""
}

func (r resource) Validate() error {
	if strings.TrimSpace(r.Collection) == "" {
		return fmt.Errorf("missing collection")
	}
	return nil
}

type driver struct{}

func (driver) Spec(_ context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := schemagen.GenerateSchema("Materialize Qdrant Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("Qdrant Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-qdrant",
		Oauth2:                   nil,
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}
	qdrantClient, err := cfg.qdrantClient()
	if err != nil {
		return nil, err
	}
	defer qdrantClient.Close()

	if err := cfg.openAIClient().VerifyModelExists(ctx); err != nil {
		return nil, err
	}

	out := make([]*pm.Response_Validated_Binding, 0, len(req.Bindings))
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}

		_, err = qdrantClient.CollectionExists(ctx, res.Collection)
		if err != nil {
			return nil, fmt.Errorf("checking collection '%s': %w", res.Collection, err)
		}

		constraints := make([]*pm.Response_Validated_ProjectionConstraint, 0, len(b.Collection.Projections))
		for _, projection := range b.Collection.Projections {
			constraint := new(pm.Response_Validated_Constraint)
			switch {
			case projection.IsPrimaryKey:
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "Components of the collection key must be materialized"
			case projection.Inference.IsSingleScalarType():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "The projection has a single scalar type"
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				constraint.Reason = "This field can be materialized"
			}
			constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{
				Field:      projection.Field,
				Constraint: constraint,
			})
		}

		out = append(out, &pm.Response_Validated_Binding{
			CaseInsensitiveFields: false,
			ProjectionConstraints: constraints,
			DeltaUpdates:          true,
			ResourcePath:          []string{res.Collection},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}
	qdrantClient, err := cfg.qdrantClient()
	if err != nil {
		return nil, err
	}
	defer qdrantClient.Close()

	var actions []string
	for _, binding := range req.Materialization.Bindings {
		res, err := resolveResourceConfig(binding.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		created, err := ensureCollection(ctx, qdrantClient, res.Collection, cfg)
		if err != nil {
			return nil, err
		}
		if created {
			actions = append(actions, fmt.Sprintf("created Qdrant collection '%s'", res.Collection))
		}
	}

	return &pm.Response_Applied{ActionDescription: strings.Join(actions, "\n")}, nil
}

func (driver) NewTransactor(ctx context.Context, open pm.Request_Open, _ *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	cfg, err := resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, nil, err
	}
	qdrantClient, err := cfg.qdrantClient()
	if err != nil {
		return nil, nil, nil, err
	}
	keepClient := false
	defer func() {
		if !keepClient {
			_ = qdrantClient.Close()
		}
	}()

	bindings := make([]binding, 0, len(open.Materialization.Bindings))
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, nil, err
		}
		bindings = append(bindings, binding{
			collection:  res.Collection,
			vectorSize:  cfg.vectorSize(),
			dataHeaders: b.FieldSelection.AllFields(),
		})
	}

	keepClient = true
	return &transactor{
		qdrantClient: qdrantClient,
		openAIClient: cfg.openAIClient(),
		bindings:     bindings,
	}, &pm.Response_Opened{}, nil, nil
}

type collectionsClient interface {
	CollectionExists(context.Context, string) (bool, error)
	CreateCollection(context.Context, *qdrant.CreateCollection) error
}

func ensureCollection(ctx context.Context, client collectionsClient, name string, cfg config) (bool, error) {
	exists, err := client.CollectionExists(ctx, name)
	if err != nil {
		return false, fmt.Errorf("checking collection '%s': %w", name, err)
	}

	if exists {
		return false, nil
	}

	distance, err := cfg.distance()
	if err != nil {
		return false, err
	}
	if err := client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: name,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(cfg.vectorSize()),
			Distance: distance,
		}),
	}); err != nil {
		return false, fmt.Errorf("creating collection '%s': %w", name, err)
	}
	return true, nil
}

func resolveEndpointConfig(specJSON json.RawMessage) (config, error) {
	cfg := config{}
	if err := pf.UnmarshalStrict(specJSON, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing endpoint config: %w", err)
	}
	return cfg, nil
}

func resolveResourceConfig(specJSON json.RawMessage) (resource, error) {
	res := resource{}
	if err := pf.UnmarshalStrict(specJSON, &res); err != nil {
		return res, fmt.Errorf("parsing resource config: %w", err)
	}
	return res, nil
}

func main() {
	boilerplate.RunMain(driver{})
}
