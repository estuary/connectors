package main

import (
	"context"
	"encoding/json"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-pinecone/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

const (
	textEmbeddingAda002             = "text-embedding-ada-002"
	textEmbeddingAda002VectorLength = 1536
)

type config struct {
	Index          string         `json:"index" jsonschema:"title=Pinecone Index" jsonschema_extras:"order=0"`
	Environment    string         `json:"environment" jsonschema:"title=Pinecone Environment" jsonschema_extras:"order=1"`
	PineconeApiKey string         `json:"pineconeApiKey" jsonschema:"title=Pinecone API Key" jsonschema_extras:"secret=true,order=2"`
	OpenAiApiKey   string         `json:"openAiApiKey" jsonschema:"title=OpenAI API Key" jsonschema_extras:"secret=true,order=3"`
	EmbeddingModel string         `json:"embeddingModel,omitempty" jsonschema:"title=Embedding Model ID,default=text-embedding-ada-002" jsonschema_extras:"order=4"`
	Advanced       advancedConfig `json:"advanced,omitempty" jsonschema_extras:"advanced=true"`
}

func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Index":
		return "Pinecone index for this materialization. Must already exist and have appropriate dimensions for the embedding model used."
	case "Environment":
		return "Cloud region for your Pinecone project. Example: us-central1-gcp"
	case "PineconeApiKey":
		return "Pinecone API key used for authentication."
	case "OpenAiApiKey":
		return "OpenAI API key used for authentication."
	case "EmbeddingModel":
		return "Embedding model ID for generating OpenAI bindings. The default text-embedding-ada-002 is recommended."
	case "Advanced":
		return "Options for advanced users. You should not typically need to modify these."
	default:
		return ""
	}
}

type advancedConfig struct {
	OpenAiOrg string `json:"openAiOrg,omitempty" jsonschema:"title=OpenAI Organization"`
}

func (advancedConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "OpenAiOrg":
		return "Optional organization name for OpenAI requests. Use this if you belong to multiple organizations to specify which organization is used for API requests."
	default:
		return ""
	}
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"index", c.Index},
		{"environment", c.Environment},
		{"pineconeApiKey", c.PineconeApiKey},
		{"openAiApiKey", c.OpenAiApiKey},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("endpoint config missing required property '%s'", req[0])
		}
	}

	return nil
}

func (c *config) pineconeClient(ctx context.Context) (*client.PineconeClient, error) {
	return client.NewPineconeClient(ctx, c.Index, c.Environment, c.PineconeApiKey)
}

func (c *config) openAiClient() *client.OpenAiClient {
	selectedModel := textEmbeddingAda002
	if c.EmbeddingModel != "" {
		selectedModel = c.EmbeddingModel
	}

	return client.NewOpenAiClient(selectedModel, c.Advanced.OpenAiOrg, c.OpenAiApiKey)
}

type resource struct {
	Namespace string `json:"namespace" jsonschema:"title=Pinecone Namespace" jsonschema_extras:"x-collection-name=true"`
}

func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Namespace":
		return "Name of the Pinecone namespace that this collection will materialize vectors into."
	default:
		return ""
	}
}

func (r resource) Validate() error {
	var requiredProperties = [][]string{
		{"namespace", r.Namespace},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("resource config missing required property '%s'", req[0])
		}
	}

	return nil
}

type driver struct{}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	es := schemagen.GenerateSchema("Materialize Pinecone Spec", &config{})
	endpointSchema, err := es.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Pinecone Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-pinecone",
		Oauth2:                   nil,
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}

	// Validate connectivity and that the index exists and is appropriately dimensioned.
	pc, err := cfg.pineconeClient(ctx)
	if err != nil {
		return nil, err
	}
	if indexStats, err := pc.DescribeIndexStats(ctx); err != nil {
		return nil, fmt.Errorf("connecting to Pinecone: %w", err)
	} else if cfg.EmbeddingModel == textEmbeddingAda002 && indexStats.Dimension != textEmbeddingAda002VectorLength {
		return nil, fmt.Errorf(
			"index '%s' has dimensions of %d but must be %d for embedding model '%s'",
			cfg.Index,
			indexStats.Dimension,
			textEmbeddingAda002VectorLength,
			textEmbeddingAda002,
		)
	} else if err := cfg.openAiClient().Ping(ctx); err != nil {
		return nil, fmt.Errorf("connecting to OpenAI: %w", err)
	}

	// Log a warning message if the 'flow_document' metadata field has not been excluded from
	// metadata indexing. This is a high cardinality field and is potentially large, and such fields
	// are not recommended to be indexed.
	indexDescribe, err := pc.DescribeIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("describing index: %w", err)
	}
	entry := log.WithFields(log.Fields{
		"index":       cfg.Index,
		"environment": cfg.Environment,
	})
	if indexDescribe.Database.MetadataConfig.Indexed == nil {
		// If no explicit metadata configuration for which fields are indexed has been provided all fields are indexed.
		entry.Warn("Metadata field 'flow_document' will be indexed since this index is not configured with selective metadata indexing. Consider using selective metadata indexing to prevent this field from being indexed to optimize memory utilization.")
	} else if slices.Contains(indexDescribe.Database.MetadataConfig.Indexed, "flow_document") {
		entry.Warn("Metadata field 'flow_document' will be indexed. This may not result in optimal memory utilization for the index.")
	}

	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}

		constraints := make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range b.Collection.Projections {

			var constraint = new(pm.Response_Validated_Constraint)
			switch {
			// We require collection keys be materialized because it seems pretty reasonable to
			// require they be included as metadata since the composite key is used as the basis for
			// the vector ID, and also to avoid complications from
			// https://github.com/estuary/flow/issues/1057.
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
				constraint.Reason = "This field can be materializaed"
			}
			constraints[projection.Field] = constraint
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.Namespace},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	// No-op since namespaces are automatically created when a vector is added for that namespace.
	return &pm.Response_Applied{
		ActionDescription: "",
	}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg, err = resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	var bindings []binding
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, err
		}

		bindings = append(bindings, binding{
			namespace:   res.Namespace,
			dataHeaders: b.FieldSelection.AllFields(),
		})
	}

	pc, err := cfg.pineconeClient(ctx)
	if err != nil {
		return nil, nil, err
	}

	log.WithFields(log.Fields{
		"index":       cfg.Index,
		"environment": cfg.Environment,
	}).Info("starting materialize-pinecone")

	return &transactor{
		pineconeClient: pc,
		openAiClient:   cfg.openAiClient(),
		bindings:       bindings,
	}, &pm.Response_Opened{}, nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing endpoint config: %w", err)
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

func main() {
	boilerplate.RunMain(driver{})
}
