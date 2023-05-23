package main

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	log "github.com/sirupsen/logrus"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-pinecone/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

const (
	defaultInputProjectionName      = "input"
	textEmbeddingAda002             = "text-embedding-ada-002"
	textEmbeddingAda002VectorLength = 1536
)

type config struct {
	Index          string `json:"index" jsonschema:"title=Pinecone Index" jsonschema_extras:"order=0"`
	Environment    string `json:"environment" jsonschema:"title=Pinecone Environment" jsonschema_extras:"order=1"`
	PineconeApiKey string `json:"pineconeApiKey" jsonschema:"title=Pinecone API Key" jsonschema_extras:"secret=true,order=2"`
	OpenAiApiKey   string `json:"openAiApiKey" jsonschema:"title=OpenAI API Key" jsonschema_extras:"secret=true,order=3"`
	EmbeddingModel string `json:"embeddingModel,omitempty" jsonschema:"title=Embedding Model ID,default=text-embedding-ada-002" jsonschema_extras:"order=4"`
	OpenAiOrg      string `json:"openAiOrg,omitempty" jsonschema:"title=OpenAI Organization" jsonschema_extras:"order=5"`
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

	return client.NewOpenAiClient(selectedModel, c.OpenAiOrg, c.OpenAiApiKey)
}

type resource struct {
	Namespace       string `json:"namespace" jsonschema:"title=Pinecone Namespace" jsonschema_extras:"x-collection-name=true"`
	InputProjection string `json:"inputProjection,omitempty" jsonschema:"title=Input Projection Name,default=input"`
}

func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Namespace":
		return "Name of the Pinecone namespace that this collection will materialize vectors into."
	case "InputProjection":
		return "Alternate name of the collection projection to use as input for creating the vector embedding. Defaults to 'input'."
	default:
		return ""
	}
}

func (r resource) inputProjectionName() string {
	out := defaultInputProjectionName
	if r.InputProjection != "" {
		out = r.InputProjection
	}
	return out
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

	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}
		inputProjectionName := res.inputProjectionName()

		var foundInput bool
		foundProjections := []string{}

		constraints := make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range b.Collection.Projections {
			foundProjections = append(foundProjections, projection.Field)

			var constraint = new(pm.Response_Validated_Constraint)
			switch {
			case projection.Field == inputProjectionName:
				if !reflect.DeepEqual(projection.Inference.Types, []string{"string"}) {
					return nil, fmt.Errorf(
						"%q must reference a string projection in collection %q: found types %s",
						inputProjectionName,
						b.Collection.Name.String(),
						projection.Inference.Types,
					)
				} else if projection.Inference.Exists != pf.Inference_MUST {
					return nil, fmt.Errorf(
						"%q cannot reference nullable projection in collection %q",
						inputProjectionName,
						b.Collection.Name.String(),
					)
				}
				foundInput = true

				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = fmt.Sprintf("The %q projection must be materialized", inputProjectionName)

			case isPossibleMetadata(&projection):
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "The projection can be materialized as metadata"
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				constraint.Reason = fmt.Sprintf("Pinecone only materializes %q and compatible metadata", inputProjectionName)
			}
			constraints[projection.Field] = constraint
		}

		if !foundInput {
			return nil, fmt.Errorf("a projection named %q does not exist in collection %q: found projections %s", inputProjectionName, b.Collection.Name.String(), foundProjections)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.Namespace},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

// See https://docs.pinecone.io/docs/metadata-filtering#supported-metadata-types for Pinecone's
// supported metadata types. String, numeric (number or integer), and bool are supported. Lists of
// strings (arrays) are also supported but we do not currently have the `items` property available
// from projection inference as far as I can tell.
func isPossibleMetadata(p *pf.Projection) bool {
	var types []string

	// Remove "null" from the list of types, since metadata fields can be nullable.
	for _, t := range p.Inference.Types {
		if t != pf.JsonTypeNull {
			types = append(types, t)
		}
	}

	// Only single types are permitted.
	if len(types) != 1 {
		return false
	}

	return types[0] == pf.JsonTypeString ||
		types[0] == pf.JsonTypeInteger ||
		types[0] == pf.JsonTypeNumber ||
		types[0] == pf.JsonTypeBoolean
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

		inputProjectionName := res.inputProjectionName()
		if inputProjectionName != defaultInputProjectionName {
			log.WithFields(log.Fields{
				"collection":     b.Collection.Name.String(),
				"projectionName": inputProjectionName,
			}).Info("using alternate input projection name")
		}

		inputIdx := -1
		metaHeaders := []string{}
		for i, f := range b.FieldSelection.AllFields() {
			if f == inputProjectionName {
				inputIdx = i
			}
			metaHeaders = append(metaHeaders, f)
		}
		if inputIdx == -1 {
			return nil, nil, fmt.Errorf("did not find %q in fields for collection %q", inputProjectionName, b.Collection.Name.String())
		}

		bindings = append(bindings, binding{
			namespace:   res.Namespace,
			inputIdx:    inputIdx,
			metaHeaders: metaHeaders,
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
