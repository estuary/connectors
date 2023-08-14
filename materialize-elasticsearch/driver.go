package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// credentials is a union representing either an api key or a username/password.
// It's allowed for all credentials to be missing, which is used for connecting to servers
// without authentication enabled.
type credentials struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	ApiKey   string `json:"apiKey,omitempty"`
}

type config struct {
	Credentials credentials `json:"credentials"`
	Endpoint    string      `json:"endpoint"`
}

// The `go-schema-gen` package doesn't have a good way of dealing with oneOf, and I couldn't get it
// to output a schema that works with Flow's UI. So the endpoint config schema is written out manually
// instead of being generated from the structs.
func configSchema() json.RawMessage {
	var schemaStr = `{
  "$schema": "http://json-schema.org/draft/2020-12/schema",
  "$id": "https://github.com/estuary/connectors/materialize-elasticsearch/config",
  "properties": {
    "endpoint": {
      "type": "string",
      "title": "Endpoint",
      "description": "Endpoint host or URL. If using Elastic Cloud this follows the format https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT",
      "order": 0
    },
    "credentials": {
      "type": "object",
      "oneOf": [
        {
          "type": "object",
          "title": "Username and Password",
          "properties": {
            "username": {
              "type": "string",
              "title": "Username",
              "description": "Username to use with the elasticsearch API"
            },
            "password": {
              "type": "string",
              "secret": true,
              "title": "Password",
              "description": "Password for the user"
            }
          },
          "required": [
            "username",
            "password"
          ]
        },
        {
          "type": "object",
          "title": "API Key",
          "properties": {
            "apiKey": {
              "type": "string",
              "secret": true,
              "title": "API key",
              "description": "API key for authenticating with the elasticsearch API"
            }
          },
          "required": [
            "apiKey"
          ]
        }
      ]
    }
  },
  "type": "object",
  "required": [
    "endpoint"
  ],
  "title": "Elasticsearch Connection"
}`
	return json.RawMessage([]byte(schemaStr))
}

func (c *credentials) Validate() error {
	if (c.Username == "") != (c.Password == "") {
		return fmt.Errorf("username and password must both be present if one of them is")
	}
	return nil
}

func (c config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("missing Endpoint")
	}
	return c.Credentials.Validate()
}

type resource struct {
	Index         string `json:"index" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates  bool   `json:"delta_updates" jsonschema:"default=false"`
	NumOfShards   int    `json:"number_of_shards,omitempty" jsonschema:"default=1"`
	NumOfReplicas int    `json:"number_of_replicas,omitempty" jsonschema:"default=0"`
}

func (r resource) Validate() error {
	if r.Index == "" {
		return fmt.Errorf("missing Index")
	}

	if r.NumOfShards <= 0 {
		return fmt.Errorf("number_of_shards is missing or non-positive")
	}
	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Index":
		return "Name of the ElasticSearch index to store the materialization results."
	case "DeltaUpdates":
		return "Should updates to this table be done via delta updates. Default is false."
	case "NumOfShards":
		return "The number of shards in ElasticSearch index. Must be greater than 0."
	case "NumOfReplicas":
		return "The number of replicas in ElasticSearch index. If not set, default to be 0. " +
			"For single-node clusters, this must be 0. For production systems, a value of 1 or more is recommended"
	default:
		return ""
	}
}

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpointSchema = configSchema()

	resourceSchema, err := schemagen.GenerateSchema("Elasticsearch Index", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         endpointSchema,
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-elasticsearch",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	elasticSearch, err := newElasticsearchClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	// TODO(whb): Do more comprehensive connection verifications.

	storedSpec, err := elasticSearch.getSpec(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("getting spec: %w", err)
	}

	var out []*pm.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		constraints, err := validateBinding(res, binding.Collection, storedSpec)
		if err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.DeltaUpdates,
			ResourcePath: []string{res.Index},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var elasticSearch, err = newElasticsearchClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	actions := []string{}
	addAction := func(a string) {
		if a != "" {
			actions = append(actions, "- "+a)
		}
	}

	desc, err := elasticSearch.createMetaIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating metadata index")
	}
	addAction(desc)

	storedSpec, err := elasticSearch.getSpec(ctx, req.Materialization.Name)
	if err != nil {
		return nil, fmt.Errorf("getting spec: %w", err)
	}

	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if err := validateSelectedFields(res, binding, storedSpec); err != nil {
			return nil, fmt.Errorf("validating selected fields: %w", err)
		}

		found, err := findExistingBinding(res.Index, binding.Collection.Name, storedSpec)
		if err != nil {
			return nil, err
		}

		if found == nil {
			desc, err := elasticSearch.ApplyIndex(res.Index, res.NumOfShards, res.NumOfReplicas, buildIndexProperties(binding), false)
			if err != nil {
				return nil, fmt.Errorf("creating elastic search index: %w", err)
			}
			addAction(desc)
		} else {
			// Map any new fields in the index.
			existingFields := found.FieldSelection.AllFields()
			for _, newField := range binding.FieldSelection.AllFields() {
				if slices.Contains(existingFields, newField) {
					continue
				}

				desc, err := elasticSearch.addMappingToIndex(res.Index, newField, propForField(newField, binding), req.DryRun)
				if err != nil {
					return nil, fmt.Errorf("adding mapping for field %q to index %q: %w", newField, res.Index, err)
				}
				addAction(desc)
			}
		}
	}

	if !req.DryRun {
		if err := elasticSearch.putSpec(ctx, req.Materialization); err != nil {
			return nil, fmt.Errorf("updating stored materialization spec: %w", err)
		}
		addAction("update stored materialize spec")
	}

	return &pm.Response_Applied{ActionDescription: strings.Join(actions, "\n")}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var elasticSearch, err = newElasticsearchClient(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating elastic search client: %w", err)
	}

	indexToBinding := make(map[string]int)
	var bindings []binding
	for idx, b := range open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}

		indexToBinding[res.Index] = idx
		bindings = append(bindings, binding{
			index:        res.Index,
			deltaUpdates: res.DeltaUpdates,
			fields:       append(b.FieldSelection.Keys, b.FieldSelection.Values...),
			docField:     b.FieldSelection.Document,
		})
	}

	var transactor = &transactor{
		elasticSearch:  elasticSearch,
		bindings:       bindings,
		indexToBinding: indexToBinding,
	}
	return transactor, &pm.Response_Opened{}, nil

}

func main() { boilerplate.RunMain(new(driver)) }
