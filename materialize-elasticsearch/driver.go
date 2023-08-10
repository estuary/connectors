package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
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

	var elasticSearch, err = newElasticsearchClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	var out []*pm.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure the specified resource is valid to build.
		// TODO: Do more comprehensive connection verifications?
		if _, err := elasticSearch.validateIndex(res.Index, res.NumOfShards); err != nil {
			return nil, fmt.Errorf("validate elastic search index: %w", err)
		}

		// TODO: Handle incompatible migrations.

		var constraints = make(map[string]*pm.Response_Validated_Constraint)

		for _, projection := range binding.Collection.Projections {
			// Types like ["string", "integer"] with "format: integer" have multiple types which we
			// cannot support, but the string value can be coerced to a isNumeric value, which we can
			// support.
			_, isNumeric := isFormattedNumeric(projection)

			var constraint = &pm.Response_Validated_Constraint{}
			switch {
			case projection.IsPrimaryKey:
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "Primary key locations are required"
			case projection.IsRootDocumentProjection() && !res.DeltaUpdates:
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document is required for a standard materialization"
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "The root document should usually be materialized"
			case projection.Inference.IsSingleScalarType() || isNumeric:
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "The projection has a single scalar type"
			case projection.Inference.IsSingleType() && !slices.Contains(projection.Inference.Types, "array"):
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				constraint.Reason = "This field is able to be materialized"

			default:
				// Anything else is either multiple different types, a single 'null' type, or an
				// array type which we currently don't support. We could potentially support array
				// types if they made the "elements" configuration avaiable and that was a single
				// type.
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				constraint.Reason = "Cannot materialize this field"
			}
			constraints[projection.Field] = constraint
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

	var actionDesc = strings.Builder{}
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if desc, err := elasticSearch.ApplyIndex(res.Index, res.NumOfShards, res.NumOfReplicas, buildIndexProperties(binding), false); err != nil {
			return nil, fmt.Errorf("creating elastic search index: %w", err)
		} else {
			actionDesc.WriteString(desc)
			actionDesc.WriteRune('\n')
		}
	}

	return &pm.Response_Applied{ActionDescription: actionDesc.String()}, nil
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

	var bindings []binding
	for _, b := range open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}
		bindings = append(bindings, binding{
			index:        res.Index,
			deltaUpdates: res.DeltaUpdates,
			fields:       append(b.FieldSelection.Keys, b.FieldSelection.Values...),
			docField:     b.FieldSelection.Document,
		})
	}

	var transactor = &transactor{
		elasticSearch: elasticSearch,
		bindings:      bindings,
	}
	return transactor, &pm.Response_Opened{}, nil

}

type binding struct {
	index        string
	deltaUpdates bool
	fields       []string
	docField     string
}

type transactor struct {
	elasticSearch *ElasticSearch
	bindings      []binding
}

const loadByIdBatchSize = 1000

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var loadingIdsByBinding = map[int][]string{}

	// TODO(johnny): We should be executing these in chunks along the way,
	// rather than queuing all until the end.
	for it.Next() {
		loadingIdsByBinding[it.Binding] = append(loadingIdsByBinding[it.Binding], documentId(it.Key))
	}

	for binding, ids := range loadingIdsByBinding {
		var b = t.bindings[binding]
		for start := 0; start < len(ids); start += loadByIdBatchSize {
			var stop = start + loadByIdBatchSize
			if stop > len(ids) {
				stop = len(ids)
			}

			var docs, err = t.elasticSearch.SearchByIds(b.index, ids[start:stop], b.docField)
			if err != nil {
				return fmt.Errorf("Load docs by ids: %w", err)
			}

			for _, doc := range docs {
				if err = loaded(binding, doc); err != nil {
					return fmt.Errorf("callback: %w", err)
				}
			}
		}
	}

	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var items []*esutil.BulkIndexerItem

	// TODO(johnny): store chunks of items along the way rather queuing
	// them all to apply at the very end.
	for it.Next() {
		var b = t.bindings[it.Binding]
		var action, docId = "create", ""
		if !b.deltaUpdates {
			action, docId = "index", documentId(it.Key)
		}

		doc := make(map[string]any)

		for idx, v := range append(it.Key, it.Values...) {
			if b, ok := v.([]byte); ok {
				// An object or array field as raw JSON bytes. We currently only support objects.
				v = json.RawMessage(b)
			}

			doc[b.fields[idx]] = v
		}
		if b.docField != "" {
			doc[b.docField] = it.RawJSON
		}

		body, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}

		var item = &esutil.BulkIndexerItem{
			Index:      t.bindings[it.Binding].index,
			Action:     action,
			DocumentID: docId,
			Body:       bytes.NewReader(body),
		}
		items = append(items, item)
	}

	if err := t.elasticSearch.Commit(it.Context(), items); err != nil {
		return nil, err
	}

	// Refresh to make segments available for search, and flush to disk.
	for _, b := range t.bindings {
		refreshResp, err := t.elasticSearch.client.Indices.Refresh(t.elasticSearch.client.Indices.Refresh.WithIndex(b.index))
		defer closeResponse(refreshResp)
		if err = t.elasticSearch.parseErrorResp(err, refreshResp); err != nil {
			return nil, fmt.Errorf("failed to refresh: %w", err)
		}

		// TODO(whb): Is this necessary?
		flushResp, err := t.elasticSearch.client.Indices.Flush(t.elasticSearch.client.Indices.Flush.WithIndex(b.index))
		defer closeResponse(flushResp)
		if err = t.elasticSearch.parseErrorResp(err, flushResp); err != nil {
			return nil, fmt.Errorf("failed to flush: %w", err)
		}
	}

	return nil, nil
}

func documentId(tuple tuple.Tuple) string {
	return base64.RawStdEncoding.EncodeToString(tuple.Pack())
}

func (t *transactor) Destroy() {}

func main() { boilerplate.RunMain(new(driver)) }
