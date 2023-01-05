package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v7/esutil"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-elasticsearch/schemabuilder"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type config struct {
	Endpoint string `json:"endpoint" jsonschema:"title=Endpoint,description=Endpoint host or URL. If using Elastic Cloud this follows the format https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT" jsonschema_extras:"order=0"`
	Username string `json:"username,omitempty" jsonschema:"title=Username,description=User to connect to the endpoint." jsonschema_extras:"order=1"`
	Password string `json:"password,omitempty" jsonschema:"title=Password,description=Password to connect to the endpoint." jsonschema_extras:"secret=true,order=2"`
}

func (c config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("missing Endpoint")
	}
	return nil
}

type resource struct {
	Index         string                        `json:"index" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates  bool                          `json:"delta_updates" jsonschema:"default=false"`
	FieldOverides []schemabuilder.FieldOverride `json:"field_overrides,omitempty"`

	NumOfShards   int `json:"number_of_shards,omitempty" jsonschema:"default=1"`
	NumOfReplicas int `json:"number_of_replicas,omitempty"`
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
		return "The number of shards in ElasticSearch index. Must set to be greater than 0."
	case "NumOfReplicas":
		return "The number of replicas in ElasticSearch index. If not set, default to be 0. " +
			"For single-node clusters, make sure this field is 0, because the " +
			"Elastic search needs to allocate replicas on different nodes."
	default:
		return ""
	}
}

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	endpointSchema, err := schemagen.GenerateSchema("Elasticsearch Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Elasticsearch Index", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/materialize-elasticsearch",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var elasticSearch, err = newElasticSearch(cfg.Endpoint, cfg.Username, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure the specified resource is valid to build
		if schema, err := schemabuilder.RunSchemaBuilder(
			binding.Collection.GetReadSchemaJson(),
			res.FieldOverides,
		); err != nil {
			return nil, fmt.Errorf("building elastic search schema: %w", err)
		} else if err = elasticSearch.CreateIndex(res.Index, res.NumOfShards, res.NumOfReplicas, schema, true); err != nil {
			// Dry run the index creation to make sure the specifications of the index are consistent with the existing one, if any.
			return nil, fmt.Errorf("validate elastic search index: %w", err)
		}

		var constraints = make(map[string]*pm.Constraint)

		for _, projection := range binding.Collection.Projections {
			var constraint = &pm.Constraint{}
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document is required."
			case projection.IsPrimaryKey:
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "Primary key locations are required."
			default:
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Non-root document fields and non-primary key locations cannot be used."
			}
			constraints[projection.Field] = constraint
		}
		out = append(out, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.DeltaUpdates,
			ResourcePath: []string{res.Index},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

func (driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var elasticSearch, err = newElasticSearch(cfg.Endpoint, cfg.Username, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	var indices []string
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if elasticSearchSchema, err := schemabuilder.RunSchemaBuilder(
			binding.Collection.GetReadSchemaJson(),
			res.FieldOverides,
		); err != nil {
			return nil, fmt.Errorf("building elastic search schema: %w", err)
		} else if err = elasticSearch.CreateIndex(res.Index, res.NumOfShards, res.NumOfReplicas, elasticSearchSchema, false); err != nil {
			return nil, fmt.Errorf("creating elastic search index: %w", err)
		}

		indices = append(indices, res.Index)
	}

	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("created indices: ", strings.Join(indices, ","))}, nil
}

func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var elasticSearch, err = newElasticSearch(cfg.Endpoint, cfg.Username, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	var indices []string
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		indices = append(indices, res.Index)
	}

	if req.DryRun {
		return &pm.ApplyResponse{ActionDescription: fmt.Sprint("to delete indices: ", strings.Join(indices, ","))}, nil
	}

	if err = elasticSearch.DeleteIndices(indices); err != nil {
		return nil, fmt.Errorf("deleting elastic search index: %w", err)
	}
	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("deleted indices: ", strings.Join(indices, ","))}, nil
}

// Transactions implements the DriverServer interface.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	return pm.RunTransactions(stream, func(ctx context.Context, open pm.TransactionRequest_Open) (pm.Transactor, *pm.TransactionResponse_Opened, error) {
		var cfg config
		if err := pf.UnmarshalStrict(open.Materialization.EndpointSpecJson, &cfg); err != nil {
			return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
		}

		var elasticSearch, err = newElasticSearch(cfg.Endpoint, cfg.Username, cfg.Password)
		if err != nil {
			return nil, nil, fmt.Errorf("creating elastic search client: %w", err)
		}

		var bindings []*binding
		for _, b := range open.Materialization.Bindings {
			var res resource
			if err := pf.UnmarshalStrict(b.ResourceSpecJson, &res); err != nil {
				return nil, nil, fmt.Errorf("parsing resource config: %w", err)
			}
			bindings = append(bindings,
				&binding{
					index:        res.Index,
					deltaUpdates: res.DeltaUpdates,
				})
		}

		var transactor = &transactor{
			elasticSearch: elasticSearch,
			bindings:      bindings,
		}
		return transactor, &pm.TransactionResponse_Opened{}, nil
	})

}

type binding struct {
	index        string
	deltaUpdates bool
}

type transactor struct {
	elasticSearch *ElasticSearch
	bindings      []*binding
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

			var docs, err = t.elasticSearch.SearchByIds(b.index, ids[start:stop])
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

		var item = &esutil.BulkIndexerItem{
			Index:      t.bindings[it.Binding].index,
			Action:     action,
			DocumentID: docId,
			Body:       bytes.NewReader(it.RawJSON),
		}
		items = append(items, item)
	}

	if err := t.elasticSearch.Commit(it.Context(), items); err != nil {
		return nil, err
	}

	for _, b := range t.bindings {
		// Using Flush instead of Refresh to make sure the data are persisted.
		// Although both operations ensure the data in ElasticSearch available for search,
		// Flush guarantees the data are persisted to disk. For details see
		// https://qbox.io/blog/refresh-flush-operations-elasticsearch-guide/)
		if err := t.elasticSearch.Flush(b.index); err != nil {
			return nil, err
		}
	}

	return nil, nil
}

func documentId(tuple tuple.Tuple) string {
	return base64.RawStdEncoding.EncodeToString(tuple.Pack())
}

func (t *transactor) Destroy() {}

func main() { boilerplate.RunMain(new(driver)) }
