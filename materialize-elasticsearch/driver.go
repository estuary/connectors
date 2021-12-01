package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alecthomas/jsonschema"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-elasticsearch/schemabuilder"
	"github.com/estuary/protocols/fdb/tuple"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	"github.com/meirf/gopart"
	log "github.com/sirupsen/logrus"
)

type config struct {
	Endpoint string `json:"endpoint"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

func (c config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("missing Endpoint")
	}
	return nil
}

type resource struct {
	Index         string                        `json:"index"`
	DeltaUpdates  bool                          `json:"delta_updates"`
	FieldOverides []schemabuilder.FieldOverride `json:"field_overrides"`

	// The number of shards in ElasticSearch index. Must set to be greater than 0.
	NumOfShards int `json:"number_of_shards,omitempty"`
	// The number of replicas in ElasticSearch index. If not set, default to be 0.
	// For single-node clusters, make sure the this field is 0, b/c the
	// Elastic search needs to allocate replicas on different nodes.
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

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	endpointSchema, err := jsonschema.Reflect(&config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := jsonschema.Reflect(&resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev#FIXME",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure the specified resource is valid to build
		if _, err := schemabuilder.RunSchemaBuilder(
			binding.Collection.SchemaJson,
			res.FieldOverides,
		); err != nil {
			return nil, fmt.Errorf("building elastic search schema: %w", err)
		}

		var constraints = make(map[string]*pm.Constraint)

		for _, projection := range binding.Collection.Projections {
			var constraint = &pm.Constraint{}
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Constraint_FIELD_REQUIRED
				constraint.Reason = "The root document is needed."
			default:
				constraint.Type = pm.Constraint_FIELD_OPTIONAL
				constraint.Reason = "Non root document fields are not required."
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

	var elasticSearch, err = NewElasticSearch(cfg.Endpoint, cfg.Username, cfg.Password)
	if err != nil {
		return nil, fmt.Errorf("creating elasticSearch: %w", err)
	}

	var indices []string
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		var elasticSearchSchema, err = schemabuilder.RunSchemaBuilder(
			binding.Collection.SchemaJson,
			res.FieldOverides,
		)
		if err != nil {
			return nil, fmt.Errorf("building elastic search schema: %w", err)
		}

		if err = elasticSearch.CreateIndex(res.Index, res.NumOfShards, res.NumOfReplicas, elasticSearchSchema); err != nil {
			return nil, fmt.Errorf("creating elastic search index: %w", err)
		}
		indices = append(indices, res.Index)
	}

	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("created indices: ", strings.Join(indices, ","))}, nil
}

// ApplyDelete is a no-op.
func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

// Transactions implements the DriverServer interface.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var elasticSearch *ElasticSearch
	elasticSearch, err = NewElasticSearch(cfg.Endpoint, cfg.Username, cfg.Password)
	if err != nil {
		return fmt.Errorf("creating elastic search client: %w", err)
	}

	var bindings []*binding
	for _, b := range open.Open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		bindings = append(bindings,
			&binding{
				index:        res.Index,
				deltaUpdates: res.DeltaUpdates,
			})
	}

	var transactor = &transactor{
		elasticSearch:    elasticSearch,
		bindings:         bindings,
		bulkIndexerItems: []*esutil.BulkIndexerItem{},
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	var log = log.WithField(
		"materialization",
		fmt.Sprintf("mat-elasticsearch-%d-%d", open.Open.KeyBegin, open.Open.KeyEnd),
	)
	return pm.RunTransactions(stream, transactor, log)
}

type binding struct {
	index        string
	deltaUpdates bool
}

type transactor struct {
	elasticSearch    *ElasticSearch
	bindings         []*binding
	bulkIndexerItems []*esutil.BulkIndexerItem
}

const loadByIdBatchSize = 1000

func (t *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	var loadingIdsByBinding = map[int][]string{}

	for it.Next() {
		var b = t.bindings[it.Binding]
		if b.deltaUpdates {
			panic("Load should not be called for delta updates.")
		}
		loadingIdsByBinding[it.Binding] = append(loadingIdsByBinding[it.Binding], documentId(it.Key))
	}

	for binding, ids := range loadingIdsByBinding {
		var b = t.bindings[binding]
		for idxRange := range gopart.Partition(len(ids), loadByIdBatchSize) {
			var docs, err = t.elasticSearch.SearchByIds(b.index, ids[idxRange.Low:idxRange.High])
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

func (t *transactor) Prepare(_ context.Context, _ pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	if len(t.bulkIndexerItems) != 0 {
		panic("non-empty bulkIndexerItems") // Invariant: previous call is finished.
	}
	return pf.DriverCheckpoint{}, nil
}

func (t *transactor) Store(it *pm.StoreIterator) error {
	var lastErr error = nil
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
			OnFailure: func(_ context.Context, _ esutil.BulkIndexerItem, r esutil.BulkIndexerResponseItem, e error) {
				log.Error(fmt.Sprintf("failed with response: %+v, error: %v", r, e))
				lastErr = fmt.Errorf("store items: %+v, %w", r, e)
			},
		}

		t.bulkIndexerItems = append(t.bulkIndexerItems, item)
	}

	return lastErr
}

func (t *transactor) Commit(ctx context.Context) error {
	defer func() { t.bulkIndexerItems = t.bulkIndexerItems[:0] }()

	if err := t.elasticSearch.Commit(ctx, t.bulkIndexerItems); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	for _, b := range t.bindings {
		// Using Flush instead of Refresh to make sure the data are persisted.
		if err := t.elasticSearch.Flush(b.index); err != nil {
			return fmt.Errorf("commit flush: %w", err)
		}
	}
	return nil
}

func (t *transactor) Acknowledge(context.Context) error {
	return nil
}

func documentId(tuple tuple.Tuple) string {
	return base64.RawStdEncoding.EncodeToString(tuple.Pack())
}

func (t *transactor) Destroy() {}

func main() { boilerplate.RunMain(new(driver)) }
