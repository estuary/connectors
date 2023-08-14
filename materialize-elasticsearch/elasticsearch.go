package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// ElasticSearch provides APIs for interacting with ElasticSearch service.
type ElasticSearch struct {
	client *elasticsearch.Client
}

type createIndexParams struct {
	Settings createIndexSettings `json:"settings"`
	Mappings indexMappings       `json:"mappings"`
}

type createIndexSettings struct {
	Shards   int `json:"number_of_shards"`
	Replicas int `json:"number_of_replicas"`
}

type indexMappings struct {
	Properties map[string]property `json:"properties"`
}

func newElasticsearchClient(cfg config) (*ElasticSearch, error) {
	var client, err = elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses: []string{cfg.Endpoint},
			Username:  cfg.Credentials.Username,
			Password:  cfg.Credentials.Password,
			APIKey:    cfg.Credentials.ApiKey,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &ElasticSearch{client: client}, nil
}

// DeleteIndex deletes a list of indices.
func (es *ElasticSearch) DeleteIndices(indices []string) error {
	resp, err := es.client.Indices.Delete(
		indices,
		es.client.Indices.Delete.WithIgnoreUnavailable(true),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return fmt.Errorf("delete indices: %w", err)
	}

	return nil
}

func (es *ElasticSearch) indexExists(index string) (bool, error) {
	resp, err := es.client.Indices.Exists([]string{index})
	closeResponse(resp)
	if err != nil {
		return false, err
	}
	switch resp.StatusCode {
	case 200:
		return true, nil
	case 401:
		return false, fmt.Errorf("the credential you provided is invalid (likely has missing or extra characters)")
	case 403:
		return false, fmt.Errorf("the user does not have permission to access the Elasticsearch index %q", index)
	case 404:
		return false, nil
	default:
		return false, fmt.Errorf("index exists: invalid response status code %d", resp.StatusCode)
	}
}

// ApplyIndex creates a new index and sets its mappings per indexProps if it doesn't already exist.
func (es *ElasticSearch) ApplyIndex(index string, shards int, replicas int, indexProps map[string]property, dryRun bool) (string, error) {
	if indexExists, err := es.indexExists(index); err != nil {
		return "", err
	} else if indexExists {
		return "", nil
	}

	// Index does not exist, create a new one.
	var actionDesc = fmt.Sprintf("create index '%s'", index)
	if dryRun {
		return actionDesc + " (skipping due to dry-run)", nil
	}

	params := createIndexParams{
		Settings: createIndexSettings{
			Shards:   shards,
			Replicas: replicas,
		},
		Mappings: indexMappings{
			Properties: indexProps,
		},
	}

	b, err := json.Marshal(params)
	if err != nil {
		return "", err
	}

	createResp, err := es.client.Indices.Create(
		index,
		es.client.Indices.Create.WithBody(bytes.NewReader(b)),
		es.client.Indices.Create.WithWaitForActiveShards("all"),
	)
	defer closeResponse(createResp)
	if err = es.parseErrorResp(err, createResp); err != nil {
		return "", fmt.Errorf("create indices: %w", err)
	}

	return actionDesc, nil
}

func (es *ElasticSearch) addMappingToIndex(index string, field string, prop property, dryRun bool) (string, error) {
	body := make(map[string]map[string]property)
	body["properties"] = map[string]property{field: prop}

	b, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("marshalling body: %w", err)
	}

	var actionDesc = fmt.Sprintf("add field '%s' to index '%s' with type '%s'", field, index, prop.Type)
	if dryRun {
		return actionDesc + " (skipping due to dry-run)", nil
	}

	res, err := es.client.Indices.PutMapping([]string{index}, bytes.NewReader(b))
	defer closeResponse(res)
	if err = es.parseErrorResp(err, res); err != nil {
		return "", err
	}

	return actionDesc, nil
}

// Commit performs the bulk operations specified by the input items.
// Note: the OnFailure callback of the `items` will be overridden by the function.
func (es *ElasticSearch) Commit(ctx context.Context, items []*esutil.BulkIndexerItem) error {
	if len(items) == 0 {
		return nil
	}

	// lastError records the most recent error ocurred during the processing of the bulk items.
	// There are three sources of errors.
	// 1. errors related to a single create/index operation specified by a bulk item.
	// 2. errors related to a bulk operation performed by the workers. (A bulk operation performs one or more single operations in one request.)
	// 3. errors related to context cancel / timeout.
	//
	// errors of type 1 are collected by the OnFailure callback on the bulk item.
	// errors of types 2 and 3 are collected by the OnError callback of the bulk indexer.
	var lastError error
	var bi, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: es.client,
		OnError: func(_ context.Context, err error) {
			if err != nil {
				log.WithFields(log.Fields{"error": err}).Error("bulk index error")
				lastError = fmt.Errorf("bulk index error: %w", err)
			}
		},
		// Makes sure the changes are propgated to all shards.
		WaitForActiveShards: "all",
		// Disable automatic flushing, instead, use bi.Close to trigger a flush.
		FlushInterval: 100 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("building bulkIndexer: %w", err)
	}

	for _, item := range items {
		item.OnFailure = func(_ context.Context, _ esutil.BulkIndexerItem, r esutil.BulkIndexerResponseItem, e error) {
			log.WithFields(log.Fields{"error": e, "response": r}).Error("index item failure")
			lastError = fmt.Errorf("index item failure, resp: %v, error: %w", r, e)
		}

		if err = bi.Add(ctx, *item); err != nil {
			return fmt.Errorf("adding item: %w", err)
		}
	}

	bi.Close(ctx)
	if lastError != nil {
		return fmt.Errorf("bulk commit: %w", lastError)
	}

	return nil
}

func (es *ElasticSearch) parseErrorResp(err error, resp *esapi.Response) error {
	if err != nil {
		return fmt.Errorf("response err: %w", err)
	}

	if resp.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return fmt.Errorf("decode error: %w", err)
		}
		return fmt.Errorf("error response [%s] %s", resp.Status(), resp.String())
	}
	return nil
}

func closeResponse(response *esapi.Response) {
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
}

const (
	defaultFlowMaterializations = "flow_materializations_v2"
)

func (es *ElasticSearch) createMetaIndex(ctx context.Context) (string, error) {
	props := map[string]property{
		"specBytes": {Type: elasticTypeBinary},
	}

	return es.ApplyIndex(defaultFlowMaterializations, 1, 0, props, false)
}

func (es *ElasticSearch) putSpec(ctx context.Context, spec *pf.MaterializationSpec) error {
	specBytes, err := spec.Marshal()
	if err != nil {
		return fmt.Errorf("marshalling spec: %w", err)
	}

	ss := struct {
		SpecBytes string `json:"specBytes"`
	}{
		SpecBytes: base64.StdEncoding.EncodeToString(specBytes),
	}

	b, err := json.Marshal(ss)
	if err != nil {
		return fmt.Errorf("marshalling storedSpec: %w", err)
	}

	res, err := es.client.Index(
		defaultFlowMaterializations,
		bytes.NewReader(b),
		es.client.Index.WithRefresh("true"),
		es.client.Index.WithDocumentID(url.PathEscape(spec.Name.String())),
	)
	defer closeResponse(res)
	if err = es.parseErrorResp(err, res); err != nil {
		return fmt.Errorf("updating stored materialization spec: %w", err)
	}

	return nil
}

func (es *ElasticSearch) getSpec(ctx context.Context, materialization pf.Materialization) (*pf.MaterializationSpec, error) {
	res, err := es.client.Get(
		defaultFlowMaterializations,
		url.PathEscape(string(materialization)),
	)
	defer closeResponse(res)
	if res.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if err = es.parseErrorResp(err, res); err != nil {
		return nil, err
	}

	spec := new(pf.MaterializationSpec)

	got := struct {
		Source struct {
			SpecBytes string `json:"specBytes"`
		} `json:"_source"`
	}{}

	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		return nil, fmt.Errorf("decoding response body: %w", err)
	} else if specBytes, err := base64.StdEncoding.DecodeString(got.Source.SpecBytes); err != nil {
		return nil, fmt.Errorf("base64.Decode: %w", err)
	} else if err := spec.Unmarshal(specBytes); err != nil {
		return nil, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("validating spec: %w", err)
	}

	return spec, nil
}
