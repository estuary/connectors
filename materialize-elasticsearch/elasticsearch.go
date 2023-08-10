package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	log "github.com/sirupsen/logrus"
)

// ElasticSearch provides APIs for interacting with ElasticSearch service.
type ElasticSearch struct {
	client *elasticsearch.Client
}

// indexResponse is used only for deserializing the settings from elasticsearch responses.
// They only return integers as strings, though the API will accept actual integers.
type indexResponse struct {
	Index struct {
		NumOfShards   string `json:"number_of_shards"`
		NumOfReplicas string `json:"number_of_replicas"`
	} `json:"index"`
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

func (es *ElasticSearch) validateIndex(index string, shards int) (*indexResponse, error) {
	indexExists, err := es.indexExists(index)
	if err != nil || !indexExists {
		return nil, err
	}

	res, err := es.getIndexSettings(index)
	if err != nil {
		return nil, fmt.Errorf("get index setting: %w", err)
	}

	if res.Index.NumOfShards != strconv.Itoa(shards) {
		return nil, fmt.Errorf(
			"%s: the number of shards cannot be changed after index creation. The number of shards in the resource "+
				"configuration (%d) is inconsistent with the current number of shards (%s). To fix this, you can either change "+
				"the number of shards back to %s, or change the name of the index to create a new one",
			index, shards, res.Index.NumOfShards, res.Index.NumOfShards,
		)
	}

	return res, nil
}

// ApplyIndex creates a new es index and sets its mappings per indexProps if it doesn't exist.
// Otherwise:
//  1. If the new index has a different number of shards than the existing index, the API stops with
//     an error, because the number of shards cannot be changed after creation.
//  2. If the new index has a different number of replicas than the existing index, the existing
//     index is updated.
func (es *ElasticSearch) ApplyIndex(index string, shards int, replicas int, indexProps map[string]property, dryRun bool) (string, error) {
	settings, err := es.validateIndex(index, shards)
	if err != nil {
		return "", err
	}

	if settings != nil {
		// Index already exists. Does it need updated?
		if settings.Index.NumOfReplicas != strconv.Itoa(replicas) {
			var actionDesc = fmt.Sprintf("update index '%s' replicas from %s to %d", index, settings.Index.NumOfReplicas, replicas)
			if dryRun {
				return actionDesc + " (skipping due to dry-run)", nil
			}
			return actionDesc, es.updateIndexReplicas(index, replicas)
		}

		return fmt.Sprintf("using existing Elasticsearch index '%s'", index), nil
	}

	// Index does not exist, create a new one.
	var actionDesc = fmt.Sprintf("create Elasticsearch index '%s'", index)
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

func (es *ElasticSearch) SearchByIds(index string, ids []string, docField string) ([]json.RawMessage, error) {
	if len(ids) == 0 {
		return []json.RawMessage{}, nil
	}

	var resp, err = es.client.Search(
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(es.buildIDQuery(ids, docField)),
		es.client.Search.WithSize(len(ids)),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return nil, fmt.Errorf("search by ids: %w", err)
	}

	var r = struct {
		Hits struct {
			Hits []struct {
				ID     string                     `json:"_id"`
				Source map[string]json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}{}

	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	var results = make([]json.RawMessage, 0, len(r.Hits.Hits))
	for _, hit := range r.Hits.Hits {
		results = append(results, hit.Source[docField])
	}
	return results, nil
}

func (es *ElasticSearch) updateIndexReplicas(index string, new_number_of_replicas int) error {
	var body = fmt.Sprintf(`{"index": {"number_of_replicas": %d}}`, new_number_of_replicas)
	resp, err := es.client.Indices.PutSettings(
		bytes.NewReader([]byte(body)),
		es.client.Indices.PutSettings.WithIndex(index),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return fmt.Errorf("update index setting: %w", err)
	}
	return nil
}

func (es *ElasticSearch) getIndexSettings(index string) (*indexResponse, error) {
	var resp, err = es.client.Indices.GetSettings(
		es.client.Indices.GetSettings.WithIndex(index),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return nil, fmt.Errorf("get index settings: %w", err)
	}

	var r = map[string]struct {
		Settings *indexResponse `json:"settings"`
	}{}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("get index setting decode: %w", err)
	}
	if m, exist := r[index]; exist {
		return m.Settings, nil
	}
	return nil, fmt.Errorf("missing index settings: %s", index)
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

func (es *ElasticSearch) buildIDQuery(ids []string, docField string) io.Reader {
	var quotedIds = make([]string, len(ids))
	for i, id := range ids {
		quotedIds[i] = fmt.Sprintf("%q", id)
	}

	var queryBody = fmt.Sprintf(`{
		"_source": %q, 
		"query": {
			"ids" : {
			  "values" : [%s]
			}
		  }
	}`,
		docField,
		strings.Join(quotedIds, ","),
	)

	return strings.NewReader(queryBody)
}

func closeResponse(response *esapi.Response) {
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
}
