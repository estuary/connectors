package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	// TODO(jixiang): test the driver with es version v8, and AWS OpenSearch in addition to Elastic cloud.
	//                extend the API as needed.
	elasticsearch "github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	log "github.com/sirupsen/logrus"
)

// ElasticSearch provides APIs for interacting with ElasticSearch service.
type ElasticSearch struct {
	client *elasticsearch.Client
}

type IndexSettings struct {
	Index struct {
		NumOfShards   string `json:"number_of_shards,omitempty"`
		NumOfReplicas string `json:"number_of_replicas"`
	} `json:"index"`
}

func newElasticSearch(endpoint string, username string, password string) (*ElasticSearch, error) {
	var client, err = elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses: []string{endpoint},
			Username:  username,
			Password:  password,
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

// CreateIndex creates a new es index and sets its mappings to be schemaJSON,
// if the index does not exist. Otherwise,
// 1. if the new index has a different mapping (or num_of_shards spec) from the existing index,
//    the API stops with an error, because the mapping and num_of_shards cannot be changed after creation.
// 2. if the new index has a different num_of_replica spec from the existing index,
//    the API resets the setting to match the new.
func (es *ElasticSearch) CreateIndex(index string, numOfShards int, numOfReplicas int, schemaJSON json.RawMessage, dryRun bool) error {
	var schema = make(map[string]interface{})
	var err = json.Unmarshal(schemaJSON, &schema)
	if err != nil {
		return fmt.Errorf("unmarshal schemaJSON: %w", err)
	}

	var numOfShardsStr = strconv.Itoa(numOfShards)
	var numOfReplicasStr = strconv.Itoa(numOfReplicas)

	resp, err := es.client.Indices.Exists([]string{index})
	defer closeResponse(resp)
	if err != nil {
		return fmt.Errorf("index exists check error: %w", err)
	} else if resp.StatusCode == 200 {
		// The index exists, make sure it is compatible to the requested.
		if mappingErr := es.checkIndexMapping(index, schema); mappingErr != nil {
			// If inconsistent schema of the index is detected, user needs to decide
			// either removing the existing index or renaming the new index. As a mapping cannot
			// be modified after creation.
			return fmt.Errorf("check index mapping: %w", mappingErr)
		} else if settings, settingErr := es.getIndexSettings(index); settingErr != nil {
			return fmt.Errorf("get index setting: %w", settingErr)
		} else if settings.Index.NumOfShards != numOfShardsStr {
			// Same as a schema, the number of shards cannot be changed after creation.
			return fmt.Errorf(
				"%s: expected number of shards (%d) is inconsistent with the existing number of shards (%s). Try one of the followings: 1) change the number of shard in spec, 2) rename the index, or 3) delete the existing index.",
				index, numOfShards, settings.Index.NumOfShards,
			)
		} else if settings.Index.NumOfReplicas != numOfReplicasStr {
			if dryRun {
				return nil
			}
			// Number of replicas is allowed to be adjusted.
			log.WithFields(log.Fields{
				"index":                     index,
				"requested num_of_replicas": numOfReplicas,
				"existing num_of_replicas":  settings.Index.NumOfReplicas,
			}).Info("expected number of replicas is inconsistent with the existing. Updating...")

			var inputSetting IndexSettings
			inputSetting.Index.NumOfReplicas = numOfReplicasStr
			return es.updateIndexSettings(index, &inputSetting)
		}
		return nil
	} else if resp.StatusCode != 404 {
		return fmt.Errorf("index exists: invalid response status code %d", resp.StatusCode)
	}

	// The index does not exist, create a new one.
	if dryRun {
		return nil
	}

	// Disable dynamic mapping.
	schema["dynamic"] = false

	var settings IndexSettings
	settings.Index.NumOfShards = numOfShardsStr
	settings.Index.NumOfReplicas = numOfReplicasStr

	body, err := json.Marshal(map[string]interface{}{
		"mappings": schema,
		"settings": settings,
	})
	if err != nil {
		return fmt.Errorf("create index marshal mappings: %w", err)
	}

	createResp, err := es.client.Indices.Create(
		index,
		es.client.Indices.Create.WithBody(bytes.NewReader(body)),
		es.client.Indices.Create.WithWaitForActiveShards("all"),
	)
	defer closeResponse(createResp)
	if err = es.parseErrorResp(err, createResp); err != nil {
		return fmt.Errorf("create indices: %w", err)
	}

	return nil
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

func (es *ElasticSearch) SearchByIds(index string, ids []string) ([]json.RawMessage, error) {
	if len(ids) == 0 {
		return []json.RawMessage{}, nil
	}

	var resp, err = es.client.Search(
		es.client.Search.WithIndex(index),
		es.client.Search.WithBody(es.buildIDQuery(ids)),
		es.client.Search.WithSize(len(ids)),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return nil, fmt.Errorf("search by ids: %w", err)
	}

	var r = struct {
		Hits struct {
			Hits []struct {
				ID     string          `json:"_id"`
				Source json.RawMessage `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}{}

	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	var results = make([]json.RawMessage, 0, len(r.Hits.Hits))
	for _, hit := range r.Hits.Hits {
		results = append(results, hit.Source)
	}
	return results, nil
}

func (es *ElasticSearch) Flush(index string) error {
	var resp, err = es.client.Indices.Flush(
		es.client.Indices.Flush.WithIndex(index),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	return nil
}

func (es *ElasticSearch) checkIndexMapping(index string, schema map[string]interface{}) error {
	var resp, err = es.client.Indices.GetMapping(
		es.client.Indices.GetMapping.WithIndex(index),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return fmt.Errorf("get index mapping: %w", err)
	}

	var r = map[string]struct {
		Mappings struct {
			Properties map[string]interface{} `json:"properties"`
		} `json:"mappings"`
	}{}

	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return fmt.Errorf("check index mapping decode: %w", err)
	}

	var a = map[string]interface{}{}
	if m, exist := r[index]; exist {
		a = m.Mappings.Properties
	}

	var b = schema["properties"]

	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("schema inconsistent. Try rename the index, or delete the existing index and restart. Details: existing: %v, new: %v", a, b)
	}
	return nil
}

func (es *ElasticSearch) updateIndexSettings(index string, settings *IndexSettings) error {
	var body, err = json.Marshal(settings)
	if err != nil {
		return fmt.Errorf("marshal index setting: %w", err)
	}

	resp, err := es.client.Indices.PutSettings(
		bytes.NewReader(body),
		es.client.Indices.PutSettings.WithIndex(index),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return fmt.Errorf("update index setting: %w", err)
	}
	return nil
}

func (es *ElasticSearch) getIndexSettings(index string) (*IndexSettings, error) {
	var resp, err = es.client.Indices.GetSettings(
		es.client.Indices.GetSettings.WithIndex(index),
	)
	defer closeResponse(resp)
	if err = es.parseErrorResp(err, resp); err != nil {
		return nil, fmt.Errorf("get index settings: %w", err)
	}

	var r = map[string]struct {
		Settings *IndexSettings `json:"settings"`
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

func (es *ElasticSearch) buildIDQuery(ids []string) io.Reader {
	var quotedIds = make([]string, len(ids))
	for i, id := range ids {
		quotedIds[i] = fmt.Sprintf("%q", id)
	}

	var queryBody = fmt.Sprintf(`{
		"query": {
			"ids" : {
			  "values" : [%s]
			}
		  }
	}`, strings.Join(quotedIds, ","))

	return strings.NewReader(queryBody)
}

func closeResponse(response *esapi.Response) {
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
}
