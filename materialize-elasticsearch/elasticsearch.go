package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	log "github.com/sirupsen/logrus"
)

type ElasticSearch struct {
	client *elasticsearch.Client
}

func NewElasticSearch(ctx context.Context, endpoint string, username string, password string) (*ElasticSearch, error) {
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

func (es *ElasticSearch) CreateIndex(index string, schemaJSON json.RawMessage) error {
	var schema = make(map[string]interface{})
	var err = json.Unmarshal(schemaJSON, &schema)
	if err != nil {
		return fmt.Errorf("unmarshal schemaJSON: %w", err)
	}

	resp, err := es.client.Indices.Exists([]string{index})
	defer resp.Body.Close()
	if err != nil {
		return fmt.Errorf("index exists check error: %w", err)
	} else if resp.StatusCode == 200 {
		// The index exists, make sure it is the same as requested.
		return es.checkIndexMapping(index, schema)
	} else if resp.StatusCode != 404 {
		return fmt.Errorf("create index response invalid status code %d", resp.StatusCode)
	}

	// The index does not exist, create a new one.

	// Disable dynamic mapping.
	schema["dynamic"] = false

	body, err := json.Marshal(map[string]interface{}{"mappings": schema})
	if err != nil {
		return fmt.Errorf("create index marshal mappings: %w", err)
	}

	createResp, err := es.client.Indices.Create(
		index,
		es.client.Indices.Create.WithBody(bytes.NewReader(body)),
		es.client.Indices.Create.WithWaitForActiveShards("all"),
	)
	defer createResp.Body.Close()
	if err = es.parseErrorResp(err, createResp); err != nil {
		return fmt.Errorf("create indices: %w", err)
	}

	return nil
}

func (es *ElasticSearch) checkIndexMapping(index string, schema map[string]interface{}) error {
	var resp, err = es.client.Indices.GetMapping(
		es.client.Indices.GetMapping.WithIndex(index),
	)
	defer resp.Body.Close()
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

	var a map[string]interface{}
	if m, exist := r[index]; exist {
		a = m.Mappings.Properties
	} else {
		a = map[string]interface{}{}
	}

	var b = schema["properties"]

	if !reflect.DeepEqual(a, b) {
		return fmt.Errorf("schema inconsistent. existing: %v, new: %v", a, b)
	}
	return nil
}

func (es *ElasticSearch) Commit(ctx context.Context, items []*esutil.BulkIndexerItem) error {
	if len(items) == 0 {
		return nil
	}

	var bi, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: es.client,
		OnError: func(_ context.Context, err error) {
			log.Error(fmt.Sprintf("indexer: %v", err))
		},
		WaitForActiveShards: "all",
		// Disable automatic flushing, which is triggered by bi.Close call.
		FlushInterval: 100 * time.Hour,
	})
	if err != nil {
		return fmt.Errorf("building bulkIndexer: %w", err)
	}

	for _, item := range items {
		if err = bi.Add(ctx, *item); err != nil {
			return fmt.Errorf("adding item: %w", err)
		}
	}

	return bi.Close(ctx)
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
	defer resp.Body.Close()
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
	defer resp.Body.Close()
	if err = es.parseErrorResp(err, resp); err != nil {
		return fmt.Errorf("flush: %w", err)
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
