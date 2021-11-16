package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	log "github.com/sirupsen/logrus"
)

type ElasticSearch struct {
	ctx    context.Context
	client *elasticsearch.Client
}

func NewElasticSearch(ctx context.Context, endpoint string) (*ElasticSearch, error) {
	var client, err = elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses: []string{endpoint},
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &ElasticSearch{ctx: ctx, client: client}, nil
}

func (es *ElasticSearch) CreateIndex(index string, schemaJSON json.RawMessage) error {
	if resp, err := es.client.Indices.Exists([]string{index}); resp == nil {
		return fmt.Errorf("update index: %w", err)
	} else if resp.StatusCode == 200 {
		// the index already exists
		// TODO(jixiang): update index?
	} else if resp.StatusCode == 404 {
		// the index is missing, create one.
		var schema = make(map[string]interface{})
		err = json.Unmarshal(schemaJSON, &schema)
		if err != nil {
			return fmt.Errorf("unmarshal mappingJSON: %w", err)
		}

		// Disable dynamic mapping.
		schema["dynamic"] = false

		body, err := json.Marshal(map[string]interface{}{"mappings": schema})
		if err != nil {
			return fmt.Errorf("marshal mappings: %w", err)
		}

		resp, err := es.client.Indices.Create(
			index,
			es.client.Indices.Create.WithBody(bytes.NewReader(body)),
			es.client.Indices.Create.WithWaitForActiveShards("1"),
		)

		if err = es.parseErrorResp(resp); err != nil {
			return fmt.Errorf("indices create: %w", err)
		}
	} else {
		return fmt.Errorf("invalid status code %d", resp.StatusCode)
	}

	return nil
}

func (es *ElasticSearch) Commit(items []*esutil.BulkIndexerItem) error {
	if len(items) == 0 {
		return nil
	}
	var bi, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client: es.client,
		OnError: func(_ context.Context, err error) {
			log.Error(err)
		},
		//NumWorkers:    r.config.NumWorkers,
		//FlushBytes:    r.config.FlushBytes,
		FlushInterval: time.Hour, // Disable automatic flushing
	})
	if err != nil {
		return fmt.Errorf("building bulkIndexer: %w", err)
	}

	for _, item := range items {
		if err = bi.Add(es.ctx, *item); err != nil {
			return fmt.Errorf("Adding item: %w", err)
		}
	}
	return bi.Close(es.ctx)
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

	if err = es.parseErrorResp(resp); err != nil {
		return nil, fmt.Errorf("search by ids: %w", err)
	}

	var r = struct {
		Took int
		Hits struct {
			Total struct {
				Value int
			}
			Hits []struct {
				ID         string          `json:"_id"`
				Source     json.RawMessage `json:"_source"`
				Highlights json.RawMessage `json:"highlight"`
				Sort       []interface{}   `json:"sort"`
			}
		}
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
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err = es.parseErrorResp(resp); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

func (es *ElasticSearch) parseErrorResp(resp *esapi.Response) error {
	if resp.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
			return err
		}
		return fmt.Errorf("[%s] %s: %s", resp.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
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
