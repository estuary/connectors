package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

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
