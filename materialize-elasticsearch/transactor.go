package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	m "github.com/estuary/connectors/go/protocols/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// Elasticsearch does not allow keys longer than 512 bytes, so a 1MB batch will allow for at least
	// ~2,000 keys.
	loadBatchSize = 1 * 1024 * 1024 // Bytes
	loadWorkers   = 5

	storeBatchSize = 5 * 1024 * 1024 // Bytes
	storeWorkers   = 5
)

type binding struct {
	index        string
	deltaUpdates bool

	// Ordered list of field names included in the field selection for the binding, which are used
	// to build the JSON document to be stored Elasticsearch.
	fields []string

	// Index of fields that are Float values, materialized as ElasticSearch "double" mappings, that
	// must be checked for special +/-Infinity & NaN string values, as ElasticSearch does not handle
	// these and we'll replace them with NULL.
	floatFields []bool

	// Index of fields that are some type that cannot be materialized natively, and must be wrapped
	// in a synthetic object and materialized into a flattened mapping.
	wrapFields []bool

	// Present if the binding includes the root document, empty if not. This is usually the default
	// "flow_document" but may have an alternate user-defined projection name.
	docField string
}

type transactor struct {
	cfg          *config
	client       *client
	bindings     []binding
	isServerless bool

	// Used to correlate the binding number for loaded documents from Elasticsearch.
	indexToBinding map[string]int
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	ctx := it.Context()

	// We are evaluating loads as they come, so we must wait for the runtime's ack of the previous
	// commit.
	it.WaitForAcknowledged()

	var mu sync.Mutex
	loadFn := func(b int, d json.RawMessage) error {
		mu.Lock()
		defer mu.Unlock()
		return loaded(b, d)
	}

	batchCh := make(chan []getDoc)
	group, groupCtx := errgroup.WithContext(ctx)
	for range loadWorkers {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case batch, ok := <-batchCh:
					if !ok {
						return nil
					} else if err := t.loadDocs(ctx, batch, loadFn); err != nil {
						return err
					}
				}
			}
		})
	}

	sendBatch := func(b []getDoc) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batchCh <- b:
			return nil
		}
	}

	var batch []getDoc
	batchSize := 0
	for it.Next() {
		id := base64.RawStdEncoding.EncodeToString(it.PackedKey)
		batch = append(batch, getDoc{
			Id:     id,
			Index:  t.bindings[it.Binding].index,
			Source: t.bindings[it.Binding].docField,
		})
		batchSize += len(id)

		if batchSize > loadBatchSize {
			if err := sendBatch(batch); err != nil {
				return err
			}
			batch = nil
			batchSize = 0
		}
	}

	if len(batch) > 0 {
		if err := sendBatch(batch); err != nil {
			return err
		}
	}

	close(batchCh)
	return group.Wait()
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	type storeBatch struct {
		buf   []byte
		index string
	}

	batchCh := make(chan storeBatch)
	group, groupCtx := errgroup.WithContext(ctx)
	for range storeWorkers {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case batch, ok := <-batchCh:
					if !ok {
						return nil
					} else if err := t.doStore(groupCtx, batch.buf, batch.index); err != nil {
						return err
					}
				}
			}
		})
	}

	sendBatch := func(batch []byte, index string) error {
		select {
		case <-groupCtx.Done():
			return group.Wait()
		case batchCh <- storeBatch{buf: batch, index: index}:
			return nil
		}
	}

	var batch []byte
	var lastIndex string
	for it.Next() {
		b := t.bindings[it.Binding]

		if len(batch) > storeBatchSize || (lastIndex != b.index && lastIndex != "") {
			if err := sendBatch(batch, lastIndex); err != nil {
				return nil, err
			}
			batch = nil
		}
		lastIndex = b.index

		id := base64.RawStdEncoding.EncodeToString(it.PackedKey)
		if it.Delete && t.cfg.HardDelete {
			// Ignore items which do not exist and are already deleted.
			if it.Exists {
				batch = append(batch, []byte(`{"delete":{"_id":"`+id+`"}}`)...)
				batch = append(batch, '\n')
			}
			continue
		}

		// The "create" action will fail if an item by the provided ID already exists, and "index"
		// is like a PUT where it will create or replace. We could just use "index" all the time,
		// but using "create" when we believe the item does not already exist provides a bit of
		// extra consistency checking.
		if it.Exists {
			batch = append(batch, []byte(`{"index":{"_id":"`+id+`"}}`)...)
		} else if !b.deltaUpdates {
			batch = append(batch, []byte(`{"create":{"_id":"`+id+`"}}`)...)
		} else {
			// Leaving the ID blank will cause Elasticsearch to generate one automatically for
			// delta updates, where we otherwise could not insert multiple rows with the same ID.
			batch = append(batch, []byte(`{"create":{}}`)...)
		}
		batch = append(batch, '\n')

		doc := make(map[string]any)
		for idx, v := range append(it.Key, it.Values...) {
			if b, ok := v.([]byte); ok {
				v = json.RawMessage(b)
			}
			if s, ok := v.(string); b.floatFields[idx] && ok {
				// ElasticSearch does not supporting indexing these special Float values.
				if s == "Infinity" || s == "-Infinity" || s == "NaN" {
					v = nil
				}
			} else if b.wrapFields[idx] {
				v = map[string]any{"json": v}
			}

			doc[b.fields[idx]] = v
		}
		if b.docField != "" {
			doc[b.docField] = it.RawJSON
		}

		bodyData, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}
		batch = append(batch, bodyData...)
		batch = append(batch, '\n')
	}
	if err := it.Err(); err != nil {
		return nil, err
	}

	if len(batch) > 0 {
		if err := sendBatch(batch, lastIndex); err != nil {
			return nil, err
		}
	}

	close(batchCh)
	return nil, group.Wait()
}

func (t *transactor) Destroy() {}

type getDoc struct {
	Id     string `json:"_id"`
	Index  string `json:"_index"`
	Source string `json:"_source"`
}

type gotDoc struct {
	Id      string                     `json:"_id"`
	Index   string                     `json:"_index"`
	Found   bool                       `json:"found"`
	Source  map[string]json.RawMessage `json:"_source"`
	Error   *docError                  `json:"error,omitempty"`
	Timeout bool                       `json:"timed_out,omitempty"`
}

type docError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (t *transactor) loadDocs(ctx context.Context, getDocs []getDoc, loaded func(int, json.RawMessage) error) error {
	res, err := t.client.es.Mget(
		esutil.NewJSONReader(map[string][]getDoc{"docs": getDocs}),
		t.client.es.Mget.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("loadDocs mGet: %w", err)
	}
	defer res.Body.Close()
	if res.IsError() {
		return fmt.Errorf("loadDocs mGet error response [%s] %s", res.Status(), res.String())
	}

	// Stream the results of the request directly into the loaded callback from the response body.
	dec := json.NewDecoder(res.Body)

	// Read until the start of the response items array.
	for {
		if t, err := dec.Token(); err != nil {
			return err
		} else if t, ok := t.(json.Delim); ok {
			if t.String() == "[" {
				break
			}
		}

		if !dec.More() {
			return fmt.Errorf("response parsing error: did not find array of response items")
		}
	}

	gotCount := 0
	for dec.More() {
		var d gotDoc
		if err := dec.Decode(&d); err != nil {
			return fmt.Errorf("decoding batch response item: %w", err)
		}
		gotCount++

		if d.Error != nil {
			return fmt.Errorf("loadDocs error: %s (%s)", d.Error.Reason, d.Error.Type)
		} else if d.Timeout {
			log.WithField("source", d.Source).Warn("loadDocs timeout")
			return fmt.Errorf("loadDocs timeout")
		} else if !d.Found {
			// We get a response item for every document requested, even if it does not exist. If it
			// does not exist, nothing more needs to be done.
			continue
		}

		binding, ok := t.indexToBinding[d.Index]
		if !ok {
			return fmt.Errorf("invalid index name %q for loaded document id %q", d.Index, d.Id)
		}

		if len(d.Source) != 1 {
			// This should never be possible, since we ask for a single key from the source
			// document. It's here to make the intent of looping over values of d.Source below
			// explicit, since we'll only see a single value.
			log.WithField("gotDoc", d).Warn("unexpected number of returned source fields")
			return fmt.Errorf("unexpected number of returned source fields: %d", len(d.Source))
		}

		var docJson json.RawMessage
		for _, v := range d.Source {
			docJson = v
		}

		if err := loaded(binding, docJson); err != nil {
			return err
		}
	}

	// Sanity check: There should have been a response document for every input document, even if it
	// wasn't found.
	if len(getDocs) != gotCount {
		return fmt.Errorf("invalid Mget response: expected %d response docs but got %d", len(getDocs), gotCount)
	}

	return nil
}

type bulkResponse struct {
	Items []map[string]struct {
		Error struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	} `json:"items"`
}

func (b bulkResponse) firstError() error {
	for _, item := range b.Items {
		for action, err := range item {
			return fmt.Errorf("(%s) %s: %s", action, err.Error.Type, err.Error.Reason)
		}
	}

	return nil
}

func (t *transactor) doStore(ctx context.Context, body []byte, index string) error {
	opts := []func(*esapi.BulkRequest){
		t.client.es.Bulk.WithContext(ctx),
		// Request bodies are structured so that all documents belong to the
		// same index, which is set in the request path as a query parameter
		// rather than on each individual document.
		t.client.es.Bulk.WithIndex(index),
		// Without this filter, the response body will contain a bit of metadata
		// for every single item in the bulk request. We only care if errors
		// occurred, so this filter reduces the response size drastically in the
		// most common case of no or few errors.
		t.client.es.Bulk.WithFilterPath("items.*.error"),
	}

	if !t.isServerless {
		// Makes sure the changes are propagated to all replica shards.
		// TODO(whb): I'm not totally convinced we need to always be requiring this. It may only
		// really be applicable to case where there is a single replica shard and Elasticsearch
		// considers a quorum to be possible by writing only to the primary shard, which could
		// result in data loss if there is then a hardware failure on that single primary shard. At
		// the very least we could consider making this an advanced configuration option in the
		// future if it is problematic.
		opts = append(opts, t.client.es.Bulk.WithWaitForActiveShards("all"))
	}

	res, err := t.client.es.Bulk(bytes.NewReader(body), opts...)
	if err != nil {
		return fmt.Errorf("bulk request submission failed: %w", err)
	}
	defer res.Body.Close()

	var parsed bulkResponse
	if res.IsError() {
		return fmt.Errorf("bulk request to index %q failed with status %d: %s", index, res.StatusCode, res.String())
	} else if err := json.NewDecoder(res.Body).Decode(&parsed); err != nil {
		return fmt.Errorf("failed to decode bulk response: %w", err)
	} else if err := parsed.firstError(); err != nil {
		return fmt.Errorf("bulk request to index %q failed: %w", index, err)
	}

	return nil
}
