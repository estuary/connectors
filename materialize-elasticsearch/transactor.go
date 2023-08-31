package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/elastic/go-elasticsearch/v8/esutil"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// Elasticsearch does not allow keys longer than 512 bytes, so a 1MB batch will allow for at least
	// ~2,000 keys.
	loadBatchSize = 1 * 1024 * 1024 // Bytes
	loadWorkers   = 5

	// These parameters are used for the BulkIndexer that the Elasticsearch SDK provides.
	storeBatchSize = 512 * 1024 // Bytes
	storeWorkers   = 5
)

type binding struct {
	index        string
	deltaUpdates bool

	// Ordered list of field names included in the field selection for the binding, which are used
	// to build the JSON document to be stored Elasticsearch.
	fields []string

	// Present if the binding includes the root document, empty if not. This is usually the default
	// "flow_document" but may have an alternate user-defined projection name.
	docField string
}

type transactor struct {
	client   *client
	bindings []binding

	// Used to correlate the binding number for loaded documents from Elasticsearch.
	indexToBinding map[string]int
}

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
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
	for idx := 0; idx < loadWorkers; idx++ {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return groupCtx.Err()
				case batch := <-batchCh:
					if batch == nil { // Channel was closed
						return nil
					}

					if err := t.loadDocs(ctx, batch, loadFn); err != nil {
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

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()
	errCh := make(chan error, 1)

	indexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers: storeWorkers,
		FlushBytes: storeBatchSize,
		Client:     t.client.es,
		OnError: func(_ context.Context, err error) {
			log.WithField("error", err.Error()).Error("bulk indexer error")

			select {
			case errCh <- err:
			default:
			}
		},
		// Makes sure the changes are propagated to all replica shards.
		// TODO(whb): I'm totally convinced we need to always be requiring this. It may only really
		// be applicable to case where there is a single replica shard and Elasticsearch considers a
		// quorum to be possible by writing only to the primary shard, which could result in data
		// loss if there is then a hardware failure on that single primary shard. At the very least
		// we could consider making this an advanced configuration option in the future if it is
		// problematic.
		WaitForActiveShards: "all",
	})
	if err != nil {
		return nil, fmt.Errorf("creating bulk indexer: %w", err)
	}

	// If a single item fails from a batch, it's common for _all_ of the items to fail from that
	// batch. We'll get the first item failure error we see via errCh and report that as the
	// connector failure error.
	onItemFailure := func(_ context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
		if err == nil {
			// The err itself may be `nil` of the request was successful, but a failure is still
			// indicated by the error from the response body.
			err = errors.New(res.Error.Reason)
		}

		select {
		case errCh <- err:
		default:
		}
	}

	for it.Next() {
		select {
		case err := <-errCh:
			// Fail fast on errors with the bulk indexer or any items it has tried to process.
			return nil, fmt.Errorf("storing document: %w", err)
		default:
		}

		b := t.bindings[it.Binding]

		doc := make(map[string]any)

		for idx, v := range append(it.Key, it.Values...) {
			if b, ok := v.([]byte); ok {
				// An object or array field is received as raw JSON bytes. We currently only support
				// objects.
				v = json.RawMessage(b)
			}

			doc[b.fields[idx]] = v
		}
		if b.docField != "" {
			doc[b.docField] = it.RawJSON
		}

		var id string
		if !b.deltaUpdates {
			// Leave ID blank for delta updates so that ES will automatically generate one.
			id = base64.RawStdEncoding.EncodeToString(it.PackedKey)
		}

		// The "create" action will fail if an item by the provided ID already exists, and "index"
		// is like a PUT where it will create or replace. We could just use "index" all the time,
		// but using "create" when we believe the item does not already exist provides a bit of
		// extra consistency checking.
		action := "create"
		if it.Exists {
			action = "index"
		}

		// This needs to be a ReadSeeker - a streaming reader can't be used in
		// esutil.BulkIndexerItem.
		body, err := json.Marshal(doc)
		if err != nil {
			return nil, err
		}

		if err := indexer.Add(it.Context(), esutil.BulkIndexerItem{
			Index:      b.index,
			Action:     action,
			DocumentID: id,
			Body:       bytes.NewReader(body),
			OnFailure:  onItemFailure,
		}); err != nil {
			return nil, fmt.Errorf("adding item to bulk indexer: %w", err)
		}
	}

	if err := indexer.Close(ctx); err != nil {
		return nil, fmt.Errorf("closing bulk indexer: %w", err)
	}

	select {
	case err := <-errCh:
		return nil, fmt.Errorf("storing document after indexer close: %w", err)
	default:
	}

	// Sanity checks that everything went as expected.
	stats := indexer.Stats()
	if stats.NumFailed != 0 || stats.NumAdded != uint64(it.Total) || stats.NumIndexed+stats.NumCreated != uint64(it.Total) {
		log.WithFields(log.Fields{
			"stored":     it.Total,
			"numFailed":  stats.NumFailed,
			"numAdded":   stats.NumAdded,
			"numIndex":   stats.NumIndexed,
			"numCreated": stats.NumCreated,
		}).Info("indexer stats")
		return nil, fmt.Errorf("indexer stats did not report successful completion of all %d stored documents", it.Total)
	}

	return nil, nil
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
