package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/estuary/connectors/materialize-pinecone/client"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"golang.org/x/sync/errgroup"
)

// TODO(whb): These may need tuning based on real-world use.
var (
	batchSize         = 100
	concurrentWorkers = 5
)

type transactor struct {
	pineconeClient *client.PineconeClient
	openAiClient   *client.OpenAiClient
	bindings       []binding

	group    *errgroup.Group
	groupCtx context.Context
}

type binding struct {
	namespace string
	// Indexes the position of the "input" value from the combined list of keys + values. The value
	// at this position should be converted to a vector embedding. All values (including the
	// "input") will be included as metadata.
	inputIdx int
	// Metadata is provided as a set of key/value pairs. The "headers" are the keys and the values
	// are the values.
	metaHeaders []string
}

type upsertDoc struct {
	input    string
	key      string
	metadata map[string]interface{}
}

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()

	t.group, t.groupCtx = errgroup.WithContext(ctx)
	t.group.SetLimit(concurrentWorkers)

	batches := make(map[string][]upsertDoc)

	for it.Next() {
		b := t.bindings[it.Binding]

		allFields := append(it.Key, it.Values...)

		input, ok := allFields[b.inputIdx].(string)
		if !ok {
			return nil, fmt.Errorf("document value for embedding must be a string: was type %T", allFields[b.inputIdx])
		}

		metadata := make(map[string]interface{})
		for idx, val := range allFields {
			if val != nil {
				// Per Pinecone's docs, null metadata fields should be omitted from the payload
				// rather than explicitly set as null.
				metadata[b.metaHeaders[idx]] = val
			}
		}

		namespace := t.bindings[it.Binding].namespace

		batches[namespace] = append(batches[namespace], upsertDoc{
			input:    input,
			key:      base64.RawURLEncoding.EncodeToString(it.PackedKey),
			metadata: metadata,
		})

		if len(batches[namespace]) >= batchSize {
			if err := t.sendBatch(namespace, batches[namespace]); err != nil {
				return nil, fmt.Errorf("sending batch of documents: %w", err)
			}
			batches[namespace] = nil
		}
	}

	// Flush remaining partial batches.
	for namespace, batch := range batches {
		if len(batch) > 0 {
			if err := t.sendBatch(namespace, batch); err != nil {
				return nil, fmt.Errorf("flushing documents batch: %w", err)
			}
		}
	}

	return nil, t.group.Wait()
}

func (t *transactor) sendBatch(namespace string, batch []upsertDoc) error {
	select {
	case <-t.groupCtx.Done():
		return t.group.Wait()
	default:
		t.group.Go(func() error {
			input := make([]string, 0, len(batch))
			for _, r := range batch {
				input = append(input, r.input)
			}

			embeddings, err := t.openAiClient.CreateEmbeddings(t.groupCtx, input)
			if err != nil {
				return fmt.Errorf("openAI creating embeddings: %w", err)
			}

			upsert := client.PineconeUpsertRequest{
				Vectors:   []client.Vector{},
				Namespace: namespace,
			}

			for _, e := range embeddings {
				thisMeta := batch[e.Index]
				upsert.Vectors = append(upsert.Vectors, client.Vector{
					Id:       thisMeta.key,
					Values:   e.Embedding,
					Metadata: thisMeta.metadata,
				})
			}

			if err := t.pineconeClient.Upsert(t.groupCtx, upsert); err != nil {
				return fmt.Errorf("pinecone upserting batch: %w", err)
			}
			return nil
		})
	}

	return nil
}

func (t *transactor) Destroy() {}
