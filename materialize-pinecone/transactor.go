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

type upserter interface {
	Upsert(context.Context, client.PineconeUpsertRequest) error
}

type embedder interface {
	CreateEmbeddings(context.Context, []string) ([]client.Embedding, error)
}

type transactor struct {
	pineconeClient upserter
	openAiClient   embedder
	bindings       []binding

	// Per-transaction variables reset by init.
	group           *errgroup.Group
	groupCtx        context.Context
	upserts         chan upsertDoc
	flushEmbeddings chan struct{}
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
	input     string
	key       string
	namespace string
	metadata  map[string]interface{}
}

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	ctx := it.Context()

	t.init(ctx)

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

		if err := t.sendDoc(ctx, upsertDoc{
			input:     input,
			key:       base64.RawURLEncoding.EncodeToString(it.PackedKey),
			namespace: t.bindings[it.Binding].namespace,
			metadata:  metadata,
		}); err != nil {
			return nil, err
		}
	}

	return nil, t.flushStores()
}

func (t *transactor) init(ctx context.Context) {
	t.upserts = make(chan upsertDoc)
	t.flushEmbeddings = make(chan struct{})

	t.group, t.groupCtx = errgroup.WithContext(ctx)
	t.group.Go(func() error {
		return t.embeddingsWorker(t.groupCtx, t.flushEmbeddings, t.upserts)
	})
}

func (t *transactor) sendDoc(ctx context.Context, u upsertDoc) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.groupCtx.Done():
		return t.group.Wait()
	case t.upserts <- u:
		return nil
	}
}

func (t *transactor) flushStores() error {
	close(t.flushEmbeddings)
	return t.group.Wait()
}

func (t *transactor) embeddingsWorker(ctx context.Context, flush <-chan struct{}, in <-chan upsertDoc) error {
	workers, workerCtx := errgroup.WithContext(ctx)
	workers.SetLimit(concurrentWorkers)

	batches := make(map[string][]upsertDoc)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-workerCtx.Done():
			return workers.Wait()
		case <-flush:
			for nn, batch := range batches {
				if len(batch) > 0 {
					t.flushEmbeddingsBatch(workerCtx, workers, batch, nn)
				}
			}
			return workers.Wait()
		case next := <-in:
			batches[next.namespace] = append(batches[next.namespace], next)

			if len(batches[next.namespace]) >= batchSize {
				// Copy for the goroutine we are going to run.
				bCopy := append([]upsertDoc(nil), batches[next.namespace]...)
				nn := next.namespace

				// Re-initialize the main loop batch buffer.
				batches[next.namespace] = batches[next.namespace][:0]

				t.flushEmbeddingsBatch(workerCtx, workers, bCopy, nn)
			}
		}
	}
}

func (t *transactor) flushEmbeddingsBatch(ctx context.Context, workers *errgroup.Group, batch []upsertDoc, namespace string) {
	input := make([]string, 0, len(batch))
	for _, r := range batch {
		input = append(input, r.input)
	}

	workers.Go(func() error {
		embeddings, err := t.openAiClient.CreateEmbeddings(ctx, input)
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

		if err := t.pineconeClient.Upsert(ctx, upsert); err != nil {
			return fmt.Errorf("pinecone upserting batch: %w", err)
		}
		return nil
	})
}

func (t *transactor) Destroy() {}
