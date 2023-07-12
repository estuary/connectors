package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

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
	namespace   string
	dataHeaders []string
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

		data := make(map[string]interface{})
		for idx, val := range allFields {
			if val != nil {
				data[b.dataHeaders[idx]] = val
			}
		}

		embeddingInput, err := makeInput(data)
		if err != nil {
			return nil, err
		}

		namespace := t.bindings[it.Binding].namespace

		batches[namespace] = append(batches[namespace], upsertDoc{
			input: embeddingInput,
			key:   base64.RawURLEncoding.EncodeToString(it.PackedKey),
			// Only the document is included as metadata.
			metadata: map[string]interface{}{
				"flow_document": string(it.RawJSON),
			},
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

// The embedding input is an aggregate string of all included keys and values of the
// materialization. It is arranged as "key: value" pairs on newlines with the value being the
// stringified JSON-encoded of the field value.
func makeInput(fields map[string]interface{}) (string, error) {
	var out strings.Builder

	count := 0
	for k, v := range fields {
		if c, ok := v.([]byte); ok {
			// We get JSON arrays and objects as raw bytes of their encoded JSON. Rather than
			// encoding that as a base64-string, we want to handle these bytes as precomputed JSON.
			v = json.RawMessage(c)
		}

		m, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("serializing input field: %w", err)
		}
		out.WriteString(k + ": " + string(m))

		count++
		if count < len(fields) {
			out.WriteString("\n")
		}
	}

	return out.String(), nil
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
