package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-pinecone/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/pinecone-io/go-pinecone/pinecone"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/structpb"
)

// TODO(whb): These may need tuning based on real-world use.
var (
	batchSize         = 100
	concurrentWorkers = 5
)

type transactor struct {
	openAiClient *client.OpenAiClient
	bindings     []binding

	group    *errgroup.Group
	groupCtx context.Context
}

type binding struct {
	conn        *pinecone.IndexConnection
	dataHeaders []string
}

type upsertDoc struct {
	input    string
	key      string
	metadata map[string]interface{}
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	t.group, t.groupCtx = errgroup.WithContext(ctx)
	t.group.SetLimit(concurrentWorkers)

	var batch []upsertDoc

	lastBinding := -1
	for it.Next() {
		if lastBinding == -1 {
			lastBinding = it.Binding
		}

		if it.Binding != lastBinding {
			if err := t.sendBatch(t.bindings[lastBinding], batch); err != nil {
				return nil, fmt.Errorf("sending batch of documents: %w", err)
			}
			batch = nil
			lastBinding = it.Binding
		}

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

		batch = append(batch, upsertDoc{
			input: embeddingInput,
			key:   fmt.Sprintf("%x", it.PackedKey),
			// Only the document is included as metadata.
			metadata: map[string]interface{}{
				"flow_document": string(it.RawJSON),
			},
		})

		if len(batch) >= batchSize {
			if err := t.sendBatch(b, batch); err != nil {
				return nil, fmt.Errorf("sending batch of documents: %w", err)
			}
			batch = nil
		}
	}

	if len(batch) != 0 {
		if err := t.sendBatch(t.bindings[lastBinding], batch); err != nil {
			return nil, fmt.Errorf("sending batch of documents: %w", err)
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

func (t *transactor) sendBatch(b binding, batch []upsertDoc) error {
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

			var vecs []*pinecone.Vector

			for _, e := range embeddings {
				thisUpsert := batch[e.Index]

				metadata, err := structpb.NewStruct(thisUpsert.metadata)
				if err != nil {
					return fmt.Errorf("creating metadata: %w", err)
				}

				vecs = append(vecs, &pinecone.Vector{
					Id:       thisUpsert.key,
					Values:   e.Embedding,
					Metadata: metadata,
				})
			}

			if count, err := b.conn.UpsertVectors(t.groupCtx, vecs); err != nil {
				return fmt.Errorf("pinecone upserting batch: %w", err)
			} else if int(count) != len(batch) {
				return fmt.Errorf("pinecone upserted %d vectors vs. expected %d", count, len(batch))
			}
			return nil
		})
	}

	return nil
}

func (t *transactor) Destroy() {}
