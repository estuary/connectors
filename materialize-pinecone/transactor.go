package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/protocols/materialize"
	"github.com/estuary/connectors/materialize-pinecone/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
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

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ts time.Time
	count := 0
	for it.Next() {
		count += 1
		if ts.IsZero() {
			ts = time.Now()
		}
	}

	log.WithFields(log.Fields{
		"count": count,
		"took":  time.Since(ts).String(),
	}).Info("finished txn")

	return nil, nil
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
