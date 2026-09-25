package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	m "github.com/estuary/connectors/go/materialize"
	embeddingclient "github.com/estuary/connectors/materialize-qdrant/client"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	qdrant "github.com/qdrant/go-client/qdrant"
	"golang.org/x/sync/errgroup"
)

const (
	batchSize         = 100
	concurrentWorkers = 5
)

type transactor struct {
	qdrantClient *qdrant.Client
	openAIClient embeddingsClient
	bindings     []binding
}

type embeddingsClient interface {
	CreateEmbeddings(context.Context, []string) ([]embeddingclient.Embedding, error)
}

type binding struct {
	collection  string
	vectorSize  int
	dataHeaders []string
}

type pendingPoint struct {
	input    string
	key      string
	document string
}

func (t *transactor) UnmarshalState(state json.RawMessage) error { return nil }

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	group, groupCtx := errgroup.WithContext(it.Context())
	group.SetLimit(concurrentWorkers)

	var batch []pendingPoint
	lastBinding := -1

	for it.Next(false) {
		if it.Binding < 0 || it.Binding >= len(t.bindings) {
			return nil, fmt.Errorf("store references invalid binding %d (have %d bindings)", it.Binding, len(t.bindings))
		}

		if lastBinding == -1 {
			lastBinding = it.Binding
		}
		if it.Binding != lastBinding {
			if err := t.sendBatch(group, groupCtx, t.bindings[lastBinding], batch); err != nil {
				return nil, fmt.Errorf("sending batch of documents: %w", err)
			}
			batch = nil
			lastBinding = it.Binding
		}

		b := t.bindings[it.Binding]
		allFields := append(it.Key, it.Values...)
		if len(allFields) != len(b.dataHeaders) {
			return nil, fmt.Errorf("binding %d has %d values but %d selected field headers", it.Binding, len(allFields), len(b.dataHeaders))
		}

		data := make(map[string]any, len(allFields))
		for idx, value := range allFields {
			if value != nil {
				data[b.dataHeaders[idx]] = value
			}
		}

		embeddingInput, err := makeInput(data)
		if err != nil {
			return nil, err
		}

		batch = append(batch, pendingPoint{
			input:    embeddingInput,
			key:      pointID(it.PackedKey),
			document: string(it.RawJSON),
		})

		if len(batch) >= batchSize {
			if err := t.sendBatch(group, groupCtx, b, batch); err != nil {
				return nil, fmt.Errorf("sending batch of documents: %w", err)
			}
			batch = nil
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterating stores: %w", err)
	}
	if len(batch) != 0 {
		if err := t.sendBatch(group, groupCtx, t.bindings[lastBinding], batch); err != nil {
			return nil, fmt.Errorf("sending batch of documents: %w", err)
		}
	}

	return nil, group.Wait()
}

// makeInput formats selected fields as sorted, JSON-encoded lines.
func makeInput(fields map[string]any) (string, error) {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var out strings.Builder
	for idx, key := range keys {
		value := fields[key]
		if bytes, ok := value.([]byte); ok {
			value = json.RawMessage(bytes)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", fmt.Errorf("serializing input field '%s': %w", key, err)
		}
		if idx != 0 {
			out.WriteByte('\n')
		}
		out.WriteString(key)
		out.WriteString(": ")
		out.Write(encoded)
	}
	return out.String(), nil
}

func (t *transactor) sendBatch(group *errgroup.Group, groupCtx context.Context, b binding, batch []pendingPoint) error {
	if len(batch) == 0 {
		return nil
	}
	select {
	case <-groupCtx.Done():
		return group.Wait()
	default:
		group.Go(func() error {
			input := make([]string, len(batch))
			for i, doc := range batch {
				input[i] = doc.input
			}

			embeddings, err := t.openAIClient.CreateEmbeddings(groupCtx, input)
			if err != nil {
				return fmt.Errorf("OpenAI creating embeddings: %w", err)
			}
			points, err := buildPoints(b, batch, embeddings)
			if err != nil {
				return err
			}

			wait := true
			if _, err := t.qdrantClient.Upsert(groupCtx, &qdrant.UpsertPoints{
				CollectionName: b.collection,
				Wait:           &wait,
				Points:         points,
			}); err != nil {
				return fmt.Errorf("Qdrant upserting batch: %w", err)
			}
			return nil
		})
	}
	return nil
}

func buildPoints(b binding, batch []pendingPoint, embeddings []embeddingclient.Embedding) ([]*qdrant.PointStruct, error) {
	if len(embeddings) != len(batch) {
		return nil, fmt.Errorf("OpenAI returned %d embeddings for %d inputs", len(embeddings), len(batch))
	}

	points := make([]*qdrant.PointStruct, 0, len(embeddings))
	seenIndexes := make([]bool, len(batch))
	for _, embedding := range embeddings {
		if embedding.Index < 0 || embedding.Index >= len(batch) {
			return nil, fmt.Errorf("OpenAI returned invalid embedding index %d", embedding.Index)
		}
		if seenIndexes[embedding.Index] {
			return nil, fmt.Errorf("OpenAI returned duplicate embedding index %d", embedding.Index)
		}
		seenIndexes[embedding.Index] = true
		if len(embedding.Embedding) != b.vectorSize {
			return nil, fmt.Errorf("OpenAI returned embedding with dimensions %d, but collection '%s' requires %d", len(embedding.Embedding), b.collection, b.vectorSize)
		}
		doc := batch[embedding.Index]
		points = append(points, &qdrant.PointStruct{
			Id:      qdrant.NewIDUUID(doc.key),
			Vectors: qdrant.NewVectorsDense(embedding.Embedding),
			Payload: map[string]*qdrant.Value{
				"flow_document": qdrant.NewValueString(doc.document),
			},
		})
	}
	return points, nil
}

func (t *transactor) Destroy() {
	if t.qdrantClient != nil {
		_ = t.qdrantClient.Close()
	}
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func pointID(packedKey []byte) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, packedKey).String()
}
