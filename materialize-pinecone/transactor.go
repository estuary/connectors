package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/estuary/connectors/materialize-pinecone/client"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/pkoukk/tiktoken-go"
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

	// Tokenizer for select embedding models. May be `nil` if the materialization is not configured
	// with a well-known embedding model.
	tokenizer        *tiktoken.Tiktoken
	maxAllowedTokens int

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

	truncatedCount := 0
	defer func() {
		if truncatedCount > 0 {
			log.WithFields(log.Fields{
				"maxAllowedTokens": t.maxAllowedTokens,
				"truncatedCount":   truncatedCount,
			}).Warn("documents were truncated for this transaction to comply with token limits. consider using a derivation for more precise splitting of input documents.")
		}
	}()

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

		input, truncated := truncateInput(embeddingInput, t.tokenizer, t.maxAllowedTokens)
		if truncated {
			truncatedCount += 1
		}

		batches[namespace] = append(batches[namespace], upsertDoc{
			input: input,
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

// truncateInput truncates the input string such that it tokenizes to a number of tokens less than
// maxAllowedTokens. If no tokenizer has been set, tokenizer will be nil and the input will not be
// truncated. Input strings are truncated proportionally to how much larger their tokenized count is
// to maxAllowedTokens. This may not always result in the longest possible input that still falls
// within maxAllowedTokens, but for reasonably large values for maxAllowedTokens the difference is
// not likely to be significant.
func truncateInput(input string, tokenizer *tiktoken.Tiktoken, maxAllowedTokens int) (string, bool) {
	if tokenizer == nil {
		return input, false
	}

	truncated := false

	for n := len(tokenizer.Encode(input, nil, nil)); n > maxAllowedTokens; n = len(tokenizer.Encode(input, nil, nil)) {
		bytesPerToken := float64(len(input)) / float64(n)
		wantByteLength := int(bytesPerToken * float64(maxAllowedTokens))

		// Don't corrupt the final rune in the input string by truncating in the middle of a
		// multi-byte character.
		for wantByteLength > 0 && !utf8.RuneStart(input[wantByteLength]) {
			wantByteLength--
		}

		input = input[:wantByteLength]
		truncated = true
	}

	return input, truncated
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
