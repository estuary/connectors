package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRejectedBatchError(t *testing.T) {
	fieldSizes := make(map[string]int)
	var batch []upsertDoc
	for key, doc := range map[string]map[string]interface{}{
		"0a": {"id": 1, "title": "short", "tags": []byte(`["a","b"]`)},
		"0b": {"id": 2, "title": "a longer title", "body": strings.Repeat("secret", 1000)},
	} {
		input, err := makeInput(doc, fieldSizes)
		require.NoError(t, err)
		batch = append(batch, upsertDoc{input: input, key: key})
	}

	openAiErr := errors.New("creating embeddings failed (400 Bad Request): '$.input' is invalid.")
	got := rejectedBatchError(binding{collection: "acmeCo/articles"}, batch, fieldSizes, openAiErr)

	require.ErrorIs(t, got, openAiErr)
	require.NotContains(t, got.Error(), "secret")
	require.Equal(t, `OpenAI rejected a batch of 2 documents from collection acmeCo/articles, most likely because a document is over the embedding model's limit of 8192 tokens. `+
		`The batch's largest document has vector ID 0b (6038 bytes) and its largest fields are "body" (6002 bytes), "title" (16 bytes), "tags" (9 bytes); `+
		`consider excluding large fields from this binding using field selection: `+
		`creating embeddings failed (400 Bad Request): '$.input' is invalid.`, got.Error())
}
