package main

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/estuary/connectors/materialize-pinecone/client"
	"github.com/stretchr/testify/require"
)

func TestTransactor(t *testing.T) {
	batchSize = 3

	ctx := context.Background()

	pc := dummyPcClient{}
	oc := dummyOAIClient{}

	tt := &transactor{
		pineconeClient: &pc,
		openAiClient:   &oc,
		bindings: []binding{
			{
				namespace: "first",
			},
			{
				namespace: "second",
			},
			{
				namespace: "third",
			},
		},
	}

	t.Run("single doc", func(t *testing.T) {
		pc.reset()
		oc.reset()

		tt.init(ctx)

		tt.sendDoc(ctx, upsertDoc{
			input:     "content",
			key:       "key",
			namespace: "first",
		})

		require.NoError(t, tt.flushStores())
		require.Equal(t, 1, pc.calls)
		require.Equal(t, 1, oc.calls)
	})

	t.Run("many docs to the same binding", func(t *testing.T) {
		oc.reset()
		pc.reset()

		tt.init(ctx)

		for idx := 0; idx < 20; idx++ {
			tt.sendDoc(ctx, upsertDoc{
				input:     fmt.Sprintf("content %d", idx),
				key:       fmt.Sprintf("key_%d", idx),
				namespace: "first",
			})
		}

		require.NoError(t, tt.flushStores())
		require.Equal(t, 7, oc.calls)
		require.Equal(t, 7, pc.calls)
	})

	t.Run("many docs to many bindings", func(t *testing.T) {
		oc.reset()
		pc.reset()

		tt.init(ctx)

		for idx := 0; idx < 20; idx++ {
			tt.sendDoc(ctx, upsertDoc{
				input:     fmt.Sprintf("content %d", idx),
				key:       fmt.Sprintf("key_%d", idx),
				namespace: "first",
			})

			tt.sendDoc(ctx, upsertDoc{
				input:     fmt.Sprintf("content %d", idx),
				key:       fmt.Sprintf("key_%d", idx),
				namespace: "second",
			})

			tt.sendDoc(ctx, upsertDoc{
				input:     fmt.Sprintf("content %d", idx),
				key:       fmt.Sprintf("key_%d", idx),
				namespace: "third",
			})
		}

		require.NoError(t, tt.flushStores())
		require.Equal(t, 21, oc.calls)
		require.Equal(t, 21, pc.calls)
	})
}

var mu sync.Mutex

type dummyOAIClient struct {
	calls int
}

func (c *dummyOAIClient) reset() {
	c.calls = 0
}

func (c *dummyOAIClient) CreateEmbeddings(context.Context, []string) ([]client.Embedding, error) {
	mu.Lock()
	defer mu.Unlock()
	c.calls++
	return nil, nil
}

type dummyPcClient struct {
	calls int
}

func (c *dummyPcClient) reset() {
	c.calls = 0
}

func (c *dummyPcClient) Upsert(context.Context, client.PineconeUpsertRequest) error {
	mu.Lock()
	defer mu.Unlock()
	c.calls++
	return nil
}
