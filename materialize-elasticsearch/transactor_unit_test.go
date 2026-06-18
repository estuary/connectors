package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStoreActionDeltaUsesGeneratedID guards the delta-updates behavior: those
// stores intentionally use "create" with a blank ID so Elasticsearch generates
// a unique ID per document and multiple rows with the same key can be inserted.
func TestStoreActionDeltaUsesGeneratedID(t *testing.T) {
	line := string(storeAction(false /* exists */, true /* deltaUpdates */, "abc", nil))
	require.Contains(t, line, `"create"`, "delta-updates store must use the create action, got %q", line)
	require.NotContains(t, line, `"_id"`, "delta-updates store must use a generated (blank) ID, got %q", line)
}

// bulkResp builds a parsed bulk response from per-item error types, mirroring
// the shape Elasticsearch returns under the "items.*.error","items.*.status"
// filter path. An empty string means the item succeeded.
func bulkResp(t *testing.T, itemErrTypes ...string) *bulkResponse {
	t.Helper()
	var sb strings.Builder
	sb.WriteString(`{"items":[`)
	for i, et := range itemErrTypes {
		if i > 0 {
			sb.WriteString(",")
		}
		if et == "" {
			sb.WriteString(`{"index":{"status":200}}`)
		} else {
			sb.WriteString(fmt.Sprintf(`{"index":{"status":429,"error":{"type":%q,"reason":"r"}}}`, et))
		}
	}
	sb.WriteString(`]}`)
	var r bulkResponse
	require.NoError(t, json.Unmarshal([]byte(sb.String()), &r))
	return &r
}

func TestClassifyBulkResponse(t *testing.T) {
	const retryable = "retry_on_primary_exception"
	const terminalType = "mapper_parsing_exception"

	t.Run("clean", func(t *testing.T) {
		terminal, idx := bulkResp(t, "", "").classify()
		require.NoError(t, terminal)
		require.Empty(t, idx)
	})

	t.Run("all retryable", func(t *testing.T) {
		terminal, idx := bulkResp(t, retryable, "", retryable).classify()
		require.NoError(t, terminal)
		require.Equal(t, []int{0, 2}, idx)
	})

	t.Run("terminal wins over retryable", func(t *testing.T) {
		terminal, _ := bulkResp(t, retryable, terminalType).classify()
		require.Error(t, terminal)
		require.Contains(t, terminal.Error(), terminalType)
	})

	t.Run("terminal only", func(t *testing.T) {
		terminal, idx := bulkResp(t, terminalType).classify()
		require.Error(t, terminal)
		require.Empty(t, idx)
	})
}

// TestRetryBulkStoreResendsOnlyFailedItems is the regression test for the shard
// crashes: a transient per-item error (retry_on_primary_exception) in a 200
// response must be retried by re-sending only the failed item, not surfaced as
// fatal. Re-sending only the failed item is what makes this safe for delta
// bindings too (succeeded generated-ID creates are never re-sent).
func TestRetryBulkStoreResendsOnlyFailedItems(t *testing.T) {
	items := [][]byte{[]byte("A\n"), []byte("B\n"), []byte("C\n")}

	responses := []*bulkResponse{
		bulkResp(t, "", "retry_on_primary_exception", ""), // item 1 fails transiently
		bulkResp(t, ""),                                   // resent item succeeds
	}
	var sentBodies [][]byte
	send := func(body []byte) (*bulkResponse, error) {
		sentBodies = append(sentBodies, append([]byte(nil), body...))
		r := responses[0]
		responses = responses[1:]
		return r, nil
	}
	noDelay := func(context.Context, int) error { return nil }

	err := retryBulkStore(context.Background(), items, maxStoreRetries, send, noDelay)
	require.NoError(t, err)
	require.Len(t, sentBodies, 2, "should send the initial batch then a retry")
	require.Equal(t, []byte("A\nB\nC\n"), sentBodies[0], "first send is the whole batch")
	require.Equal(t, []byte("B\n"), sentBodies[1], "retry re-sends only the failed item")
}

// TestRetryBulkStoreFailsFastOnTerminal guards that a non-retryable item error
// is returned immediately without retrying.
func TestRetryBulkStoreFailsFastOnTerminal(t *testing.T) {
	items := [][]byte{[]byte("A\n")}

	var sends int
	send := func(body []byte) (*bulkResponse, error) {
		sends++
		return bulkResp(t, "mapper_parsing_exception"), nil
	}
	noDelay := func(context.Context, int) error { return nil }

	err := retryBulkStore(context.Background(), items, maxStoreRetries, send, noDelay)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mapper_parsing_exception")
	require.Equal(t, 1, sends, "terminal error must not be retried")
}

// TestRetryBulkStoreExhaustsAttempts guards that a persistently retryable error
// gives up after maxAttempts (rather than looping forever) and surfaces the
// underlying error.
func TestRetryBulkStoreExhaustsAttempts(t *testing.T) {
	items := [][]byte{[]byte("A\n")}

	var sends int
	send := func(body []byte) (*bulkResponse, error) {
		sends++
		return bulkResp(t, "retry_on_primary_exception"), nil
	}
	noDelay := func(context.Context, int) error { return nil }

	err := retryBulkStore(context.Background(), items, 3 /* maxAttempts */, send, noDelay)
	require.Error(t, err)
	require.Contains(t, err.Error(), "retry_on_primary_exception")
	require.Equal(t, 3, sends, "should attempt exactly maxAttempts times")
}
