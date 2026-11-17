package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/gjson"
)

const (
	// MGET response sizes are unknown until the documents are loaded.
	loadBatchSize   = 40
	storeBatchSize  = 1000
	storeBatchBytes = 8 * 1024 * 1024
)

type binding struct {
	resource
	path []string
}

type transactor struct {
	client     *redis.Client
	bindings   []binding
	hardDelete bool
	be         *m.BindingEvents
}

var _ m.Transactor = &transactor{}

func (t *transactor) RecoverCheckpoint(context.Context, pf.MaterializationSpec, pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(json.RawMessage) error { return nil }
func (t *transactor) Flush(context.Context, map[int]time.Time) error {
	return nil
}

func (t *transactor) Acknowledge(context.Context, []json.RawMessage, []string) (*pf.ConnectorState, error) {
	return nil, nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	it.WaitForAcknowledged()
	keys := make([]string, 0, loadBatchSize)
	bindings := make([]int, 0, loadBatchSize)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		if err := t.load(it.Context(), keys, bindings, loaded); err != nil {
			return err
		}
		keys, bindings = keys[:0], bindings[:0]
		return nil
	}

	for it.Next() {
		b := t.bindings[it.Binding]
		if b.DeltaUpdates {
			return fmt.Errorf("unexpected load for delta-updates binding %q", b.KeyPrefix)
		}
		key, err := b.redisKey(it.Key)
		if err != nil {
			return err
		}
		if len(bindings) > 0 && (t.bindings[bindings[0]].Format == "json") != (b.Format == "json") {
			if err := flush(); err != nil {
				return err
			}
		}
		keys = append(keys, key)
		bindings = append(bindings, it.Binding)
		if len(keys) == loadBatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	return flush()
}

func (t *transactor) load(ctx context.Context, keys []string, bindings []int, loaded func(int, json.RawMessage) error) error {
	var values []any
	var err error
	if t.bindings[bindings[0]].Format == "json" {
		// The legacy root path returns a document, without JSONPath's array wrapper.
		values, err = t.client.JSONMGet(ctx, ".", keys...).Result()
	} else {
		values, err = t.client.MGet(ctx, keys...).Result()
	}
	if err != nil {
		return fmt.Errorf("loading Redis keys: %w", err)
	}
	for i, value := range values {
		if value == nil {
			continue
		}
		doc, ok := value.(string)
		if !ok || !json.Valid([]byte(doc)) {
			return fmt.Errorf("Redis key %q does not contain a JSON document", keys[i])
		}
		if err := loaded(bindings[i], json.RawMessage(doc)); err != nil {
			return err
		}
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()
	pipe := t.client.Pipeline()
	for _, b := range t.bindings {
		if b.Format == "json" {
			// Keep JSON.SET and its expiration change atomic, as with SET ... EX.
			pipe = t.client.TxPipeline()
			break
		}
	}
	defer pipe.Discard()
	stored := make([]int64, len(t.bindings))
	batchBytes := 0
	flush := func() error {
		if pipe.Len() == 0 {
			return nil
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("storing Redis keys: %w", err)
		}
		batchBytes = 0
		return nil
	}

	// DEL is idempotent, including for delta updates where Exists is always false.
	for it.Next(false) {
		b := t.bindings[it.Binding]
		key, err := b.redisKey(it.Key)
		if err != nil {
			return nil, err
		}
		// Source deletions may only be indicated by the document's _meta.op.
		if t.hardDelete && (it.Delete || gjson.GetBytes(it.RawJSON, "_meta.op").Str == "d") {
			pipe.Del(ctx, key)
		} else {
			var doc bytes.Buffer
			if err := json.Compact(&doc, it.RawJSON); err != nil {
				return nil, fmt.Errorf("compacting document: %w", err)
			}
			if batchBytes+doc.Len() > storeBatchBytes {
				if err := flush(); err != nil {
					return nil, err
				}
			}
			if b.Format == "json" {
				pipe.JSONSet(ctx, key, "$", doc.String())
				// JSON.SET preserves an existing TTL, unlike SET.
				if b.TTLSeconds > 0 {
					pipe.Expire(ctx, key, time.Duration(b.TTLSeconds)*time.Second)
				} else {
					pipe.Persist(ctx, key)
				}
			} else {
				pipe.Set(ctx, key, doc.String(), time.Duration(b.TTLSeconds)*time.Second)
			}
			batchBytes += doc.Len()
		}
		stored[it.Binding]++
		if pipe.Len() >= storeBatchSize || batchBytes >= storeBatchBytes {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	if err := flush(); err != nil {
		return nil, err
	}
	for i, n := range stored {
		if n > 0 {
			t.be.ReportRowStats(it.Round, t.bindings[i].path, m.TotalRowStats(n))
		}
	}
	return nil, nil
}

func (t *transactor) Destroy() {
	t.client.Close()
}

func (r resource) redisKey(key tuple.Tuple) (string, error) {
	if r.KeyEncoding != "json" {
		return redisKey(r.KeyPrefix, key)
	}
	for _, value := range key {
		switch value.(type) {
		case nil, string, bool, int, int64, uint64, float32, float64, json.Number:
		default:
			return "", fmt.Errorf("unsupported Redis key type %T", value)
		}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(append(tuple.Tuple{}, key...)); err != nil {
		return "", fmt.Errorf("encoding Redis key: %w", err)
	}
	// Keep Unicode separators literal without changing escaped backslashes.
	encoded := strings.NewReplacer(`\\`, `\\`, `\u2028`, "\u2028", `\u2029`, "\u2029").Replace(strings.TrimSuffix(buf.String(), "\n"))
	return r.KeyPrefix + ":" + encoded, nil
}

func redisKey(prefix string, key tuple.Tuple) (string, error) {
	var out strings.Builder
	out.WriteString(prefix)
	for _, value := range key {
		out.WriteByte(':')
		if s, ok := value.(string); ok {
			out.WriteString(s)
			continue
		}
		switch value.(type) {
		case nil, bool, int, int64, uint64, float32, float64, json.Number:
			encoded, err := json.Marshal(value)
			if err != nil {
				return "", fmt.Errorf("encoding Redis key: %w", err)
			}
			out.Write(encoded)
		default:
			return "", fmt.Errorf("unsupported Redis key type %T", value)
		}
	}
	return out.String(), nil
}
