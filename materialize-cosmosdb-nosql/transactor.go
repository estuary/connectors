package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf16"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pc "go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

const (
	idField               = "id"
	idFieldAlt            = "_flow_id"
	loadBatchSize         = 100
	concurrentLoadWorkers = 5
)

// systemFields are the properties Cosmos DB adds to every stored item.
var systemFields = []string{"_rid", "_self", "_etag", "_attachments", "_ts", "_lsn"}

type transactor struct {
	cfg      config
	client   *azcosmos.Client
	bindings []binding
	be       *m.BindingEvents
}

type binding struct {
	container         *azcosmos.ContainerClient
	path              []string
	stateKey          string
	containerRID      string
	deltaUpdates      bool
	partitionKey      string
	partitionKeyIndex int
	// unusable is set for a binding whose container did not exist or had a
	// different partition key when the transactor was built, or for a
	// standard-updates binding on an account whose consistency is too weak. It
	// may only drain prior state, never Load or Store.
	unusable error

	// Store records the latest session token per physical partition so the
	// next Load observes our writes, including after a restart. Only the single
	// store worker mutates it after restoring connector state.
	sessionTokens map[string]string
}

type connectorState struct {
	BindingStateV1 map[string]bindingState `json:"bindingStateV1"`
}

// bindingState ties session tokens to the container resource id that issued
// them, since a container recreated under the same name starts a new session.
type bindingState struct {
	ContainerRID  string            `json:"containerRid"`
	SessionTokens map[string]string `json:"sessionTokens"`
}

func (t *transactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	return nil, nil
}

func (t *transactor) UnmarshalState(raw json.RawMessage) error {
	var state connectorState
	if err := json.Unmarshal(raw, &state); err != nil {
		return err
	}
	for i := range t.bindings {
		b := &t.bindings[i]
		if s := state.BindingStateV1[b.stateKey]; s.SessionTokens != nil && s.ContainerRID == b.containerRID {
			b.sessionTokens = s.SessionTokens
		}
	}
	return nil
}

func (t *transactor) Flush(context.Context, map[int]time.Time) error { return nil }

func (t *transactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	return nil, nil
}

type loadRequest struct {
	binding int
	itemKey
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	it.WaitForAcknowledged()
	var batch []loadRequest
	for it.Next() {
		b := &t.bindings[it.Binding]
		if b.deltaUpdates {
			continue
		} else if b.unusable != nil {
			return b.unusable
		}
		key, err := b.itemKey(it.Key)
		if err != nil {
			return err
		}
		batch = append(batch, loadRequest{binding: it.Binding, itemKey: key})
		if len(batch) == loadBatchSize {
			if err := t.loadBatch(it.Context(), batch, loaded); err != nil {
				return err
			}
			batch = nil
		}
	}
	if err := it.Err(); err != nil {
		return err
	}
	return t.loadBatch(it.Context(), batch, loaded)
}

func (t *transactor) loadBatch(ctx context.Context, batch []loadRequest, loaded func(int, json.RawMessage) error) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(concurrentLoadWorkers)
	var mu sync.Mutex
	for _, req := range batch {
		group.Go(func() error {
			b := &t.bindings[req.binding]
			resp, err := b.container.ReadItem(req.context(groupCtx), req.partitionKey, req.id, b.readOptions())
			if isItemNotFound(err) {
				return nil
			} else if err != nil {
				return fmt.Errorf("reading item from container %q: %w", b.container.ID(), err)
			}
			doc, err := loadDocument(resp.Value)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			return loaded(req.binding, doc)
		})
	}
	return group.Wait()
}

type storeRequest struct {
	loadRequest
	doc    json.RawMessage
	delete bool
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx, cancel := context.WithCancel(it.Context())
	requests := make(chan storeRequest)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return t.storeWorker(groupCtx, it.Round, requests) })
	defer func() {
		cancel()
		_ = group.Wait()
	}()

	// A deletion must reach Cosmos even if this key has never been loaded.
	for it.Next(false) {
		b := &t.bindings[it.Binding]
		if b.unusable != nil {
			return nil, b.unusable
		}
		key, err := b.itemKey(it.Key)
		if err != nil {
			return nil, err
		}
		deleted := t.cfg.HardDelete && it.Delete
		var doc json.RawMessage
		if !deleted {
			var sourceDeleted bool
			doc, sourceDeleted, err = storeDocument(it.RawJSON, key.id)
			if err != nil {
				return nil, err
			}
			deleted = t.cfg.HardDelete && sourceDeleted
		}
		req := storeRequest{
			loadRequest: loadRequest{binding: it.Binding, itemKey: key},
			doc:         doc,
			delete:      deleted,
		}
		select {
		case <-groupCtx.Done():
			return nil, group.Wait()
		case requests <- req:
		}
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	close(requests)
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return t.startCommit, nil
}

func (t *transactor) startCommit(ctx context.Context, runtimeCheckpoint *pc.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
	state := connectorState{BindingStateV1: make(map[string]bindingState)}
	for _, b := range t.bindings {
		state.BindingStateV1[b.stateKey] = bindingState{ContainerRID: b.containerRID, SessionTokens: b.sessionTokens}
	}
	raw, err := json.Marshal(state)
	if err != nil {
		return nil, m.FinishedOperation(err)
	}
	return &pf.ConnectorState{UpdatedJson: raw}, nil
}

func (t *transactor) storeWorker(ctx context.Context, round int, requests <-chan storeRequest) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case req, ok := <-requests:
			if !ok {
				return nil
			}
			b := &t.bindings[req.binding]
			var resp azcosmos.ItemResponse
			var err error
			if req.delete {
				resp, err = b.container.DeleteItem(req.context(ctx), req.partitionKey, req.id, nil)
				if isItemNotFound(err) {
					// The store iterator counted this deletion, so report it too.
					t.be.ReportRowStats(round, b.path, m.TotalRowStats(1))
					continue
				}
			} else {
				resp, err = b.container.UpsertItem(req.context(ctx), req.partitionKey, req.doc, nil)
			}
			if err != nil {
				return fmt.Errorf("storing item in container %q: %w", b.container.ID(), err)
			}
			if resp.SessionToken != nil {
				for _, token := range strings.Split(*resp.SessionToken, ",") {
					if partition, _, ok := strings.Cut(token, ":"); ok {
						b.sessionTokens[partition] = token
					}
				}
			}
			t.be.ReportRowStats(round, b.path, m.TotalRowStats(1))
		}
	}
}

func (b *binding) readOptions() *azcosmos.ItemOptions {
	if len(b.sessionTokens) == 0 {
		return nil
	}
	tokens := make([]string, 0, len(b.sessionTokens))
	for _, token := range b.sessionTokens {
		tokens = append(tokens, token)
	}
	token := strings.Join(tokens, ",")
	return &azcosmos.ItemOptions{SessionToken: &token}
}

func (t *transactor) Destroy() { t.client.Close() }

// escapeKeyString escapes `~`, characters the Cosmos DB gateway rejects or
// decodes in item ids (`% : / \ ? #`), control characters and trailing
// whitespace as `~XX`. The id never contains `%`, which the gateway would
// decode once more when routing a point read or delete. A value of "." or ".."
// is fully escaped, since the request path would otherwise resolve it as a
// dot segment.
func escapeKeyString(s string) string {
	if s == "." || s == ".." {
		return strings.Repeat("~2E", len(s))
	}
	trailing := len(strings.TrimRightFunc(s, unicode.IsSpace))
	var out strings.Builder
	for i, r := range s {
		if i >= trailing || unicode.IsControl(r) || strings.ContainsRune(`~%:/\?#`, r) {
			for _, c := range []byte(string(r)) {
				fmt.Fprintf(&out, "~%02X", c)
			}
		} else {
			out.WriteRune(r)
		}
	}
	return out.String()
}

func encodeKey(key tuple.Tuple) (string, error) {
	parts := make([]string, len(key))
	for i, value := range key {
		switch v := value.(type) {
		case string:
			parts[i] = escapeKeyString(v)
		case nil:
			// Escaped strings always follow `~` with two hex digits, so they
			// cannot produce this sentinel.
			parts[i] = "~N"
		case bool, int, int64, uint64, float32, float64:
			encoded, err := json.Marshal(v)
			if err != nil {
				return "", fmt.Errorf("encoding key: %w", err)
			}
			parts[i] = string(encoded)
		default:
			return "", fmt.Errorf("unsupported key type %T", value)
		}
	}
	id := strings.Join(parts, ":")
	if id == "" && len(key) == 1 {
		// Cosmos DB rejects an empty id. Escaping never produces a lone `~`,
		// so it stands for a single empty string key.
		id = "~"
	}
	if id == "" {
		return "", fmt.Errorf("document key encodes to an empty Cosmos DB id")
	}
	return id, nil
}

type itemKey struct {
	id               string
	partitionKey     azcosmos.PartitionKey
	partitionKeyJSON string
}

func (k itemKey) context(ctx context.Context) context.Context {
	// The SDK's numeric partition key constructor accepts only float64.
	// Override its header through the SDK to preserve exact integer values.
	// The header is ASCII-only JSON, as the SDK sends it.
	return policy.WithHTTPHeader(ctx, http.Header{"x-ms-documentdb-partitionkey": {k.partitionKeyJSON}})
}

func (b *binding) itemKey(key tuple.Tuple) (itemKey, error) {
	id, err := encodeKey(key)
	if err != nil {
		return itemKey{}, err
	}
	var value any = id
	if b.partitionKey != "/id" {
		if b.partitionKeyIndex < 0 || b.partitionKeyIndex >= len(key) {
			return itemKey{}, fmt.Errorf("missing partition key %q in key tuple", b.partitionKey)
		}
		value = key[b.partitionKeyIndex]
	}
	var pk azcosmos.PartitionKey
	switch v := value.(type) {
	case nil:
		pk = azcosmos.NullPartitionKey
	case string:
		pk = azcosmos.NewPartitionKeyString(v)
	case bool:
		pk = azcosmos.NewPartitionKeyBool(v)
	case int:
		pk = azcosmos.NewPartitionKeyNumber(float64(v))
	case int64:
		pk = azcosmos.NewPartitionKeyNumber(float64(v))
	case uint64:
		pk = azcosmos.NewPartitionKeyNumber(float64(v))
	case float32:
		pk = azcosmos.NewPartitionKeyNumber(float64(v))
	case float64:
		pk = azcosmos.NewPartitionKeyNumber(v)
	default:
		return itemKey{}, fmt.Errorf("unsupported partition key type %T", v)
	}
	encoded, err := asciiJSON([]any{value})
	if err != nil {
		return itemKey{}, fmt.Errorf("encoding partition key: %w", err)
	}
	return itemKey{id: id, partitionKey: pk, partitionKeyJSON: encoded}, nil
}

// asciiJSON encodes v as JSON with non-ASCII characters escaped as \uXXXX,
// since HTTP header values are not reliably read as UTF-8.
func asciiJSON(v any) (string, error) {
	encoded, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	for _, r := range string(encoded) {
		// encoding/json leaves DEL unescaped, but net/http rejects it in headers.
		if r < 0x7f {
			out.WriteRune(r)
			continue
		}
		for _, u := range utf16.Encode([]rune{r}) {
			fmt.Fprintf(&out, `\u%04x`, u)
		}
	}
	return out.String(), nil
}

func storeDocument(raw json.RawMessage, id string) (json.RawMessage, bool, error) {
	// RawMessage keeps all numbers exact, including non-key numbers and nested values.
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, false, fmt.Errorf("decoding document: %w", err)
	} else if doc == nil {
		return nil, false, fmt.Errorf("document must be a JSON object")
	}
	if original, ok := doc[idField]; ok {
		doc[idFieldAlt] = original
	}
	doc[idField], _ = json.Marshal(id)

	var meta map[string]json.RawMessage
	var op string
	deleted := json.Unmarshal(doc["_meta"], &meta) == nil && json.Unmarshal(meta["op"], &op) == nil && op == "d"
	encoded, err := json.Marshal(doc)
	return encoded, deleted, err
}

func loadDocument(raw json.RawMessage) (json.RawMessage, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("decoding loaded document: %w", err)
	} else if doc == nil {
		return nil, fmt.Errorf("loaded document must be a JSON object")
	}
	delete(doc, idField)
	for _, field := range systemFields {
		delete(doc, field)
	}
	if original, ok := doc[idFieldAlt]; ok {
		doc[idField] = original
		delete(doc, idFieldAlt)
	}
	return json.Marshal(doc)
}
