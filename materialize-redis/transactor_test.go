package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisKey(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix string
		key    tuple.Tuple
		want   string
	}{
		{"integer", "orders", tuple.Tuple{int64(1)}, "orders:1"},
		{"composite", "events", tuple.Tuple{"us", int64(7)}, "events:us:7"},
		{"string unchanged", "lookup", tuple.Tuple{"a:b 雪\""}, "lookup:a:b 雪\""},
		{"empty string", "lookup", tuple.Tuple{""}, "lookup:"},
		{"scalar types", "lookup", tuple.Tuple{true, false, -3.5, int64(-9), nil}, "lookup:true:false:-3.5:-9:null"},
		{"integer precision", "lookup", tuple.Tuple{uint64(math.MaxUint64)}, "lookup:18446744073709551615"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key, err := redisKey(tt.prefix, tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.want, key)
		})
	}
	_, err := redisKey("lookup", tuple.Tuple{math.Inf(1)})
	require.ErrorContains(t, err, "encoding Redis key")
	_, err = redisKey("lookup", tuple.Tuple{[]byte(`{}`)})
	require.ErrorContains(t, err, "unsupported Redis key type")
}

func TestLoad(t *testing.T) {
	client := testClient(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
		require.Equal(t, []any{"mget", "orders:1", "events:us:7", "orders:9"}, cmd.Args())
		cmd.(*redis.SliceCmd).SetVal([]any{`{"id":1}`, `{"region":"us","seq":7}`, nil})
		return nil
	}})
	tx := &transactor{client: client, bindings: []binding{{}, {}}}
	var bindings []int
	var docs []string
	err := tx.load(context.Background(), []string{"orders:1", "events:us:7", "orders:9"}, []int{0, 1, 0}, func(binding int, doc json.RawMessage) error {
		bindings = append(bindings, binding)
		docs = append(docs, string(doc))
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, bindings)
	require.Equal(t, []string{`{"id":1}`, `{"region":"us","seq":7}`}, docs)
}

func TestLoadErrors(t *testing.T) {
	for _, tt := range []struct {
		name        string
		value       any
		redisErr    error
		callbackErr error
		wantErr     string
	}{
		{"Redis failure", nil, errors.New("load failed"), nil, "load failed"},
		{"invalid JSON", "not JSON", nil, nil, "does not contain a JSON document"},
		{"unexpected type", int64(1), nil, nil, "does not contain a JSON document"},
		{"callback failure", `{"id":1}`, nil, errors.New("callback failed"), "callback failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := testClient(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				cmd.(*redis.SliceCmd).SetVal([]any{tt.value})
				return tt.redisErr
			}})
			tx := &transactor{client: client, bindings: []binding{{}, {}}}
			err := tx.load(context.Background(), []string{"orders:1"}, []int{0}, func(int, json.RawMessage) error { return tt.callbackErr })
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestStore(t *testing.T) {
	for _, hardDelete := range []bool{false, true} {
		t.Run(fmt.Sprintf("hardDelete=%t", hardDelete), func(t *testing.T) {
			var commands [][]any
			client := testClient(t, redisHook{pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
				for _, cmd := range cmds {
					commands = append(commands, cmd.Args())
				}
				return nil
			}})
			tx := &transactor{
				client:     client,
				hardDelete: hardDelete,
				be:         m.NewBindingEvents(),
				bindings: []binding{
					{resource: resource{KeyPrefix: "orders"}, path: []string{"orders"}},
					{resource: resource{KeyPrefix: "events", TTLSeconds: 86400}, path: []string{"events"}},
					{resource: resource{KeyPrefix: "delta", DeltaUpdates: true}, path: []string{"delta"}},
				},
			}
			deletion := storeRequest(0, tuple.Tuple{int64(3)}, `{"id":3,"_meta":{"op":"d"}}`)
			deletion.Store.Delete, deletion.Store.Exists = true, true
			deltaDeletion := storeRequest(2, tuple.Tuple{int64(4)}, `{"id":4,"_meta":{"op":"d"}}`)
			deltaDeletion.Store.Delete = true
			it := storeIterator(
				storeRequest(0, tuple.Tuple{int64(1)}, " { \"id\": 1, \"status\": \"new\" }\n"),
				storeRequest(1, tuple.Tuple{"us", int64(7)}, `{"region":"us","seq":7}`),
				deletion,
				deltaDeletion,
			)
			commit, err := tx.Store(it)
			require.NoError(t, err)
			require.Nil(t, commit)
			want := [][]any{
				{"set", "orders:1", `{"id":1,"status":"new"}`},
				{"set", "events:us:7", `{"region":"us","seq":7}`, "ex", int64(86400)},
			}
			if hardDelete {
				want = append(want, []any{"del", "orders:3"}, []any{"del", "delta:4"})
			} else {
				want = append(want, []any{"set", "orders:3", string(deletion.Store.DocJson)}, []any{"set", "delta:4", string(deltaDeletion.Store.DocJson)})
			}
			require.Equal(t, want, commands)
		})
	}
}

func TestStoreDeletes(t *testing.T) {
	for _, hardDelete := range []bool{false, true} {
		for _, deltaUpdates := range []bool{false, true} {
			t.Run(fmt.Sprintf("hardDelete=%t/deltaUpdates=%t", hardDelete, deltaUpdates), func(t *testing.T) {
				for _, tt := range []struct {
					name           string
					doc            string
					exists         bool
					protocolDelete bool
					wantDelete     bool
				}{
					{"source deletion loaded", `{"id":3,"_meta":{"op":"d"}}`, true, false, true},
					{"source deletion not loaded", `{"id":3,"_meta":{"op":"d"}}`, false, false, true},
					{"escaped source deletion", `{"id":3,"_meta":{"op":"\u0064"}}`, true, false, true},
					{"protocol deletion loaded", `{"id":3}`, true, true, true},
					{"protocol deletion not loaded", `{"id":3}`, false, true, true},
					{"update", `{"id":3,"_meta":{"op":"u"}}`, true, false, false},
					{"no operation", `{"id":3,"_meta":{"uuid":"test-uuid"}}`, true, false, false},
					{"no metadata", `{"id":3}`, false, false, false},
					{"null metadata", `{"id":3,"_meta":null}`, false, false, false},
					{"non-string operation", `{"id":3,"_meta":{"op":["d"]}}`, true, false, false},
					{"nested operation", `{"id":3,"data":{"_meta":{"op":"d"}}}`, true, false, false},
					{"top-level operation", `{"id":3,"op":"d"}`, true, false, false},
				} {
					t.Run(tt.name, func(t *testing.T) {
						var commands [][]any
						client := testClient(t, redisHook{pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
							for _, cmd := range cmds {
								commands = append(commands, cmd.Args())
							}
							return nil
						}})
						tx := &transactor{
							client:     client,
							hardDelete: hardDelete,
							be:         m.NewBindingEvents(),
							bindings: []binding{{
								resource: resource{KeyPrefix: "orders", TTLSeconds: 86400, DeltaUpdates: deltaUpdates},
								path:     []string{"orders"},
							}},
						}
						request := storeRequest(0, tuple.Tuple{int64(3)}, tt.doc)
						request.Store.Exists, request.Store.Delete = tt.exists, tt.protocolDelete
						commit, err := tx.Store(storeIterator(request))
						require.NoError(t, err)
						require.Nil(t, commit)
						if hardDelete && tt.wantDelete {
							require.Equal(t, [][]any{{"del", "orders:3"}}, commands)
						} else {
							require.Equal(t, [][]any{{"set", "orders:3", tt.doc, "ex", int64(86400)}}, commands)
						}
					})
				}
			})
		}
	}
}

func TestStoreBatches(t *testing.T) {
	var sizes []int
	client := testClient(t, redisHook{pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
		sizes = append(sizes, len(cmds))
		return nil
	}})
	tx := &transactor{client: client, be: m.NewBindingEvents(), bindings: []binding{{resource: resource{KeyPrefix: "orders"}, path: []string{"orders"}}}}
	var requests []*pm.Request
	for i := 0; i < storeBatchSize+1; i++ {
		requests = append(requests, storeRequest(0, tuple.Tuple{int64(i)}, fmt.Sprintf(`{"id":%d}`, i)))
	}
	_, err := tx.Store(storeIterator(requests...))
	require.NoError(t, err)
	require.Equal(t, []int{storeBatchSize, 1}, sizes)
}

func TestStoreLargeDocuments(t *testing.T) {
	for _, tt := range []struct {
		name  string
		size  int
		count int
	}{
		{"large batch", 200 * 1024, 100},
		{"document larger than batch limit", 9 * 1024 * 1024, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			doc := `{"data":"` + strings.Repeat("x", tt.size) + `"}`
			var keys []string
			client := testClient(t, redisHook{pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
				var size int
				for _, cmd := range cmds {
					args := cmd.Args()
					require.Equal(t, "set", args[0])
					require.Equal(t, doc, args[2])
					keys = append(keys, args[1].(string))
					size += len(args[2].(string))
				}
				if len(cmds) > 1 && size > 8*1024*1024 {
					return fmt.Errorf("pipeline contains %d document bytes", size)
				}
				return nil
			}})
			tx := &transactor{client: client, be: m.NewBindingEvents(), bindings: []binding{{resource: resource{KeyPrefix: "orders"}, path: []string{"orders"}}}}
			var requests []*pm.Request
			var wantKeys []string
			for i := 0; i < tt.count; i++ {
				requests = append(requests, storeRequest(0, tuple.Tuple{int64(i)}, " \n"+doc+"\n "))
				wantKeys = append(wantKeys, fmt.Sprintf("orders:%d", i))
			}
			_, err := tx.Store(storeIterator(requests...))
			require.NoError(t, err)
			require.Equal(t, wantKeys, keys)
		})
	}
}

func TestStoreErrors(t *testing.T) {
	client := testClient(t, redisHook{pipeline: func(context.Context, []redis.Cmder) error {
		return errors.New("pipeline failed")
	}})
	tx := &transactor{client: client, bindings: []binding{{resource: resource{KeyPrefix: "orders"}}}}
	_, err := tx.Store(storeIterator(storeRequest(0, tuple.Tuple{int64(1)}, `{"id":1}`)))
	require.ErrorContains(t, err, "pipeline failed")
	_, err = tx.Store(storeIterator(storeRequest(0, tuple.Tuple{int64(1)}, `not JSON`)))
	require.ErrorContains(t, err, "compacting document")

	stream := &fakeStream{requests: []*pm.Request{storeRequest(0, tuple.Tuple{int64(1)}, `{"id":1}`)}}
	_, err = tx.Store(m.NewStoreIterator(context.Background(), stream, &pm.Request{Flush: &pm.Request_Flush{}}))
	require.ErrorContains(t, err, "reading Store: EOF")
}

type redisHook struct {
	process  redis.ProcessHook
	pipeline redis.ProcessPipelineHook
}

func (h redisHook) DialHook(redis.DialHook) redis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("unit tests must not connect to Redis")
	}
}

func (h redisHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	if h.process != nil {
		return h.process
	}
	return func(context.Context, redis.Cmder) error { return errors.New("unexpected Redis command") }
}

func (h redisHook) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	if h.pipeline != nil {
		return h.pipeline
	}
	return func(context.Context, []redis.Cmder) error { return errors.New("unexpected Redis pipeline") }
}

func testClient(t *testing.T, hook redisHook) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "redis.invalid:6379"})
	client.AddHook(hook)
	t.Cleanup(func() { client.Close() })
	return client
}

func storeRequest(binding int, key tuple.Tuple, doc string) *pm.Request {
	return &pm.Request{Store: &pm.Request_Store{
		Binding:      uint32(binding),
		KeyPacked:    key.Pack(),
		ValuesPacked: tuple.Tuple{}.Pack(),
		DocJson:      json.RawMessage(doc),
	}}
}

func storeIterator(requests ...*pm.Request) *m.StoreIterator {
	stream := &fakeStream{requests: append(requests, &pm.Request{StartCommit: &pm.Request_StartCommit{}})}
	return m.NewStoreIterator(context.Background(), stream, &pm.Request{Flush: &pm.Request_Flush{}})
}

type fakeStream struct {
	requests []*pm.Request
}

func (s *fakeStream) Send(*pm.Response) error { return nil }

func (s *fakeStream) RecvMsg(r *pm.Request) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	*r, s.requests = *s.requests[0], s.requests[1:]
	return nil
}
