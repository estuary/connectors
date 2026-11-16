package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestJSONKeyEncoding(t *testing.T) {
	res := resource{KeyPrefix: "orders", KeyEncoding: "json"}
	seen := make(map[string]bool)
	for _, key := range []tuple.Tuple{
		{"a:b", "c"}, {"a", "b:c"}, {"1"}, {int64(1)}, {true}, {"true"}, {nil}, {"null"},
		{""}, {}, {"", ""}, {"\"],:[", "\\雪"}, {uint64(math.MaxUint64)},
	} {
		encoded, err := res.redisKey(key)
		require.NoError(t, err)
		require.False(t, seen[encoded], "key collision: %s", encoded)
		seen[encoded] = true
		var decoded []any
		decoder := json.NewDecoder(strings.NewReader(strings.TrimPrefix(encoded, "orders:")))
		decoder.UseNumber()
		require.NoError(t, decoder.Decode(&decoded))
		require.Len(t, decoded, len(key))
	}
	encoded, err := res.redisKey(tuple.Tuple{"a:b", int64(1)})
	require.NoError(t, err)
	require.Equal(t, `orders:["a:b",1]`, encoded)
	for _, tt := range []struct {
		name string
		key  tuple.Tuple
		want string
	}{
		{"HTML characters", tuple.Tuple{"a&b<c>"}, `orders:["a&b<c>"]`},
		{"Unicode", tuple.Tuple{"https://x.com/?a=1&b=<2>", "雪\u2028\u2029"}, "orders:[\"https://x.com/?a=1&b=<2>\",\"雪\u2028\u2029\"]"},
		{"literal escapes", tuple.Tuple{`\u2028`, `\u2029`, "\\\u2028", "\\\\\u2029"}, "orders:[\"\\\\u2028\",\"\\\\u2029\",\"\\\\\u2028\",\"\\\\\\\\\u2029\"]"},
		{"JSON escapes", tuple.Tuple{"\"\\/\b\f\n\r\t\x00\x1f"}, `orders:["\"\\/\b\f\n\r\t\u0000\u001f"]`},
		{"numbers", tuple.Tuple{uint64(math.MaxUint64), 1.0, 1e-6, 1e-7, 1e20, 1e21, json.Number("1.50")}, `orders:[18446744073709551615,1,0.000001,1e-7,100000000000000000000,1e+21,1.50]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := res.redisKey(tt.key)
			require.NoError(t, err)
			require.Equal(t, tt.want, encoded)
		})
	}
	for _, value := range []any{math.Inf(1), []byte("x"), map[string]any{"a": 1}} {
		_, err := res.redisKey(tuple.Tuple{value})
		require.Error(t, err)
	}
	res.KeyEncoding = "legacy"
	encoded, err = res.redisKey(tuple.Tuple{"a:b", int64(1)})
	require.NoError(t, err)
	require.Equal(t, "orders:a:b:1", encoded)
}

func TestJSONStore(t *testing.T) {
	for _, delta := range []bool{false, true} {
		for _, hardDelete := range []bool{false, true} {
			t.Run(fmt.Sprintf("delta=%t/hardDelete=%t", delta, hardDelete), func(t *testing.T) {
				var commands [][]any
				client := testClient(t, redisHook{pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
					for _, cmd := range cmds {
						commands = append(commands, cmd.Args())
					}
					return nil
				}})
				tx := &transactor{client: client, hardDelete: hardDelete, be: m.NewBindingEvents(), bindings: []binding{
					{resource: resource{KeyPrefix: "persistent", Format: "json", KeyEncoding: "json", DeltaUpdates: delta}},
					{resource: resource{KeyPrefix: "expiring", Format: "json", TTLSeconds: 60, DeltaUpdates: delta}},
				}}
				deletion := storeRequest(0, tuple.Tuple{"1"}, `{"id":"1","_meta":{"op":"\u0064"}}`)
				protocolDeletion := storeRequest(1, tuple.Tuple{int64(3)}, `{"id":3}`)
				protocolDeletion.Store.Delete = true
				_, err := tx.Store(storeIterator(
					storeRequest(0, tuple.Tuple{int64(1)}, " { \"id\": 1 } "),
					storeRequest(1, tuple.Tuple{int64(2)}, `{"id":2}`),
					deletion, protocolDeletion,
				))
				require.NoError(t, err)
				want := [][]any{
					{"multi"},
					{"JSON.SET", "persistent:[1]", "$", `{"id":1}`}, {"persist", "persistent:[1]"},
					{"JSON.SET", "expiring:2", "$", `{"id":2}`}, {"expire", "expiring:2", int64(60)},
				}
				if hardDelete {
					want = append(want, []any{"del", `persistent:["1"]`}, []any{"del", "expiring:3"})
				} else {
					want = append(want,
						[]any{"JSON.SET", `persistent:["1"]`, "$", string(deletion.Store.DocJson)}, []any{"persist", `persistent:["1"]`},
						[]any{"JSON.SET", "expiring:3", "$", `{"id":3}`}, []any{"expire", "expiring:3", int64(60)},
					)
				}
				want = append(want, []any{"exec"})
				require.Equal(t, want, commands)
			})
		}
	}
}

func TestJSONLoad(t *testing.T) {
	var commands [][]string
	client := redis.NewClient(&redis.Options{
		Addr: "redis.invalid:6379", Protocol: 2, DisableIdentity: true,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			conn, server := net.Pipe()
			t.Cleanup(func() { conn.Close(); server.Close() })
			go serveRESP(server, func(args []string) any {
				switch strings.ToLower(args[0]) {
				case "hello":
					return errors.New("ERR unknown command 'hello'")
				case "json.mget":
					commands = append(commands, args)
					return []any{`{"id":1,"prior":"value"}`, nil, `null`}
				default:
					return "OK"
				}
			})
			return conn, nil
		},
	})
	t.Cleanup(func() { client.Close() })
	tx := &transactor{client: client, bindings: []binding{{resource: resource{Format: "json"}}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var docs []string
	require.NoError(t, tx.load(ctx, []string{"orders:[1]", "orders:[2]", "orders:[3]"}, []int{0, 0, 0}, func(binding int, doc json.RawMessage) error {
		require.Zero(t, binding)
		docs = append(docs, string(doc))
		return nil
	}))
	require.Equal(t, []string{`{"id":1,"prior":"value"}`, "null"}, docs)
	require.Equal(t, [][]string{{"JSON.MGET", "orders:[1]", "orders:[2]", "orders:[3]", "."}}, commands)
}

func TestJSONLoadErrors(t *testing.T) {
	for _, tt := range []struct {
		name        string
		value       any
		err         error
		callbackErr error
		want        string
	}{
		{"missing module", nil, errors.New("ERR unknown command 'JSON.MGET'"), nil, "unknown command"},
		{"invalid document", "invalid", nil, nil, "does not contain a JSON document"},
		{"callback error", `{"id":1}`, nil, errors.New("callback failed"), "callback failed"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := testClient(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
				cmd.(*redis.JSONSliceCmd).SetVal([]any{tt.value})
				return tt.err
			}})
			tx := &transactor{client: client, bindings: []binding{{resource: resource{Format: "json"}}}}
			err := tx.load(context.Background(), []string{"orders:1"}, []int{0}, func(int, json.RawMessage) error { return tt.callbackErr })
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestJSONStoreProtocol(t *testing.T) {
	for _, failure := range []string{"", "JSON.SET", "EXPIRE"} {
		t.Run("failure="+failure, func(t *testing.T) {
			var commands [][]string
			client := redis.NewClient(&redis.Options{
				Addr: "redis.invalid:6379", Protocol: 2, DisableIdentity: true,
				ReadTimeout: time.Second, WriteTimeout: time.Second,
				Dialer: func(context.Context, string, string) (net.Conn, error) {
					conn, server := net.Pipe()
					t.Cleanup(func() { conn.Close(); server.Close() })
					go serveRESP(server, func(args []string) any {
						switch strings.ToLower(args[0]) {
						case "hello":
							return errors.New("ERR unknown command 'hello'")
						case "multi":
							commands = append(commands, args)
							return "OK"
						case "json.set", "expire":
							commands = append(commands, args)
							return "QUEUED"
						case "exec":
							commands = append(commands, args)
							results := []any{"OK", int64(1)}
							if failure == "JSON.SET" {
								results[0] = errors.New("WRONGTYPE invalid JSON.SET target")
							} else if failure == "EXPIRE" {
								results[1] = errors.New("NOPERM EXPIRE denied")
							}
							return results
						default:
							return "OK"
						}
					})
					return conn, nil
				},
			})
			defer client.Close()
			tx := &transactor{client: client, be: m.NewBindingEvents(), bindings: []binding{
				{resource: resource{KeyPrefix: "orders", Format: "json", TTLSeconds: 60}},
			}}
			_, err := tx.Store(storeIterator(storeRequest(0, tuple.Tuple{int64(1)}, `{"id":1}`)))
			if failure == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, failure)
			}
			require.Equal(t, [][]string{
				{"multi"}, {"JSON.SET", "orders:1", "$", `{"id":1}`}, {"expire", "orders:1", "60"}, {"exec"},
			}, commands)
		})
	}
}

func TestJSONStoreBatches(t *testing.T) {
	var commands int
	client := testClient(t, redisHook{pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
		require.Equal(t, []any{"multi"}, cmds[0].Args())
		require.Equal(t, []any{"exec"}, cmds[len(cmds)-1].Args())
		require.LessOrEqual(t, len(cmds)-2, storeBatchSize+1)
		commands += len(cmds) - 2
		return nil
	}})
	tx := &transactor{client: client, be: m.NewBindingEvents(), bindings: []binding{
		{resource: resource{KeyPrefix: "strings"}}, {resource: resource{KeyPrefix: "json", Format: "json"}},
	}}
	requests := []*pm.Request{storeRequest(0, tuple.Tuple{int64(1)}, `{"id":1}`)}
	for i := 0; i < storeBatchSize; i++ {
		requests = append(requests, storeRequest(1, tuple.Tuple{int64(i)}, fmt.Sprintf(`{"id":%d}`, i)))
	}
	_, err := tx.Store(storeIterator(requests...))
	require.NoError(t, err)
	require.Equal(t, storeBatchSize*2+1, commands)
}

func TestValidateFormatChange(t *testing.T) {
	for _, field := range []string{"format", "keyEncoding"} {
		for _, delta := range []bool{false, true} {
			for _, backfill := range []uint32{0, 1} {
				t.Run(fmt.Sprintf("%s/delta=%t/backfill=%d", field, delta, backfill), func(t *testing.T) {
					d := testDriver(t, redisHook{process: func(ctx context.Context, cmd redis.Cmder) error {
						cmd.(*redis.StatusCmd).SetVal("PONG")
						return nil
					}})
					_, err := d.Validate(context.Background(), &pm.Request_Validate{
						ConfigJson: json.RawMessage(`{"address":"redis:6379"}`),
						LastMaterialization: &pf.MaterializationSpec{Bindings: []*pf.MaterializationSpec_Binding{{
							ResourcePath: []string{"orders"}, ResourceConfigJson: json.RawMessage(`{"keyPrefix":"orders"}`),
						}}},
						Bindings: []*pm.Request_Validate_Binding{{
							ResourceConfigJson: json.RawMessage(fmt.Sprintf(`{"keyPrefix":"orders",%q:"json","delta_updates":%t}`, field, delta)),
							Backfill:           backfill,
							Collection:         pf.CollectionSpec{},
						}},
					})
					if delta || backfill == 0 {
						require.ErrorContains(t, err, "changing format or keyEncoding")
					} else {
						require.NoError(t, err)
					}
				})
			}
		}
	}
}

func TestMixedFormatsTransaction(t *testing.T) {
	raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(raw))
	spec.ConfigJson = json.RawMessage(`{"address":"redis:6379"}`)
	template := *spec.Bindings[0]
	spec.Bindings = nil
	for _, res := range []resource{
		{KeyPrefix: "strings"},
		{KeyPrefix: "json", Format: "json", KeyEncoding: "json"},
		{KeyPrefix: "other-json", Format: "json"},
		{KeyPrefix: "safe-strings", KeyEncoding: "json"},
	} {
		b := template
		b.ResourcePath = []string{res.KeyPrefix}
		b.ResourceConfigJson, err = json.Marshal(res)
		require.NoError(t, err)
		spec.Bindings = append(spec.Bindings, &b)
	}
	var reads, writes [][]any
	d := testDriver(t, redisHook{
		process: func(ctx context.Context, cmd redis.Cmder) error {
			switch cmd.Name() {
			case "ping":
				cmd.(*redis.StatusCmd).SetVal("PONG")
			case "mget":
				reads = append(reads, cmd.Args())
				cmd.(*redis.SliceCmd).SetVal([]any{`{"id":1,"prior":"string"}`})
			case "json.mget":
				reads = append(reads, cmd.Args())
				cmd.(*redis.JSONSliceCmd).SetVal([]any{`{"id":1,"prior":"json"}`, `{"id":"1","prior":"json"}`, nil})
			default:
				return fmt.Errorf("unexpected command %v", cmd.Args())
			}
			return nil
		},
		pipeline: func(ctx context.Context, cmds []redis.Cmder) error {
			for _, cmd := range cmds {
				writes = append(writes, cmd.Args())
			}
			return nil
		},
	})
	stream := &loadedStream{}
	stream.requests = append(stream.requests, &pm.Request{Acknowledge: &pm.Request_Acknowledge{}})
	for _, load := range []struct {
		binding int
		key     tuple.Tuple
	}{{0, tuple.Tuple{int64(1)}}, {1, tuple.Tuple{int64(1)}}, {1, tuple.Tuple{"1"}}, {2, tuple.Tuple{int64(1)}}, {3, tuple.Tuple{int64(1)}}} {
		stream.requests = append(stream.requests, &pm.Request{Load: &pm.Request_Load{Binding: uint32(load.binding), KeyPacked: load.key.Pack()}})
	}
	stream.requests = append(stream.requests,
		&pm.Request{Flush: &pm.Request_Flush{}},
		storeRequest(1, tuple.Tuple{int64(1)}, `{"id":1,"prior":"json","new":true}`),
		&pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}},
		&pm.Request{Acknowledge: &pm.Request_Acknowledge{}},
	)
	err = m.RunTransactions(context.Background(), d, stream, &pm.Request_Open{
		Materialization: &spec, Version: "v1", Range: &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
	}, log.WarnLevel)
	require.NoError(t, err)
	require.Equal(t, [][]any{
		{"mget", "strings:1"},
		{"JSON.MGET", "json:[1]", `json:["1"]`, "other-json:1", "."},
		{"mget", "safe-strings:[1]"},
	}, reads)
	require.Equal(t, [][]any{
		{"multi"}, {"JSON.SET", "json:[1]", "$", `{"id":1,"prior":"json","new":true}`}, {"persist", "json:[1]"}, {"exec"},
	}, writes)
	require.Equal(t, []uint32{0, 1, 1, 3}, stream.bindings)
	require.Equal(t, []string{`{"id":1,"prior":"string"}`, `{"id":1,"prior":"json"}`, `{"id":"1","prior":"json"}`, `{"id":1,"prior":"string"}`}, stream.docs)
}

type loadedStream struct {
	fakeStream
	mu       sync.Mutex
	bindings []uint32
	docs     []string
}

func (s *loadedStream) Send(response *pm.Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if response.Loaded != nil {
		s.bindings = append(s.bindings, response.Loaded.Binding)
		s.docs = append(s.docs, string(response.Loaded.DocJson))
	}
	return nil
}
