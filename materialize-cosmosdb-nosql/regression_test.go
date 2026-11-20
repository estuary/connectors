package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
)

type cosmosTransportFunc func(*http.Request) (*http.Response, error)

func (f cosmosTransportFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func itemErrorResponse(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"code":"test failure"}`)),
		Request:    req,
	}
}

// Nullable string keys with defaults must remain distinct from "null" through
// upserts, loads and hard deletes, both for /id and for compound keys.
func TestNullKeyTransactions(t *testing.T) {
	for _, delta := range []bool{false, true} {
		t.Run(fmt.Sprintf("delta=%t", delta), func(t *testing.T) {
			f, d := newCosmosFixture()
			cfg := testConfig()
			cfg.HardDelete = true
			spec := testMaterialization(t, cfg, delta)
			for _, b := range spec.Bindings {
				p := b.Collection.GetProjection(b.FieldSelection.Keys[0])
				p.Inference.Types = []string{"null", "string"}
				p.Inference.DefaultJson = json.RawMessage(`"unknown"`)
			}
			_, err := d.Validate(context.Background(), validateRequest(spec))
			require.NoError(t, err)
			applySpec(t, d, spec)

			rows := []struct {
				binding uint32
				key     tuple.Tuple
				doc     string
				itemKey string
				stored  string
			}{
				{0, tuple.Tuple{nil}, `{"id":null,"who":"null-key"}`, `["~N"]:~N`, `{"id":"~N","_flow_id":null,"who":"null-key"}`},
				{0, tuple.Tuple{"null"}, `{"id":"null","who":"string-key"}`, `["null"]:null`, `{"id":"null","_flow_id":"null","who":"string-key"}`},
				{1, tuple.Tuple{nil, int64(7)}, `{"region":null,"seq":7,"who":"null-key"}`, `[7]:~N:7`, `{"id":"~N:7","region":null,"seq":7,"who":"null-key"}`},
				{1, tuple.Tuple{"null", int64(7)}, `{"region":"null","seq":7,"who":"string-key"}`, `[7]:null:7`, `{"id":"null:7","region":"null","seq":7,"who":"string-key"}`},
			}
			for _, deleted := range []int{0, 1} {
				stream := &transactionStream{requests: []pm.Request{ack(), flush()}}
				for _, row := range rows {
					stream.requests = append(stream.requests, storeRequestFor(row.binding, row.key, row.doc))
				}
				stream.requests = append(stream.requests, startCommit(), ack())
				runTransactions(t, d, spec, stream)
				for _, container := range []string{"orders", "events"} {
					require.Len(t, f.containers[container].items, 2)
				}
				for _, row := range rows {
					container := spec.Bindings[row.binding].ResourcePath[1]
					require.JSONEq(t, row.stored, string(f.containers[container].items[row.itemKey]))
					if !delta {
						stream = &transactionStream{requests: []pm.Request{
							ack(), loadRequestFor(row.binding, row.key), flush(), startCommit(), ack(),
						}}
						runTransactions(t, d, spec, stream)
						require.Equal(t, []string{row.doc}, loadedDocs(stream.responses))
					}
				}

				stream = &transactionStream{requests: []pm.Request{ack(), flush()}}
				for _, i := range []int{deleted, deleted + 2} {
					row := rows[i]
					req := storeRequestFor(row.binding, row.key, row.doc)
					req.Store.Delete = true
					stream.requests = append(stream.requests, req)
				}
				stream.requests = append(stream.requests, startCommit(), ack())
				runTransactions(t, d, spec, stream)
				for _, i := range []int{1 - deleted, 3 - deleted} {
					row := rows[i]
					container := spec.Bindings[row.binding].ResourcePath[1]
					require.Len(t, f.containers[container].items, 1)
					require.JSONEq(t, row.stored, string(f.containers[container].items[row.itemKey]))
				}
			}
		})
	}
}

func TestItemNotFoundSubstatus(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		for _, substatus := range []string{"", "0", "1002"} {
			t.Run(method+"/"+substatus, func(t *testing.T) {
				f, d := newCosmosFixture()
				cfg := testConfig()
				cfg.HardDelete = true
				spec := testMaterialization(t, cfg, false)
				applySpec(t, d, spec)
				var attempts int
				d.clientOptions.Transport = cosmosTransportFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == method && strings.HasSuffix(req.URL.Path, "/docs/1") {
						attempts++
						resp := itemErrorResponse(req, http.StatusNotFound)
						resp.Header.Set("x-ms-substatus", substatus)
						return resp, nil
					}
					return f.Do(req)
				})
				stream := &transactionStream{requests: []pm.Request{ack()}}
				if method == http.MethodGet {
					stream.requests = append(stream.requests, loadRequestFor(0, tuple.Tuple{int64(1)}), flush())
				} else {
					deleted := storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1}`)
					deleted.Store.Delete = true
					stream.requests = append(stream.requests, flush(), deleted)
				}
				stream.requests = append(stream.requests, startCommit(), ack())
				err := m.RunTransactions(context.Background(), d, stream, transactionOpen(spec, nil), log.DebugLevel)
				if substatus == "1002" {
					require.Error(t, err)
					require.True(t, isStatus(err, http.StatusNotFound), "%v", err)
					require.Nil(t, lastStartedCommit(stream.responses))
				} else {
					require.NoError(t, err)
				}
				require.Positive(t, attempts)
			})
		}
	}
}

func TestSessionStateRestart(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(fmt.Sprintf("acknowledged=%t", acknowledged), func(t *testing.T) {
			f, d := newCosmosFixture()
			spec := testMaterialization(t, testConfig(), false)
			spec.Bindings[0].StateKey = "orders.v1"
			spec.Bindings[1].StateKey = "events.v1"
			applySpec(t, d, spec)
			d.clientOptions.Transport = cosmosTransportFunc(func(req *http.Request) (*http.Response, error) {
				resp, err := f.Do(req)
				if err == nil && req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/orders/docs") && f.containers["orders"].writes == 2 {
					resp.Header.Set("x-ms-session-token", "1:2")
				}
				return resp, err
			})
			stream := &transactionStream{requests: []pm.Request{
				ack(), flush(),
				storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"note":"keep"}`),
				storeRequestFor(0, tuple.Tuple{int64(2)}, `{"id":2,"note":"second partition"}`),
				storeRequestFor(1, tuple.Tuple{"eu", int64(7)}, `{"region":"eu","seq":7,"note":"other binding"}`),
				startCommit(),
			}}
			if acknowledged {
				stream.requests = append(stream.requests, ack())
			}
			err := m.RunTransactions(context.Background(), d, stream, transactionOpen(spec, nil), log.DebugLevel)
			if acknowledged {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, io.EOF)
			}
			started := lastStartedCommit(stream.responses)
			require.NotNil(t, started)
			state := started.State
			require.False(t, state.MergePatch)
			require.JSONEq(t, `{"bindingStateV1":{
				"orders.v1":{"containerRid":"orders-1","sessionTokens":{"0":"0:1","1":"1:2"}},
				"events.v1":{"containerRid":"events-1","sessionTokens":{"0":"0:1"}}}}`, string(state.UpdatedJson))

			// A replica returns a stale document unless the restarted client supplies both tokens.
			d.clientOptions.Transport = cosmosTransportFunc(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/docs/") {
					want := []string{"0:1", "1:2"}
					if strings.Contains(req.URL.Path, "/events/") {
						want = []string{"0:1"}
					}
					got := strings.Split(req.Header.Get("x-ms-session-token"), ",")
					slices.Sort(got)
					if !slices.Equal(want, got) {
						resp := itemErrorResponse(req, http.StatusOK)
						resp.Body = io.NopCloser(strings.NewReader(`{"id":"1","_flow_id":1}`))
						return resp, nil
					}
				}
				return f.Do(req)
			})
			slices.Reverse(spec.Bindings) // State follows StateKey, not binding position.
			stream = &transactionStream{requests: []pm.Request{
				ack(),
				loadRequestFor(1, tuple.Tuple{int64(1)}), loadRequestFor(1, tuple.Tuple{int64(2)}),
				loadRequestFor(0, tuple.Tuple{"eu", int64(7)}), flush(),
				storeRequestFor(1, tuple.Tuple{int64(1)}, `{"id":1,"status":"paid"}`),
				startCommit(), ack(),
			}}
			stream.beforeStore = mergeLoaded(1)
			require.NoError(t, m.RunTransactions(context.Background(), d, stream, transactionOpen(spec, state.UpdatedJson), log.DebugLevel))
			require.JSONEq(t, `{"id":"1","_flow_id":1,"note":"keep","status":"paid"}`, string(f.containers["orders"].items[`["1"]:1`]))
			loaded := loadedDocs(stream.responses)
			require.Len(t, loaded, 3)
			for _, doc := range loaded {
				require.Contains(t, doc, `"note":`)
			}
		})
	}
}

func TestSessionStateRecreatedContainer(t *testing.T) {
	ctx := context.Background()
	f, d := newCosmosFixture()
	spec := testMaterialization(t, testConfig(), false)
	applySpec(t, d, spec)
	stream := &transactionStream{requests: []pm.Request{
		ack(), flush(),
		storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"note":"old container"}`),
		storeRequestFor(0, tuple.Tuple{int64(2)}, `{"id":2,"note":"old container"}`),
		startCommit(), ack(),
	}}
	runTransactions(t, d, spec, stream)
	state := lastStartedCommit(stream.responses).State.UpdatedJson
	require.JSONEq(t, `{"bindingStateV1":{
		"orders":{"containerRid":"orders-1","sessionTokens":{"0":"0:2"}},
		"events":{"containerRid":"events-1","sessionTokens":{}}}}`, string(state))

	// The container is deleted in Azure and recreated by a republish without a backfill.
	delete(f.containers, "orders")
	_, err := d.Apply(ctx, &pm.Request_Apply{Materialization: spec, LastMaterialization: spec, Version: "v2"})
	require.NoError(t, err)
	require.Equal(t, "orders-2", f.containers["orders"].properties.ResourceID)

	// Cosmos rejects a session token ahead of the container's writes as session-unavailable.
	var tokens []string
	d.clientOptions.Transport = cosmosTransportFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/orders/docs/") {
			token := req.Header.Get("x-ms-session-token")
			tokens = append(tokens, token)
			f.mu.Lock()
			writes := f.containers["orders"].writes
			f.mu.Unlock()
			if _, lsn, ok := strings.Cut(token, ":"); ok {
				if n, _ := strconv.Atoi(lsn); n > writes {
					resp := itemErrorResponse(req, http.StatusNotFound)
					resp.Header.Set("x-ms-substatus", "1002")
					return resp, nil
				}
			}
		}
		return f.Do(req)
	})
	stream = &transactionStream{requests: []pm.Request{
		ack(), loadRequestFor(0, tuple.Tuple{int64(1)}), flush(),
		storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"note":"new container"}`),
		startCommit(), ack(), loadRequestFor(0, tuple.Tuple{int64(1)}), flush(),
		startCommit(), ack(),
	}}
	require.NoError(t, m.RunTransactions(ctx, d, stream, transactionOpen(spec, state), log.DebugLevel))
	require.Equal(t, []string{"", "0:1"}, tokens, "tokens of the deleted container must not be sent")
	require.Equal(t, []string{`{"id":1,"note":"new container"}`}, loadedDocs(stream.responses))
	state = lastStartedCommit(stream.responses).State.UpdatedJson
	require.Contains(t, string(state), `"orders":{"containerRid":"orders-2","sessionTokens":{"0":"0:1"}}`)
}

func TestAccountConsistency(t *testing.T) {
	for _, tt := range []struct {
		level azcosmos.ConsistencyLevel
		ok    bool
	}{
		{azcosmos.ConsistencyLevelStrong, true},
		{azcosmos.ConsistencyLevelBoundedStaleness, true},
		{azcosmos.ConsistencyLevelSession, true},
		{azcosmos.ConsistencyLevelConsistentPrefix, false},
		{azcosmos.ConsistencyLevelEventual, false},
	} {
		for _, delta := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/delta=%t", tt.level, delta), func(t *testing.T) {
				ctx := context.Background()
				f, d := newCosmosFixture()
				cfg := testConfig()
				key, err := base64.StdEncoding.DecodeString(cfg.Key)
				require.NoError(t, err)
				var accountReads int
				d.clientOptions.Transport = cosmosTransportFunc(func(req *http.Request) (*http.Response, error) {
					// The SDK's own account reads check that the connector signs them the same way.
					if strings.Trim(req.URL.Path, "/") == "" {
						if got, want := req.Header.Get("Authorization"), accountKeyPolicy(key).sign(req.Header.Get("x-ms-date")); got != want {
							return nil, fmt.Errorf("account read authorization %q, expected %q", got, want)
						}
						accountReads++
					}
					return f.Do(req)
				})
				f.consistency = tt.level
				spec := testMaterialization(t, cfg, delta)
				_, validateErr := d.Validate(ctx, validateRequest(spec))
				applySpec(t, d, spec)
				stream := &transactionStream{requests: []pm.Request{
					ack(), flush(),
					storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1}`),
					startCommit(), ack(),
				}}
				openErr := m.RunTransactions(ctx, d, stream, transactionOpen(spec, nil), log.DebugLevel)
				if tt.ok || delta {
					require.NoError(t, validateErr)
					require.NoError(t, openErr)
				} else {
					want := fmt.Sprintf("the account's default consistency level is %s", tt.level)
					require.ErrorContains(t, validateErr, want)
					require.ErrorContains(t, openErr, want)
				}
				require.Positive(t, accountReads)
			})
		}
	}
	for _, tt := range []struct {
		name, endpoint string
		ok             bool
	}{
		{"emulator", "http://localhost:8081/", true},
		{"azure account named like the emulator", "https://cosmosdev.documents.azure.com:443/", false},
		{"uppercase azure host named like the emulator", "https://COSMOSDEV.DOCUMENTS.AZURE.COM:443/", false},
		{"mixed-case azure host named like the emulator", "https://cosmosdev.Documents.azure.com:443/", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, d := newCosmosFixture()
			f.accountID = "cosmosdev"
			f.consistency = azcosmos.ConsistencyLevelEventual
			cfg := testConfig()
			cfg.Endpoint = tt.endpoint
			_, err := d.Validate(context.Background(), validateRequest(testMaterialization(t, cfg, false)))
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "the account's default consistency level is Eventual")
			}
		})
	}
	t.Run("read error", func(t *testing.T) {
		f, d := newCosmosFixture()
		cfg := testConfig()
		f.failures["GET "] = http.StatusUnauthorized
		_, err := d.Validate(context.Background(), validateRequest(testMaterialization(t, cfg, false)))
		require.ErrorContains(t, err, "reading account consistency level")
		require.NotContains(t, err.Error(), cfg.Key)
	})
}

func partitionMaterialization(t *testing.T, ptr string, explicit bool) *pf.MaterializationSpec {
	t.Helper()
	cfg := testConfig()
	cfg.Advanced.FeatureFlags = "allow_existing_tables_for_new_bindings"
	spec := testMaterialization(t, cfg, false)
	spec.Bindings = spec.Bindings[:1]
	b := spec.Bindings[0]
	b.Collection.Key = []string{ptr}
	b.Collection.Projections = []pf.Projection{
		{Field: "flow_document", Ptr: "", Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MUST}},
		{Field: "key", Ptr: ptr, IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}},
	}
	b.FieldSelection.Keys = []string{"key"}
	res := resource{Container: "orders"}
	if explicit {
		res.PartitionKey = ptr
	}
	var err error
	b.ResourceConfigJson, err = json.Marshal(res)
	require.NoError(t, err)
	return spec
}

func TestPartitionKeyChangeWithBackfill(t *testing.T) {
	ctx := context.Background()
	f, d := newCosmosFixture()
	cfg := testConfig()
	cfg.Endpoint = "http://account.cosmos.test:8081/"
	spec := testMaterialization(t, cfg, false)
	applySpec(t, d, spec)
	require.Equal(t, []string{"/seq"}, f.containers["events"].properties.PartitionKeyDefinition.Paths)

	next := testMaterialization(t, cfg, false)
	events := next.Bindings[1]
	require.Equal(t, "events", events.ResourcePath[1])
	resourceJSON, err := json.Marshal(resource{Container: "events", PartitionKey: "/region"})
	require.NoError(t, err)
	events.ResourceConfigJson = resourceJSON

	// Without a backfill the existing container is kept, so its partition key
	// must still match.
	req := validateRequest(next)
	req.LastMaterialization = spec
	_, err = d.Validate(ctx, req)
	require.ErrorContains(t, err, `expected a single partition key at "/region"`)

	// A backfill recreates the container with the new partition key.
	events.Backfill++
	req = validateRequest(next)
	req.LastMaterialization = spec
	_, err = d.Validate(ctx, req)
	require.NoError(t, err)
	_, err = d.Apply(ctx, &pm.Request_Apply{Materialization: next, LastMaterialization: spec, Version: "v2"})
	require.NoError(t, err)
	require.Equal(t, []string{"/region"}, f.containers["events"].properties.PartitionKeyDefinition.Paths)
}

func TestPartitionKeyValidation(t *testing.T) {
	for _, tt := range []struct {
		ptr      string
		optional bool
		want     string
	}{
		{"/id/number", false, "replaces id with an encoded string"},
		{"/id/tenant", false, "replaces id with an encoded string"},
		{"/region", true, "must be present in every document"},
		{"/location/region", true, "must be present in every document"},
		{"/id", true, ""},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%t", tt.ptr, explicit), func(t *testing.T) {
				f, d := newCosmosFixture()
				spec := partitionMaterialization(t, tt.ptr, explicit)
				if tt.optional {
					projection := &spec.Bindings[0].Collection.Projections[1]
					projection.Inference.Exists = pf.Inference_MAY
					projection.Inference.DefaultJson = json.RawMessage(`"unknown"`)
				}
				_, err := d.Validate(context.Background(), validateRequest(spec))
				if tt.want == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tt.want)
					require.Empty(t, f.requests)
				}
			})
		}
	}
	t.Run("optional non-partition key", func(t *testing.T) {
		_, d := newCosmosFixture()
		spec := testMaterialization(t, testConfig(), false)
		p := spec.Bindings[1].Collection.GetProjection("region")
		p.Inference.Exists = pf.Inference_MAY
		p.Inference.DefaultJson = json.RawMessage(`"unknown"`)
		_, err := d.Validate(context.Background(), validateRequest(spec))
		require.NoError(t, err) // The partition key is the required /seq field.
	})
}

func TestPartitionPointerPaths(t *testing.T) {
	for _, tt := range []struct{ ptr, path, doc string }{
		{"/region", "/region", `{"region":"eu"}`},
		{"/a~1b", `/"a/b"`, `{"a/b":"eu"}`},
		{"/a~0b", `/"a~b"`, `{"a~b":"eu"}`},
		{"/outer/a~1b", `/outer/"a/b"`, `{"outer":{"a/b":"eu"}}`},
	} {
		t.Run(tt.ptr, func(t *testing.T) {
			f, d := newCosmosFixture()
			spec := partitionMaterialization(t, tt.ptr, false)
			_, err := d.Validate(context.Background(), validateRequest(spec))
			require.NoError(t, err)
			applySpec(t, d, spec)
			require.Equal(t, []string{tt.path}, f.containers["orders"].properties.PartitionKeyDefinition.Paths)
			_, err = d.Validate(context.Background(), validateRequest(spec))
			require.NoError(t, err)
			applySpec(t, d, spec)
			stream := &transactionStream{requests: []pm.Request{
				ack(), flush(),
				storeRequestFor(0, tuple.Tuple{"eu"}, tt.doc),
				startCommit(), ack(), loadRequestFor(0, tuple.Tuple{"eu"}),
				flush(), startCommit(),
				ack(),
			}}
			runTransactions(t, d, spec, stream)
			require.Equal(t, []string{tt.doc}, loadedDocs(stream.responses))
			f.containers["orders"].properties.PartitionKeyDefinition.Paths = []string{"/wrong"}
			_, err = d.Validate(context.Background(), validateRequest(spec))
			require.ErrorContains(t, err, "expected a single partition key")
		})
	}
}

func TestThrottleRetries(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		for _, mode := range []string{"recover", "exhaust", "cancel"} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				f, d := newCosmosFixture()
				cfg := testConfig()
				cfg.HardDelete = true
				spec := testMaterialization(t, cfg, false)
				applySpec(t, d, spec)
				f.containers["orders"].items[`["1"]:1`] = json.RawMessage(`{"id":"1","_flow_id":1,"note":"keep"}`)
				d.clientOptions.Retry = policy.RetryOptions{}
				var attempts int
				var first time.Time
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				throttled := make(chan struct{})
				var once sync.Once
				d.clientOptions.Transport = cosmosTransportFunc(func(req *http.Request) (*http.Response, error) {
					if req.Method == method && strings.Contains(req.URL.Path, "/docs") {
						attempts++
						if attempts == 1 {
							first = time.Now()
						}
						if mode != "recover" || attempts <= 4 {
							resp := itemErrorResponse(req, http.StatusTooManyRequests)
							resp.Header.Set("x-ms-retry-after-ms", "5")
							if mode == "cancel" {
								resp.Header.Set("x-ms-retry-after-ms", "1000")
								once.Do(func() { close(throttled) })
							}
							return resp, nil
						}
					}
					return f.Do(req)
				})
				if mode == "cancel" {
					go func() {
						select {
						case <-throttled:
							time.Sleep(20 * time.Millisecond)
							cancel()
						case <-ctx.Done():
						}
					}()
				}
				stream := &transactionStream{requests: []pm.Request{ack()}}
				if method == http.MethodGet {
					stream.requests = append(stream.requests, loadRequestFor(0, tuple.Tuple{int64(1)}), flush())
				} else {
					store := storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1,"number":9007199254740993}`)
					store.Store.Delete = method == http.MethodDelete
					stream.requests = append(stream.requests, flush(), store)
				}
				stream.requests = append(stream.requests, startCommit(), ack())
				err := m.RunTransactions(ctx, d, stream, transactionOpen(spec, nil), log.DebugLevel)
				switch mode {
				case "recover":
					require.NoError(t, err)
					require.Equal(t, 5, attempts)
					require.GreaterOrEqual(t, time.Since(first), 20*time.Millisecond)
					if method == http.MethodPost {
						require.True(t, bytes.Contains(f.containers["orders"].items[`["1"]:1`], []byte(`9007199254740993`)))
					} else if method == http.MethodDelete {
						require.Empty(t, f.containers["orders"].items)
					}
				case "exhaust":
					require.True(t, isStatus(err, http.StatusTooManyRequests), "%v", err)
					require.Equal(t, 20, attempts)
				case "cancel":
					require.ErrorIs(t, err, context.Canceled)
					require.Equal(t, 1, attempts)
					require.Less(t, time.Since(first), time.Second)
				}
				if mode != "recover" {
					require.Nil(t, lastStartedCommit(stream.responses))
				}
			})
		}
	}
}

// Keys with characters the gateway decodes or rejects in ids, and non-ASCII
// partition values, must round-trip through Store, Load and a hard delete.
func TestEscapedKeyRoundTrip(t *testing.T) {
	f, d := newCosmosFixture()
	cfg := testConfig()
	cfg.HardDelete = true
	spec := testMaterialization(t, cfg, false)
	spec.Bindings[1].Collection.Key = []string{"/seq", "/region"}
	applySpec(t, d, spec)
	keys := []tuple.Tuple{{int64(8), "eu/west"}, {int64(9), "2026-11-16T10:00:00Z"}, {int64(10), "100% ok "}}
	var stream transactionStream
	stream.requests = append(stream.requests, ack(), flush())
	for _, key := range keys {
		stream.requests = append(stream.requests, storeRequestFor(1, key, fmt.Sprintf(`{"seq":%d,"region":%q}`, key[0], key[1])))
	}
	stream.requests = append(stream.requests, storeRequestFor(0, tuple.Tuple{"München"}, `{"id":"München"}`))
	stream.requests = append(stream.requests, startCommit(), ack())
	for _, key := range keys {
		stream.requests = append(stream.requests, loadRequestFor(1, key))
	}
	stream.requests = append(stream.requests, loadRequestFor(0, tuple.Tuple{"München"}), flush())
	for _, key := range keys {
		deleted := storeRequestFor(1, key, `null`)
		deleted.Store.Delete, deleted.Store.Exists = true, true
		stream.requests = append(stream.requests, deleted)
	}
	stream.requests = append(stream.requests, startCommit(), ack())
	runTransactions(t, d, spec, &stream)

	require.Len(t, loadedDocs(stream.responses), len(keys)+1)
	require.Empty(t, f.containers["events"].items)
	require.Len(t, f.containers["orders"].items, 1)
}

// A hard delete of a missing item is counted by the store iterator, so it
// must be reported to keep the transaction health check balanced.
func TestHardDeleteNotFoundRowStats(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()
	_, d := newCosmosFixture()
	cfg := testConfig()
	cfg.HardDelete = true
	spec := testMaterialization(t, cfg, true)
	applySpec(t, d, spec)
	deleted := storeRequestFor(0, tuple.Tuple{int64(1)}, `null`)
	deleted.Store.Delete = true
	runTransactions(t, d, spec, &transactionStream{requests: []pm.Request{
		ack(), flush(),
		storeRequestFor(0, tuple.Tuple{int64(2)}, `{"id":2}`), deleted,
		startCommit(), ack(),
	}})
	var verdicts []any
	for _, e := range hook.AllEntries() {
		if e.Message == "transaction health" {
			verdicts = append(verdicts, e.Data["verdict"])
		}
	}
	require.Equal(t, []any{"ok"}, verdicts)
}

// Apply drains pending state with the last spec's bindings. A container that
// the new spec no longer names must not fail building that transactor.
func TestApplyDrainWithRemovedBinding(t *testing.T) {
	ctx := context.Background()
	f, d := newCosmosFixture()
	last := testMaterialization(t, testConfig(), false)
	applySpec(t, d, last)
	next := testMaterialization(t, testConfig(), true)
	next.Bindings = next.Bindings[1:]
	state := json.RawMessage(`{"bindingStateV1":{"orders":{"containerRid":"orders-1","sessionTokens":{"0":"0:1"}}}}`)
	_, err := d.Apply(ctx, &pm.Request_Apply{Materialization: next, LastMaterialization: last, Version: "v2", LastVersion: "v1", StateJson: state})
	require.NoError(t, err)

	// A binding without its container still refuses real work.
	delete(f.containers, "orders")
	err = m.RunTransactions(ctx, d, &transactionStream{requests: []pm.Request{
		ack(), flush(),
		storeRequestFor(0, tuple.Tuple{int64(1)}, `{"id":1}`),
		startCommit(),
	}}, transactionOpen(last, nil), log.DebugLevel)
	require.ErrorContains(t, err, `container "orders" does not exist`)
}

// Switching every binding to delta updates is the remedy for a weak account
// consistency, so draining the last spec's standard-updates state must not
// fail the consistency check.
func TestApplyDrainToDeltaUpdatesWithWeakConsistency(t *testing.T) {
	ctx := context.Background()
	f, d := newCosmosFixture()
	last := testMaterialization(t, testConfig(), false)
	applySpec(t, d, last)

	f.consistency = azcosmos.ConsistencyLevelEventual
	next := testMaterialization(t, testConfig(), true)
	validate := validateRequest(next)
	validate.LastMaterialization = last
	_, err := d.Validate(ctx, validate)
	require.NoError(t, err)
	state := json.RawMessage(`{"bindingStateV1":{"orders":{"containerRid":"orders-1","sessionTokens":{"0":"0:1"}}}}`)
	_, err = d.Apply(ctx, &pm.Request_Apply{Materialization: next, LastMaterialization: last, Version: "v2", LastVersion: "v1", StateJson: state})
	require.NoError(t, err)
}

// Re-running an Apply that drains pending state must not fail on a binding
// whose container an earlier attempt already recreated with a new partition key.
func TestApplyDrainRetryAfterPartitionKeyBackfill(t *testing.T) {
	ctx := context.Background()
	f, d := newCosmosFixture()
	last := testMaterialization(t, testConfig(), false)
	applySpec(t, d, last)

	next := testMaterialization(t, testConfig(), false)
	ordersRes, err := json.Marshal(resource{Container: "orders", DeltaUpdates: true})
	require.NoError(t, err)
	next.Bindings[0].ResourceConfigJson = ordersRes
	next.Bindings[0].DeltaUpdates = true
	eventsRes, err := json.Marshal(resource{Container: "events", PartitionKey: "/region"})
	require.NoError(t, err)
	next.Bindings[1].ResourceConfigJson = eventsRes
	next.Bindings[1].Backfill++
	next.Bindings[1].StateKey = "events.v1"

	state := json.RawMessage(`{"bindingStateV1":{"orders":{"containerRid":"orders-1","sessionTokens":{"0":"0:1"}}}}`)
	req := &pm.Request_Apply{Materialization: next, LastMaterialization: last, Version: "v2", LastVersion: "v1", StateJson: state}
	_, err = d.Apply(ctx, req)
	require.NoError(t, err)
	require.Equal(t, []string{"/region"}, f.containers["events"].properties.PartitionKeyDefinition.Paths)
	_, err = d.Apply(ctx, req)
	require.NoError(t, err)
}
