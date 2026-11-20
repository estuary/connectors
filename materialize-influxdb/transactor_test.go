package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

func TestStoreFirstLoad(t *testing.T) {
	var rec recorder
	d := rec.driver(t)

	// The publication time is supplied by Flow's extractor, not a field of the document.
	stream := &storeStream{requests: []pm.Request{
		{Acknowledge: &pm.Request_Acknowledge{}},
		{Flush: &pm.Request_Flush{}},
		storeRequest(0, tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{int64(1), []byte(`{"fw":"1.2"}`), `say "hi", ok`, true, 20.5}),
		storeRequest(0, tuple.Tuple{"s1", "2026-01-01T00:01:00Z"}, tuple.Tuple{nil, nil, nil, nil, int64(21)}),
		storeRequest(0, tuple.Tuple{"lab 1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, 19.5}),
		storeRequest(1, tuple.Tuple{int64(1)}, tuple.Tuple{10.5, "2026-01-01T12:00:00Z", "new"}),
		storeRequest(1, tuple.Tuple{int64(2)}, tuple.Tuple{int64(20), "2026-01-01T12:00:00Z", "paid"}),
		{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}},
		{Acknowledge: &pm.Request_Acknowledge{}},
		{Flush: &pm.Request_Flush{}},
		storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{7.25, "2026-01-01T12:01:00Z", "new"}),
		{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}},
		{Acknowledge: &pm.Request_Acknowledge{}},
	}}
	hook := logtest.NewGlobal()
	defer hook.Reset()
	require.NoError(t, m.RunTransactions(context.Background(), d, stream, testOpen(), log.InfoLevel))
	require.Equal(t, []string{
		`readings,sensor=s1 count=1i,meta="{\"fw\":\"1.2\"}",note="say \"hi\", ok",ok=true,temp=20.5 1767225600000000000` + "\n" +
			"readings,sensor=s1 temp=21 1767225660000000000\n" +
			"readings,sensor=lab\\ 1 temp=19.5 1767225600000000000\n" +
			"orders,id=1 amount=10.5,status=\"new\" 1767268800000000000\n" +
			"orders,id=2 amount=20,status=\"paid\" 1767268800000000000\n",
		"orders,id=3 amount=7.25,status=\"new\" 1767268860000000000\n",
	}, rec.writes)
	require.Empty(t, rec.deletes)
	requireCommitted(t, stream, hook, 2, 6)
}

func TestStoreBatches(t *testing.T) {
	var sizes []int
	tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
		requireWriteRequest(t, req)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		sizes = append(sizes, strings.Count(string(body), "\n"))
		return noContent(), nil
	})
	var requests []pm.Request
	for i := 0; i < 2*maxBatchLines+1; i++ {
		requests = append(requests, storeRequest(1, tuple.Tuple{int64(i)}, tuple.Tuple{int64(20), "2026-01-01T00:00:00Z", "paid"}))
	}
	commit, err := tx.Store(storeIterator(context.Background(), requests...))
	require.NoError(t, err)
	require.Nil(t, commit)
	require.Equal(t, []int{5000, 5000, 1}, sizes)
}

func TestStoreNumericTags(t *testing.T) {
	var rec recorder
	tx, _, _, err := rec.driver(t).NewTransactor(context.Background(), *testOpen(), nil)
	require.NoError(t, err)
	defer tx.Destroy()

	// Equivalent integer and float keys must share a series and delete predicate.
	floatDelete := storeRequest(1, tuple.Tuple{float64(1000000)}, tuple.Tuple{nil, nil, nil})
	floatDelete.Store.Delete = true
	intDelete := storeRequest(1, tuple.Tuple{int64(1000000)}, tuple.Tuple{nil, nil, nil})
	intDelete.Store.Delete = true
	_, err = tx.Store(storeIterator(context.Background(),
		storeRequest(1, tuple.Tuple{float64(1000000)}, tuple.Tuple{7.25, "2026-01-01T00:00:00Z", "new"}),
		storeRequest(1, tuple.Tuple{int64(1000000)}, tuple.Tuple{7.25, "2026-01-01T00:00:01Z", "new"}),
		intDelete, floatDelete,
	))
	require.NoError(t, err)
	require.Equal(t, []string{
		"orders,id=1000000 amount=7.25,status=\"new\" 1767225600000000000\n" +
			"orders,id=1000000 amount=7.25,status=\"new\" 1767225601000000000\n",
	}, rec.writes)
	require.Equal(t, []deleteRequest{
		{Start: "1677-09-21T00:12:43.145224194Z", Stop: "2262-04-11T23:47:16.854775806Z", Predicate: `_measurement="orders" AND "id"="1000000"`},
		{Start: "1677-09-21T00:12:43.145224194Z", Stop: "2262-04-11T23:47:16.854775806Z", Predicate: `_measurement="orders" AND "id"="1000000"`},
	}, rec.deletes)
}

func TestStoreBatchBytes(t *testing.T) {
	var sizes []int
	var lines int
	tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
		requireWriteRequest(t, req)
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		sizes = append(sizes, len(body))
		lines += strings.Count(string(body), "\n")
		return noContent(), nil
	})
	status := strings.Repeat("x", 10<<10)
	var requests []pm.Request
	for i := 0; i < 2000; i++ {
		requests = append(requests, storeRequest(1, tuple.Tuple{int64(i)}, tuple.Tuple{int64(20), "2026-01-01T00:00:00Z", status}))
	}
	_, err := tx.Store(storeIterator(context.Background(), requests...))
	require.NoError(t, err)
	require.Equal(t, 2000, lines)
	require.Greater(t, len(sizes), 1)
	for _, size := range sizes {
		require.LessOrEqual(t, size, maxBatchBytes)
	}
}

func TestStoreWaitsForRequest(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete=%t", deletion), func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
				if deletion {
					requireDeleteRequest(t, req)
				}
				close(entered)
				<-release
				return noContent(), nil
			})
			request := storeRequest(1, tuple.Tuple{int64(1)}, tuple.Tuple{10.5, "2026-01-01T00:00:00Z", "new"})
			request.Store.Delete = deletion
			done := make(chan error, 1)
			go func() {
				_, err := tx.Store(storeIterator(context.Background(), request))
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("Store did not send the request")
			}
			select {
			case err := <-done:
				t.Fatalf("Store returned before the request was accepted: %v", err)
			default:
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("Store did not finish after the request was accepted")
			}
		})
	}
}

func TestStoreErrors(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("batch %d", failAt), func(t *testing.T) {
			var calls int
			tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
				calls++
				if calls == failAt {
					return &http.Response{StatusCode: http.StatusUnprocessableEntity, Body: io.NopCloser(strings.NewReader(`{"message":"field type conflict"}`))}, nil
				}
				return noContent(), nil
			})
			var requests []pm.Request
			for i := 0; i < maxBatchLines+1; i++ {
				requests = append(requests, storeRequest(1, tuple.Tuple{int64(i)}, tuple.Tuple{10.5, "2026-01-01T00:00:00Z", "new"}))
			}
			commit, err := tx.Store(storeIterator(context.Background(), requests...))
			require.ErrorContains(t, err, "HTTP 422: field type conflict")
			require.Nil(t, commit)
			require.Equal(t, failAt, calls)
		})
	}

	var calls int
	tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
		calls++
		return noContent(), nil
	})
	request := storeRequest(1, tuple.Tuple{int64(1)}, tuple.Tuple{10.5, "invalid timestamp", "new"})
	_, err := tx.Store(storeIterator(context.Background(), request))
	require.ErrorContains(t, err, `parsing timestamp field "flow_published_at"`)
	require.Zero(t, calls)

	request = storeRequest(1, tuple.Tuple{int64(1)}, tuple.Tuple{10.5, "2026-01-01T00:00:00Z", "new"})
	streamErr := fmt.Errorf("test stream failure")
	stream := &storeStream{requests: []pm.Request{request}, err: streamErr}
	_, err = tx.Store(m.NewStoreIterator(context.Background(), stream, &pm.Request{Flush: &pm.Request_Flush{}}))
	require.ErrorIs(t, err, streamErr)
	require.Zero(t, calls)

	request.Store.ValuesPacked = []byte{0xff}
	_, err = tx.Store(storeIterator(context.Background(), request))
	require.ErrorContains(t, err, "unpacking Store values")
	require.Zero(t, calls)
}

func TestStoreSkipsEmptyPoints(t *testing.T) {
	var batches []string
	tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		batches = append(batches, string(body))
		return noContent(), nil
	})
	hook := logtest.NewGlobal()
	defer hook.Reset()
	empty := storeRequest(0, tuple.Tuple{"empty", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil})
	valid := storeRequest(0, tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, int64(21)})
	_, err := tx.Store(storeIterator(context.Background(), empty, valid))
	require.NoError(t, err)
	require.Equal(t, []string{"readings,sensor=s1 temp=21 1767225600000000000\n"}, batches)
	require.Equal(t, log.WarnLevel, hook.LastEntry().Level)
	require.Contains(t, hook.LastEntry().Message, "no non-null fields")

	_, err = tx.Store(storeIterator(context.Background(), empty))
	require.NoError(t, err)
	_, err = tx.Store(storeIterator(context.Background()))
	require.NoError(t, err)
	require.Len(t, batches, 1)
}

func TestStoreBeyondRetention(t *testing.T) {
	messages := []string{
		"failure writing points to database: partial write: points beyond retention policy dropped=2",
		"partial write: points beyond retention policy dropped=3",
	}
	var calls int
	tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
		body := fmt.Sprintf(`{"code":"unprocessable entity","message":%q}`, messages[calls])
		calls++
		return &http.Response{StatusCode: http.StatusUnprocessableEntity, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	hook := logtest.NewGlobal()
	defer hook.Reset()
	// Each round's count is logged, since the status keeps only the first warning.
	for _, want := range []string{
		`dropped 2 points older than the retention period of bucket "sensors & lab"`,
		"dropped 3 points",
	} {
		request := storeRequest(0, tuple.Tuple{"s1", "2020-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, int64(21)})
		_, err := tx.Store(storeIterator(context.Background(), request))
		require.NoError(t, err)
		require.Equal(t, log.WarnLevel, hook.LastEntry().Level)
		require.Contains(t, hook.LastEntry().Message, want)
	}
	require.Equal(t, 2, calls)
	require.Equal(t, 5, tx.droppedPoints)
	require.Equal(t, 5, hook.LastEntry().Data["total"])
	require.Contains(t, hook.LastEntry().Message, "(5 since the connector started)")
}

func TestStoreUpdatesAndDeletes(t *testing.T) {
	var rec recorder
	d := rec.driver(t)
	open := testOpen()
	for _, binding := range open.Materialization.Bindings {
		binding.Collection.Projections = append([]pf.Projection{{
			Field: "_meta/op", Ptr: "/_meta/op", Inference: pf.Inference{Types: []string{"string"}},
		}}, binding.Collection.Projections...)
		binding.FieldSelection.Values = append([]string{"_meta/op"}, binding.FieldSelection.Values...)
	}
	// Fresh runtime state has neither loaded records nor the root document.
	// Deletions arrive through the selected operation without a protocol flag.
	requests := []pm.Request{
		storeRequest(0, tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, int64(2), []byte(`{"fw":"1.3"}`), "updated", false, 22.5}),
		storeRequest(0, tuple.Tuple{"lab 1", "2026-01-01T00:00:00Z"}, tuple.Tuple{"d", nil, nil, nil, nil, nil}),
		storeRequest(0, tuple.Tuple{"s2", "2026-01-01T00:02:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil, int64(18)}),
		storeRequest(1, tuple.Tuple{int64(2)}, tuple.Tuple{nil, int64(20), "2026-01-01T12:02:00Z", "shipped"}),
		storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{"d", nil, "2026-01-01T12:02:00Z", nil}),
		storeRequest(1, tuple.Tuple{int64(4)}, tuple.Tuple{nil, int64(5), "2026-01-01T12:02:00Z", "new"}),
	}
	stream := &storeStream{requests: []pm.Request{{Acknowledge: &pm.Request_Acknowledge{}}, {Flush: &pm.Request_Flush{}}}}
	for _, request := range requests {
		request.Store.DocJson = nil
		stream.requests = append(stream.requests, request)
	}
	stream.requests = append(stream.requests,
		pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}},
		pm.Request{Acknowledge: &pm.Request_Acknowledge{}},
	)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	require.NoError(t, m.RunTransactions(context.Background(), d, stream, open, log.InfoLevel))
	require.Equal(t, []string{
		"/proxy/api/v2/write", "/proxy/api/v2/delete", "/proxy/api/v2/write", "/proxy/api/v2/delete", "/proxy/api/v2/write",
	}, rec.paths)
	require.Equal(t, []string{
		`readings,sensor=s1 count=2i,meta="{\"fw\":\"1.3\"}",note="updated",ok=false,temp=22.5 1767225600000000000` + "\n",
		"readings,sensor=s2 temp=18 1767225720000000000\n" +
			"orders,id=2 amount=20,status=\"shipped\" 1767268920000000000\n",
		"orders,id=4 amount=5,status=\"new\" 1767268920000000000\n",
	}, rec.writes)
	require.Equal(t, []deleteRequest{
		{Start: "2026-01-01T00:00:00Z", Stop: "2026-01-01T00:00:00Z", Predicate: `_measurement="readings" AND "sensor"="lab 1"`},
		{Start: "1677-09-21T00:12:43.145224194Z", Stop: "2262-04-11T23:47:16.854775806Z", Predicate: `_measurement="orders" AND "id"="3"`},
	}, rec.deletes)
	requireCommitted(t, stream, hook, 1, 6)
}

func TestStoreDeletePreservesAdjacentPoints(t *testing.T) {
	for _, stamp := range []string{
		"2026-01-01T00:00:00Z",
		"2026-01-01T01:00:00.123456789+01:00",
		"2026-01-01T00:00:00.999999999Z",
		"1677-09-21T00:12:43.145224194Z",
		"2262-04-11T23:47:16.854775806Z",
	} {
		t.Run(stamp, func(t *testing.T) {
			timestamp, err := time.Parse(time.RFC3339Nano, stamp)
			require.NoError(t, err)
			points := []time.Time{timestamp.Add(-time.Nanosecond), timestamp, timestamp.Add(time.Nanosecond)}
			var calls int
			tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
				calls++
				requireDeleteRequest(t, req)
				var request deleteRequest
				require.NoError(t, json.NewDecoder(req.Body).Decode(&request))
				require.Equal(t, `_measurement="readings" AND "sensor"="lab 1"`, request.Predicate)
				start, err := time.Parse(time.RFC3339Nano, request.Start)
				require.NoError(t, err)
				stop, err := time.Parse(time.RFC3339Nano, request.Stop)
				require.NoError(t, err)
				// InfluxDB 2.x deletes both endpoints, including a point at stop.
				var remaining []time.Time
				for _, point := range points {
					if point.Before(start) || point.After(stop) {
						remaining = append(remaining, point)
					}
				}
				points = remaining
				return noContent(), nil
			})
			request := storeRequest(0, tuple.Tuple{"lab 1", stamp}, tuple.Tuple{nil, nil, nil, nil, nil})
			request.Store.Delete = true
			_, err = tx.Store(storeIterator(t.Context(), request))
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Equal(t, []time.Time{timestamp.Add(-time.Nanosecond), timestamp.Add(time.Nanosecond)}, points)
		})
	}
}

func TestStoreDeleteSignals(t *testing.T) {
	for _, tc := range []struct {
		name     string
		doc      string
		protocol bool
		deleted  bool
	}{
		{"protocol", "", true, true},
		{"source metadata", `{"_meta":{"op":"d"}}`, false, true},
		{"escaped metadata", `{"_m\u0065ta":{"op":"\u0064"}}`, false, true},
		{"source update", `{"_meta":{"op":"u"}}`, false, false},
		// Differently cased keys must neither trigger nor override a source deletion.
		{"uppercase metadata", `{"_META":{"op":"d"},"id":7}`, false, false},
		{"uppercase metadata without operation", `{"_META":{"op":"d"},"_meta":{"uuid":"x"},"id":7}`, false, false},
		{"uppercase operation without operation", `{"_meta":{"OP":"d","uuid":"x"},"id":7}`, false, false},
		{"uppercase metadata after update", `{"id":7,"_meta":{"op":"u"},"_META":{"op":"d"}}`, false, false},
		{"uppercase metadata before update", `{"_META":{"op":"d"},"_meta":{"op":"u","uuid":"x"},"id":7}`, false, false},
		{"uppercase operation after update", `{"_meta":{"op":"u","OP":"d"},"id":7}`, false, false},
		{"uppercase operation before update", `{"_meta":{"OP":"d","op":"u"},"id":7}`, false, false},
		{"uppercase metadata after delete", `{"_meta":{"op":"d"},"_META":{"op":"u"}}`, false, true},
		{"uppercase operation after delete", `{"_meta":{"op":"d","OP":"u"}}`, false, true},
		{"uppercase operation value", `{"_meta":{"op":"D"}}`, false, false},
		{"nested metadata", `{"data":{"_meta":{"op":"d"}}}`, false, false},
		{"string containing metadata", `{"note":"\"_meta\":{\"op\":\"d\"}"}`, false, false},
		{"null metadata", `{"_meta":null}`, false, false},
		{"non-string operation", `{"_meta":{"op":true}}`, false, false},
		{"no metadata", `{}`, false, false},
	} {
		for _, exists := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s exists=%t", tc.name, exists), func(t *testing.T) {
				var paths []string
				tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
					paths = append(paths, req.URL.Path)
					return noContent(), nil
				})
				request := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{7.25, "2026-01-01T00:00:00Z", "new"})
				request.Store.DocJson = json.RawMessage(tc.doc)
				request.Store.Delete = tc.protocol
				request.Store.Exists = exists
				_, err := tx.Store(storeIterator(context.Background(), request))
				require.NoError(t, err)
				if tc.deleted {
					require.Equal(t, []string{"/proxy/api/v2/delete"}, paths)
				} else {
					require.Equal(t, []string{"/proxy/api/v2/write"}, paths)
				}
			})
		}
	}
}

func TestStoreSoftDeletes(t *testing.T) {
	for _, hardDelete := range []string{"omitted", "false"} {
		for _, tc := range []struct {
			name     string
			doc      string
			protocol bool
			op       any
			fields   string
		}{
			{"protocol", `{"id":3}`, true, nil, `flow_document="{\"id\":3}"`},
			{"source metadata", `{"id":3,"_meta":{"op":"d"}}`, false, nil, `flow_document="{\"id\":3,\"_meta\":{\"op\":\"d\"}}"`},
			{"operation projection", `{"id":3}`, false, "d", `source_operation="d",flow_document="{\"id\":3}"`},
		} {
			for _, exists := range []bool{false, true} {
				t.Run(fmt.Sprintf("hardDelete=%s/%s/exists=%t", hardDelete, tc.name, exists), func(t *testing.T) {
					var rec recorder
					d := rec.driver(t)
					open := testOpen()
					var endpoint map[string]any
					require.NoError(t, json.Unmarshal(testEndpoint, &endpoint))
					if hardDelete == "false" {
						endpoint["hardDelete"] = false
					}
					raw, err := json.Marshal(endpoint)
					require.NoError(t, err)
					open.Materialization.ConfigJson = raw
					binding := open.Materialization.Bindings[1]
					addProjections(&binding.Collection, pf.Projection{
						Field: "source_operation", Ptr: "/_meta/op", Inference: pf.Inference{Types: []string{"null", "string"}},
					},
					)
					binding.FieldSelection = pf.FieldSelection{
						Keys: []string{"id"}, Values: []string{"flow_published_at", "source_operation"}, Document: "flow_document",
					}
					tx, _, _, err := d.NewTransactor(context.Background(), *open, nil)
					require.NoError(t, err)
					defer tx.Destroy()
					request := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{"2026-01-01T00:00:00Z", tc.op})
					request.Store.DocJson = json.RawMessage(tc.doc)
					request.Store.Delete = tc.protocol
					request.Store.Exists = exists
					_, err = tx.Store(storeIterator(context.Background(), request))
					require.NoError(t, err)
					require.Equal(t, []string{"orders,id=3 " + tc.fields + " 1767225600000000000\n"}, rec.writes)
					require.Empty(t, rec.deletes)
				})
			}
		}
	}
}

func TestStoreSparseUpdatesWithoutMetadata(t *testing.T) {
	for _, exists := range []bool{false, true} {
		t.Run(fmt.Sprintf("exists=%t", exists), func(t *testing.T) {
			var rec recorder
			d := rec.driver(t)
			open := testOpen()
			open.Materialization.Bindings[0].FieldSelection.Values = []string{
				"count", "flow_published_at", "meta", "note", "ok", "temp",
			}
			tx, _, _, err := d.NewTransactor(context.Background(), *open, nil)
			require.NoError(t, err)
			defer tx.Destroy()

			// Without a selected operation or root document, these sparse updates
			// also look like source deletes whose metadata Flow did not forward.
			// Missing point fields alone must never trigger a delete.
			requests := []pm.Request{
				storeRequest(0, tuple.Tuple{"lab 1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, "1970-01-01T00:00:01.000000000Z", nil, nil, nil, nil}),
				storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{nil, "1970-01-01T00:00:04.000000000Z", nil}),
			}
			for i := range requests {
				requests[i].Store.DocJson = nil
				requests[i].Store.Exists = exists
			}
			_, err = tx.Store(storeIterator(context.Background(), requests...))
			require.NoError(t, err)
			require.Equal(t, []string{
				`readings,sensor=lab\ 1 flow_published_at="1970-01-01T00:00:01.000000000Z" 1767225600000000000` + "\n",
			}, rec.writes)
			require.Empty(t, rec.deletes)
		})
	}
}

func TestStoreRootDocumentDeletes(t *testing.T) {
	for _, documentField := range []string{"flow_document", "source_document"} {
		t.Run(documentField, func(t *testing.T) {
			var rec recorder
			d := rec.driver(t)
			open := testOpen()
			for _, binding := range open.Materialization.Bindings {
				binding.Collection.GetProjection("flow_document").Field = documentField
				addProjections(&binding.Collection)
				binding.FieldSelection.Document = documentField
			}
			update := storeRequest(0, tuple.Tuple{"s1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, 22.5})
			update.Store.DocJson = json.RawMessage(`{"sensor":"s1","ts":"2026-01-01T00:00:00Z","temp":22.5}`)
			readingDelete := storeRequest(0, tuple.Tuple{"lab 1", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil})
			readingDelete.Store.DocJson = json.RawMessage(`{"sensor":"lab 1","ts":"2026-01-01T00:00:00Z","_meta":{"op":"d"}}`)
			orderDelete := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{nil, "2026-01-01T12:02:00Z", nil})
			orderDelete.Store.DocJson = json.RawMessage(`{"id":3,"_meta":{"op":"\u0064"}}`)
			sparse := storeRequest(1, tuple.Tuple{int64(4)}, tuple.Tuple{nil, "2026-01-01T12:02:00Z", nil})
			sparse.Store.DocJson = json.RawMessage(`{"id":4}`)
			// Neither the operation projection nor a protocol delete flag is present.
			stream := &storeStream{requests: []pm.Request{
				{Acknowledge: &pm.Request_Acknowledge{}},
				{Flush: &pm.Request_Flush{}},
				update, readingDelete, orderDelete, sparse,
				{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}},
				{Acknowledge: &pm.Request_Acknowledge{}},
			}}
			require.NoError(t, m.RunTransactions(context.Background(), d, stream, open, log.InfoLevel))
			require.Equal(t, []string{
				"/proxy/api/v2/write", "/proxy/api/v2/delete", "/proxy/api/v2/delete", "/proxy/api/v2/write",
			}, rec.paths)
			require.Equal(t, []string{
				`readings,sensor=s1 temp=22.5,` + documentField + `="{\"sensor\":\"s1\",\"ts\":\"2026-01-01T00:00:00Z\",\"temp\":22.5}" 1767225600000000000` + "\n",
				`orders,id=4 ` + documentField + `="{\"id\":4}" 1767268920000000000` + "\n",
			}, rec.writes)
			require.Equal(t, []deleteRequest{
				{Start: "2026-01-01T00:00:00Z", Stop: "2026-01-01T00:00:00Z", Predicate: `_measurement="readings" AND "sensor"="lab 1"`},
				{Start: "1677-09-21T00:12:43.145224194Z", Stop: "2262-04-11T23:47:16.854775806Z", Predicate: `_measurement="orders" AND "id"="3"`},
			}, rec.deletes)
		})
	}
}

func TestStoreDeleteErrors(t *testing.T) {
	for _, failAt := range []string{"write", "delete"} {
		t.Run(failAt, func(t *testing.T) {
			var paths []string
			tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
				paths = append(paths, req.URL.Path)
				if req.URL.Path == "/proxy/api/v2/"+failAt {
					return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader(`{"message":"test failure"}`))}, nil
				}
				return noContent(), nil
			})
			write := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{7.25, "2026-01-01T00:00:00Z", "new"})
			deletion := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{nil, nil, nil})
			deletion.Store.Delete = true
			commit, err := tx.Store(storeIterator(context.Background(), write, deletion, write))
			require.ErrorContains(t, err, "HTTP 400: test failure")
			require.Nil(t, commit)
			if failAt == "write" {
				require.Equal(t, []string{"/proxy/api/v2/write"}, paths)
			} else {
				require.Equal(t, []string{"/proxy/api/v2/write", "/proxy/api/v2/delete"}, paths)
			}
		})
	}
	var calls int
	tx := testTransactor(t, func(*http.Request) (*http.Response, error) {
		calls++
		return noContent(), nil
	})
	request := storeRequest(0, tuple.Tuple{"s1", "invalid"}, tuple.Tuple{nil, nil, nil, nil, nil})
	request.Store.Delete = true
	_, err := tx.Store(storeIterator(context.Background(), request))
	require.ErrorContains(t, err, "encoding delete for measurement")
	require.Zero(t, calls)
}

func TestStoreSkipsEmptyKeyTagDeletes(t *testing.T) {
	var paths []string
	tx := testTransactor(t, func(req *http.Request) (*http.Response, error) {
		paths = append(paths, req.URL.Path)
		return noContent(), nil
	})
	hook := logtest.NewGlobal()
	defer hook.Reset()
	empty := storeRequest(0, tuple.Tuple{"", "2026-01-01T00:00:00Z"}, tuple.Tuple{nil, nil, nil, nil, nil})
	empty.Store.Delete = true
	other := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{nil, nil, nil})
	other.Store.Delete = true
	_, err := tx.Store(storeIterator(context.Background(), empty, other))
	require.NoError(t, err)
	require.Equal(t, []string{"/proxy/api/v2/delete"}, paths)
	var warned bool
	for _, entry := range hook.AllEntries() {
		if entry.Level == log.WarnLevel && strings.Contains(entry.Message, `Skipping a delete for measurement "readings"`) {
			warned = true
		}
	}
	require.True(t, warned)
}

func TestStoreOperationProjection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   any
		deleted bool
	}{
		{"string", "d", true},
		{"escaped JSON string", []byte(`"\u0064"`), true},
		{"update", "u", false},
		{"null", nil, false},
		{"non-string", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var rec recorder
			d := rec.driver(t)
			open := testOpen()
			binding := open.Materialization.Bindings[1]
			addProjections(&binding.Collection, pf.Projection{
				Field: "source_operation", Ptr: "/_meta/op", Inference: pf.Inference{Types: []string{"null", "string"}},
			},
			)
			binding.FieldSelection.Values = []string{"amount", "flow_published_at", "source_operation", "status"}
			tx, _, _, err := d.NewTransactor(context.Background(), *open, nil)
			require.NoError(t, err)
			defer tx.Destroy()
			request := storeRequest(1, tuple.Tuple{int64(3)}, tuple.Tuple{7.25, "2026-01-01T00:00:00Z", tc.value, "d"})
			request.Store.DocJson = nil
			_, err = tx.Store(storeIterator(context.Background(), request))
			require.NoError(t, err)
			if tc.deleted {
				require.Equal(t, []string{"/proxy/api/v2/delete"}, rec.paths)
			} else {
				require.Equal(t, []string{"/proxy/api/v2/write"}, rec.paths)
			}
		})
	}
}

func testTransactor(t *testing.T, roundTrip roundTripFunc) *transactor {
	t.Helper()
	d := driver{httpClient: &http.Client{Transport: roundTrip}}
	tx, _, _, err := d.NewTransactor(context.Background(), *testOpen(), nil)
	require.NoError(t, err)
	t.Cleanup(tx.Destroy)
	return tx.(*transactor)
}

func testOpen() *pm.Request_Open {
	orders := pf.CollectionSpec{
		Name: "tests/orders", Key: []string{"/id"}, UuidPtr: "/_meta/uuid", PartitionTemplate: testJournal("tests/orders/pivot=00"),
		Projections: []pf.Projection{
			{Field: "amount", Ptr: "/amount", Inference: pf.Inference{Types: []string{"number"}}},
			{Field: "flow_document", Ptr: "", Inference: pf.Inference{Types: []string{"object"}}},
			{Field: "flow_published_at", Ptr: "/_meta/uuid", Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST}},
			{Field: "id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST}},
			{Field: "status", Ptr: "/status", Inference: pf.Inference{Types: []string{"string"}}},
		},
	}
	return &pm.Request_Open{
		Materialization: &pf.MaterializationSpec{
			Name: "tests/influxdb", ConnectorType: pf.MaterializationSpec_IMAGE, ConfigJson: testHardDeleteEndpoint,
			Bindings: []*pf.MaterializationSpec_Binding{
				{
					ResourceConfigJson: json.RawMessage(`{"measurement":"readings","timestampField":"ts"}`),
					ResourcePath:       []string{"readings"},
					Collection:         testCollection(),
					FieldSelection:     pf.FieldSelection{Keys: []string{"sensor", "ts"}, Values: []string{"count", "meta", "note", "ok", "temp"}},
					DeltaUpdates:       true,
				},
				{
					ResourceConfigJson: json.RawMessage(`{"measurement":"orders"}`),
					ResourcePath:       []string{"orders"},
					Collection:         orders,
					FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"amount", "flow_published_at", "status"}},
					DeltaUpdates:       true,
				},
			},
			ShardTemplate:       &pc.ShardSpec{Id: "tests/influxdb/shard", MaxTxnDuration: time.Second},
			RecoveryLogTemplate: testJournal("recovery/tests/influxdb/log"),
		},
		Version: "v1",
		Range:   &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
	}
}

func storeRequest(binding uint32, key, values tuple.Tuple) pm.Request {
	return pm.Request{Store: &pm.Request_Store{
		Binding: binding, KeyPacked: key.Pack(), ValuesPacked: values.Pack(), DocJson: json.RawMessage(`{}`),
	}}
}

func storeIterator(ctx context.Context, requests ...pm.Request) *m.StoreIterator {
	stream := &storeStream{requests: append(requests, pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}})}
	return m.NewStoreIterator(ctx, stream, &pm.Request{Flush: &pm.Request_Flush{}})
}

type storeStream struct {
	mu        sync.Mutex
	requests  []pm.Request
	responses []pm.Response
	err       error
}

func (s *storeStream) Send(response *pm.Response) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, *response)
	return nil
}

func (s *storeStream) RecvMsg(request *pm.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		if s.err != nil {
			return s.err
		}
		return io.EOF
	}
	*request = s.requests[0]
	s.requests = s.requests[1:]
	return nil
}

func noContent() *http.Response {
	return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}
}

// recorder accepts every request a transactor sends and records its path and body.
type recorder struct {
	paths, writes []string
	deletes       []deleteRequest
}

func (r *recorder) driver(t *testing.T) driver {
	return driver{httpClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		r.paths = append(r.paths, req.URL.Path)
		if req.URL.Path == "/proxy/api/v2/delete" {
			requireDeleteRequest(t, req)
			var request deleteRequest
			require.NoError(t, json.NewDecoder(req.Body).Decode(&request))
			r.deletes = append(r.deletes, request)
		} else {
			requireWriteRequest(t, req)
			body, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			r.writes = append(r.writes, string(body))
		}
		return noContent(), nil
	})}}
}

// requireCommitted checks the number of commits and the rows reported by non-recovery transaction health logs.
func requireCommitted(t *testing.T, stream *storeStream, hook *logtest.Hook, commits int, rows int64) {
	t.Helper()
	var committed int
	for _, response := range stream.responses {
		if response.StartedCommit != nil {
			committed++
		}
	}
	require.Equal(t, commits, committed)
	var total int64
	for _, entry := range hook.AllEntries() {
		if entry.Message != "transaction health" {
			continue
		}
		raw, err := json.Marshal(entry.Data)
		require.NoError(t, err)
		var health struct {
			Recovery bool   `json:"recovery"`
			Verdict  string `json:"verdict"`
			Fidelity string `json:"fidelity"`
			Actual   struct {
				Total int64 `json:"total"`
			} `json:"actual"`
		}
		require.NoError(t, json.Unmarshal(raw, &health))
		if !health.Recovery {
			require.Equal(t, "ok", health.Verdict)
			require.Equal(t, "total", health.Fidelity)
			total += health.Actual.Total
		}
	}
	require.Equal(t, rows, total)
}
