package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/filesink"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
	"go.gazette.dev/core/consumer/protocol"
)

type testStream struct {
	requests []*pm.Request
}

func (*testStream) Send(*pm.Response) error { return nil }

func (s *testStream) RecvMsg(r *pm.Request) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	*r, s.requests = *s.requests[0], s.requests[1:]
	return nil
}

// testDriver opens a new store on the seed's test share for each transactor.
func testDriver(seed *smbStore) smbDriver {
	d := driver
	d.NewStore = func(ctx context.Context, _ filesink.Config, _ map[string]bool) (filesink.Store[*smbUpload], error) {
		conn := seed.connection.(*testConnection)
		conn.state.connections++
		return &smbStore{connection: &testConnection{share: conn.share, state: conn.state}}, nil
	}
	return d
}

func TestTransactorResume(t *testing.T) {
	ctx := t.Context()
	seed, share := newTestStore(t)
	d := testDriver(seed)
	encoded, err := json.Marshal(testConfig())
	require.NoError(t, err)
	open := pm.Request_Open{Materialization: &pf.MaterializationSpec{
		ConfigJson: encoded,
		Bindings: []*pf.MaterializationSpec_Binding{
			{
				ResourceConfigJson: json.RawMessage(`{"path":"orders"}`),
				ResourcePath:       []string{"orders"},
				StateKey:           "orders",
				FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"canary", "status"}},
			},
			{
				ResourceConfigJson: json.RawMessage(`{"path":"events/raw"}`),
				ResourcePath:       []string{"events/raw"},
				StateKey:           "events/raw",
				FieldSelection:     pf.FieldSelection{Keys: []string{"region", "seq"}, Values: []string{"canary"}},
			},
		},
	}}
	start := func(state json.RawMessage) m.Transactor {
		transactor, _, _, err := d.NewTransactor(ctx, open, m.NewBindingEvents())
		require.NoError(t, err)
		t.Cleanup(transactor.Destroy)
		require.NoError(t, transactor.UnmarshalState(state))
		return transactor
	}
	storeRequest := func(binding uint32, key, values tuple.Tuple, document string, deleted bool) *pm.Request {
		return &pm.Request{Store: &pm.Request_Store{
			Binding: binding, KeyPacked: key.Pack(), ValuesPacked: values.Pack(),
			DocJson: json.RawMessage(document), Exists: deleted, Delete: deleted,
		}}
	}
	stage := func(transactor m.Transactor, requests ...*pm.Request) json.RawMessage {
		stream := &testStream{requests: append(requests, &pm.Request{StartCommit: &pm.Request_StartCommit{}})}
		it := m.NewStoreIterator(ctx, stream, &pm.Request{Flush: &pm.Request_Flush{}})
		commit, err := transactor.Store(it)
		require.NoError(t, err)
		state, future := commit(ctx, &protocol.Checkpoint{})
		if future != nil {
			require.NoError(t, future.Err())
		}
		require.NotNil(t, state)
		require.False(t, state.MergePatch)
		return state.UpdatedJson
	}
	ack := func(transactor m.Transactor) json.RawMessage {
		state, err := transactor.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, state)
		return state.UpdatedJson
	}

	first := start(json.RawMessage(`{}`))
	state := stage(first,
		storeRequest(0, tuple.Tuple{int64(1)}, tuple.Tuple{"o1", "new"}, `{"id":1}`, false),
		storeRequest(0, tuple.Tuple{int64(2)}, tuple.Tuple{`o2, "quoted"`, "paid"}, `{"id":2}`, false),
		storeRequest(1, tuple.Tuple{"us", int64(7)}, tuple.Tuple{"e7"}, `{"region":"us","seq":7}`, false),
	)
	var partials int
	require.NoError(t, filepath.WalkDir(share.root, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if !entry.IsDir() {
			require.True(t, strings.HasSuffix(path, ".csv.gz.partial"), "uncommitted file: %s", path)
			partials++
		}
		return nil
	}))
	require.Equal(t, 2, partials)
	first.Destroy()

	// Recover a committed checkpoint whose uploads have not been acknowledged.
	raw := testConfig()
	raw["directory"] = "/exports//old/../flow/"
	open.Materialization.ConfigJson, err = json.Marshal(raw)
	require.NoError(t, err)
	recovered := start(state)
	ack(recovered)
	files, rows := readCSVFiles(t, share, "exports/flow")
	require.Len(t, rows, 3)
	recovered.Destroy()

	// Repeat that checkpoint after a crash between renaming and persisting the acknowledgement.
	replayed := start(state)
	ack(replayed)
	replayedFiles, replayedRows := readCSVFiles(t, share, "exports/flow")
	require.Equal(t, files, replayedFiles)
	require.Equal(t, rows, replayedRows)
	stage(replayed, storeRequest(0, tuple.Tuple{int64(3)}, tuple.Tuple{"o3", "new"}, `{"id":3}`, false))
	state = ack(replayed)
	initial, rows := readCSVFiles(t, share, "exports/flow")
	want, err := os.ReadFile(".snapshots/TestIntegrationMaterialize-initial")
	require.NoError(t, err)
	require.Equal(t, string(want), strings.Join(rows, "\n")+"\n")
	replayed.Destroy()

	raw["directory"] = `\exports\flow`
	open.Materialization.ConfigJson, err = json.Marshal(raw)
	require.NoError(t, err)
	resumed := start(state)
	stage(resumed,
		storeRequest(0, tuple.Tuple{int64(2)}, tuple.Tuple{"o2-v2", "shipped"}, `{"id":2}`, false),
		storeRequest(0, tuple.Tuple{int64(3)}, tuple.Tuple{nil, nil}, `{"id":3,"_meta":{"op":"d"}}`, true),
		storeRequest(0, tuple.Tuple{int64(4)}, tuple.Tuple{"o4", "new"}, `{"id":4}`, false),
	)
	ack(resumed)
	final, rows := readCSVFiles(t, share, "exports/flow")
	want, err = os.ReadFile(".snapshots/TestIntegrationMaterialize-resumed")
	require.NoError(t, err)
	require.Equal(t, string(want), strings.Join(rows, "\n")+"\n")
	for key, contents := range initial {
		require.Equal(t, contents, final[key], "resuming must preserve %s", key)
	}
	require.Contains(t, final, "exports/flow/orders/v0000000000/00000000000000000002.csv.gz")
}

func TestDestroyAfterStoreError(t *testing.T) {
	ctx := t.Context()
	seed, share := newTestStore(t)
	d := testDriver(seed)
	encoded, err := json.Marshal(testConfig())
	require.NoError(t, err)
	transactor, _, _, err := d.NewTransactor(ctx, pm.Request_Open{Materialization: &pf.MaterializationSpec{
		ConfigJson: encoded,
		Bindings: []*pf.MaterializationSpec_Binding{{
			ResourceConfigJson: json.RawMessage(`{"path":"orders"}`),
			ResourcePath:       []string{"orders"},
			StateKey:           "orders",
			FieldSelection:     pf.FieldSelection{Keys: []string{"id"}, Values: []string{"canary", "status"}},
		}},
	}}, m.NewBindingEvents())
	require.NoError(t, err)
	require.NoError(t, transactor.UnmarshalState(json.RawMessage(`{}`)))

	// The stream ends mid-transaction, leaving the upload waiting on more rows.
	stream := &testStream{requests: []*pm.Request{{Store: &pm.Request_Store{
		KeyPacked: tuple.Tuple{int64(1)}.Pack(), ValuesPacked: tuple.Tuple{"o1", "new"}.Pack(), DocJson: json.RawMessage(`{"id":1}`),
	}}}}
	_, err = transactor.Store(m.NewStoreIterator(ctx, stream, &pm.Request{Flush: &pm.Request_Flush{}}))
	require.ErrorContains(t, err, "store iterator error")
	require.Eventually(t, func() bool {
		_, err := os.Stat(filepath.Join(share.root, "exports/flow/orders/v0000000000/00000000000000000000.csv.gz.partial"))
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)

	destroyed := make(chan struct{})
	go func() {
		transactor.Destroy()
		close(destroyed)
	}()
	select {
	case <-destroyed:
	case <-time.After(5 * time.Second):
		t.Fatal("Destroy blocked on an abandoned upload")
	}
}
