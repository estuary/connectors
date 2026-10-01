package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
	"resty.dev/v3"
)

func init() {
	stdsql.Register("fakesnowflake", fakeSnowflakeDriver{})
}

// fakeSnowflakeDB opens a connection to the fake Snowflake, which lists the channels
// the fake sidecar holds committed offset tokens for.
func fakeSnowflakeDB(t *testing.T) *stdsql.DB {
	t.Helper()
	db, err := stdsql.Open("fakesnowflake", "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// fakeSidecarChannels reports the channels the fake sidecar holds committed offset
// tokens for, which stand in for the channels on a table's default pipe.
func fakeSidecarChannels() ([]string, error) {
	var path = os.Getenv("FAKE_SIDECAR_STATE")
	if path == "" {
		return nil, nil
	}
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var state struct {
		Committed map[string]string `json:"committed"`
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return nil, err
	}
	var names []string
	for name := range state.Committed {
		names = append(names, name)
	}
	return names, nil
}

// fakeSnowflakeDriver is a database/sql driver whose one statement is SHOW CHANNELS.
// The PIPE form answers from the fake sidecar's state and the TABLE form answers
// nothing, as a table streamed into only through its default pipe would.
type fakeSnowflakeDriver struct{}

func (fakeSnowflakeDriver) Open(string) (driver.Conn, error) { return fakeSnowflakeConn{}, nil }

type fakeSnowflakeConn struct{}

func (fakeSnowflakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("the fake Snowflake prepares no statements")
}
func (fakeSnowflakeConn) Close() error { return nil }
func (fakeSnowflakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("the fake Snowflake has no transactions")
}

func (fakeSnowflakeConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.HasPrefix(query, "SHOW CHANNELS IN PIPE "):
		names, err := fakeSidecarChannels()
		return &fakeChannelRows{names: names}, err
	case strings.HasPrefix(query, "SHOW CHANNELS IN TABLE "):
		return &fakeChannelRows{}, nil
	default:
		return nil, fmt.Errorf("the fake Snowflake does not answer %q", query)
	}
}

// fakeChannelRows is a channel listing of one column, name.
type fakeChannelRows struct {
	names []string
	next  int
}

func (*fakeChannelRows) Columns() []string { return []string{"name"} }
func (*fakeChannelRows) Close() error      { return nil }
func (r *fakeChannelRows) Next(dest []driver.Value) error {
	if r.next >= len(r.names) {
		return io.EOF
	}
	dest[0] = r.names[r.next]
	r.next++
	return nil
}

// fakeStreamManager builds a snowpipe_streaming manager against a fake Snowflake
// that records every channel it is asked to drop, reported by the returned func.
func fakeStreamManager(t *testing.T, task string) (*streamManager, func() []string) {
	t.Helper()
	var drops []string
	var mux = http.NewServeMux()
	mux.HandleFunc("POST /v1/streaming/channels/drop", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Channel string `json:"channel"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
		drops = append(drops, req.Channel)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"message":"Success","status_code":0}`)
	})
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	pkey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)
	var sm = &streamManager{
		c: &streamClient{
			r:        resty.New().SetBaseURL(ts.URL + "/v1/streaming").SetDisableWarn(true),
			key:      pkey,
			user:     "TEST_USER",
			database: "DB",
			account:  "TEST_ACCOUNT",
		},
		tableStreams: map[int]*tableStream{},
		channelName:  newChannelName(task, 0),
	}
	return sm, func() []string { return drops }
}

// newFakeStreamV2Manager builds a streaming v2 manager for one shard of a task,
// backed by the fake Snowflake and the fake sidecar, whose shared state outlives
// the manager through FAKE_SIDECAR_STATE.
func newFakeStreamV2Manager(t *testing.T, ctx context.Context, cfg *config, task string, keyRange *pf.RangeSpec) *streamV2Manager {
	t.Helper()
	sm, _ := fakeStreamManager(t, task)
	var m = newStreamV2Manager(ctx, cfg, fakeSnowflakeDB(t), testDialect, sm, task, "acct", keyRange)
	m.argv = fakeSidecarArgv(t)
	t.Cleanup(m.stop)
	return m
}

// packKey packs a document key the way the runtime does, so a test row routes
// through the same hash the runtime would route its document by.
func packKey(key tuple.TupleElement) []byte {
	return tuple.Tuple{key}.Pack()
}

// testWriteRow stores one converted row under the key in its first value, which is
// the KEY column of every table these tests store into.
func testWriteRow(ctx context.Context, m *streamV2Manager, binding int, converted []any) error {
	return m.writeRow(ctx, binding, packKey(converted[0]), converted)
}

// soleItem returns the one non-nil checkpoint item a flush produced for a binding,
// for tests pinned to a single-channel layout.
func soleCheckpointItem(t *testing.T, entries map[int]streamV2Checkpoint, binding int) *streamV2ChannelCheckpointItem {
	t.Helper()
	var sv2Checkpoint []*streamV2ChannelCheckpointItem
	for _, sv2ChannelCheckpointItem := range entries[binding] {
		if sv2ChannelCheckpointItem != nil {
			sv2Checkpoint = append(sv2Checkpoint, sv2ChannelCheckpointItem)
		}
	}
	if len(sv2Checkpoint) != 1 {
		t.Fatalf("expected exactly one item for binding %d, got %d", binding, len(sv2Checkpoint))
	}
	return sv2Checkpoint[0]
}

// singleChannelLayout pins streamV2ChannelsPerShard to one for a test whose
// assertions follow a single channel's counter, committed offset, or token. The write
// path's behavior per channel is identical at any depth; these tests are about that
// behavior, not about routing, which has coverage of its own.
func singleChannelLayout(t *testing.T) {
	var restore = streamV2ChannelsPerShard
	t.Cleanup(func() { streamV2ChannelsPerShard = restore })
	streamV2ChannelsPerShard = 1
}

// fullKeyRange is the key range of a single-channel layout on an unsplit task.
var fullKeyRange = streamV2Range{keyEnd: math.MaxUint32}
