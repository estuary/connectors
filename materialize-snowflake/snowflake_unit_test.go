package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	"resty.dev/v3"
)

func TestAcknowledgeSubsetLeavesOtherKeysPending(t *testing.T) {
	d := &transactor{
		cp: checkpoint{
			"a_table.v1": {Table: "a_table", Query: "MERGE INTO a", StagedDir: "dir"},
		},
		bindings: []*binding{{target: sql.Table{StateKey: "a_table.v1"}}},
	}

	// The staged entry's state key is not requested: nothing may execute (the
	// nil database would panic if a query ran) and no state update is
	// returned, so the entry remains pending in the persisted state.
	state, err := d.Acknowledge(context.Background(), nil, []string{"other_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotNil(t, d.cp["a_table.v1"])
	require.True(t, d.didRecovery)
}

// The streaming v2 item is durable per-binding state, not a staged
// transaction: the checkpoint-clearing pass of Acknowledge - which is also what
// the Apply-time pending-transaction drain relies on - must leave it in place,
// and must report no state update on its account, since an update which changes
// nothing would burn an iteration of the runtime's bounded Apply loop.
func TestAcknowledgeKeepsStreamV2Item(t *testing.T) {
	d := &transactor{
		cp: checkpoint{
			"a_table.v1": {Table: "a_table", StreamV2: streamV2Checkpoint{
				fullKeyRange: {ChannelName: "chan", Routed: 42},
			}},
		},
		bindings: []*binding{{target: sql.Table{StateKey: "a_table.v1"}, streamingV2: true}},
		be:       m.NewBindingEvents(),
		// The rows the item accounts for were committed as the checkpoint was
		// produced, so Acknowledge needs nothing from the manager and no sidecar
		// is started.
		snowpipeStreamingV2: &streamV2Manager{},
	}

	state, err := d.Acknowledge(context.Background(), nil, []string{"a_table.v1"})
	require.NoError(t, err)
	require.Nil(t, state)
	require.Equal(t, int64(42), d.cp["a_table.v1"].StreamV2[fullKeyRange].Routed)
}

// Staged work is persisted under its state key and then the shard's range
// key, so disjoint ranges give disjoint entries. Items of every other write
// path keep the bare state key.
func TestCheckpointEncodeScopesStagedWorkByRange(t *testing.T) {
	var d = &transactor{
		rangeKey: "00000000-ffffffff",
		cp: checkpoint{
			"a.v1": {Table: "A", Query: "MERGE INTO a", StagedDir: "dir-a", Version: "v", Round: 3, Rows: 7},
			"b.v1": {Table: "B", StreamV2: streamV2Checkpoint{fullKeyRange: {ChannelName: "chan", Routed: 42}}},
			"c.v1": {StreamBlobs: []*blobMetadata{{Path: "blob"}}, EncryptionKey: "key"},
		},
	}

	out, err := d.encodeCheckpoint()
	require.NoError(t, err)

	var want = map[string]any{
		"a.v1": map[string]any{"00000000-ffffffff": d.cp["a.v1"]},
		"b.v1": d.cp["b.v1"],
		"c.v1": d.cp["c.v1"],
	}
	wantJSON, err := json.Marshal(want)
	require.NoError(t, err)
	require.JSONEq(t, string(wantJSON), string(out))
}

// Streaming v2 state is persisted at the bare state key, so a staged item
// that carries recovered streaming v2 state in memory leaves it out of its
// range entry.
func TestCheckpointEncodeKeepsStreamV2OutOfRangeEntry(t *testing.T) {
	var d = &transactor{
		rangeKey: "00000000-ffffffff",
		cp: checkpoint{
			"a.v1": {
				Table: "A", Query: "MERGE INTO a", StagedDir: "dir-a",
				StreamV2: streamV2Checkpoint{fullKeyRange: {ChannelName: "chan", Routed: 42}},
			},
		},
	}

	out, err := d.encodeCheckpoint()
	require.NoError(t, err)

	var got map[string]map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &got))
	require.Len(t, got, 1)
	require.Len(t, got["a.v1"], 1)
	require.NotContains(t, string(got["a.v1"]["00000000-ffffffff"]), "StreamV2")
	require.Contains(t, string(got["a.v1"]["00000000-ffffffff"]), "MERGE INTO a")
}

func TestCheckpointEncodeRequiresRangeKeyForStagedWork(t *testing.T) {
	var d = &transactor{cp: checkpoint{"a.v1": {Query: "MERGE INTO a"}}}
	_, err := d.encodeCheckpoint()
	require.Error(t, err)
}

// A drained state key is patched to null whole, which takes its range entries,
// any item persisted before range scoping, and its streaming v2 state.
func TestCheckpointEncodeClearNullsStateKey(t *testing.T) {
	var d = &transactor{
		rangeKey: "00000000-ffffffff",
		cp: checkpoint{
			"a.v1": {Query: "MERGE INTO a"},
			"c.v1": {StreamBlobs: []*blobMetadata{{Path: "blob"}}},
		},
	}

	out, err := d.encodeCheckpointClear([]string{"a.v1", "c.v1"})
	require.NoError(t, err)
	require.JSONEq(t, `{"a.v1":null,"c.v1":null}`, string(out))
}

func TestCheckpointDecodeRangeScoped(t *testing.T) {
	var persisted = `{
		"a.v1": {
			"00000000-ffffffff": {"Table": "A", "Query": "MERGE INTO a", "StagedDir": "dir-a"},
			"Table": "A",
			"StreamV2": {"00000000-ffffffff": {"ChannelName": "chan", "Routed": 42}}
		},
		"b.v1": {"StreamBlobs": [{"Path": "blob"}], "EncryptionKey": "key"}
	}`

	cp, err := decodeCheckpoint(json.RawMessage(persisted), "00000000-ffffffff")
	require.NoError(t, err)
	require.Len(t, cp, 2)

	// The range entry's staged work and the bare fields' streaming v2 state
	// are one working item, as they were when the whole item lived at the
	// bare state key.
	require.Equal(t, "MERGE INTO a", cp["a.v1"].Query)
	require.Equal(t, "dir-a", cp["a.v1"].StagedDir)
	require.Equal(t, int64(42), cp["a.v1"].StreamV2[fullKeyRange].Routed)

	require.Equal(t, "blob", cp["b.v1"].StreamBlobs[0].Path)
	require.Equal(t, "key", cp["b.v1"].EncryptionKey)
}

// A checkpoint written before range scoping holds staged work in the state
// key's bare fields, and it is still pending work for this shard.
func TestCheckpointDecodeLegacyStagedWork(t *testing.T) {
	var persisted = `{"a.v1": {"Table": "A", "Query": "MERGE INTO a", "StagedDir": "dir-a"}}`

	cp, err := decodeCheckpoint(json.RawMessage(persisted), "00000000-ffffffff")
	require.NoError(t, err)
	require.Equal(t, "MERGE INTO a", cp["a.v1"].Query)
	require.Equal(t, "dir-a", cp["a.v1"].StagedDir)
}

// Another shard's range entry is not this shard's work.
func TestCheckpointDecodeIgnoresPeerRanges(t *testing.T) {
	var persisted = `{
		"a.v1": {
			"00000000-7fffffff": {"Query": "MERGE INTO a (lower half)"},
			"80000000-ffffffff": {"Query": "MERGE INTO a (upper half)"}
		}
	}`

	cp, err := decodeCheckpoint(json.RawMessage(persisted), "80000000-ffffffff")
	require.NoError(t, err)
	require.Len(t, cp, 1)
	require.Equal(t, "MERGE INTO a (upper half)", cp["a.v1"].Query)

	cp, err = decodeCheckpoint(json.RawMessage(persisted), "c0000000-ffffffff")
	require.NoError(t, err)
	require.Empty(t, cp)
}

// A build predating range scoping reads the state key's value as an item of
// its own and, on its next checkpoint, writes that item's empty fields back
// beside the range entry. Those fields carry no work, and the staged work is
// still pending.
func TestCheckpointDecodeSkipsEmptyFieldsWrittenByOlderBuild(t *testing.T) {
	var persisted = `{
		"a.v1": {
			"00000000-ffffffff": {"Table": "A", "Query": "MERGE INTO a", "StagedDir": "dir-a"},
			"Table": "", "Query": "", "StagedDir": "", "PipeName": "", "Version": "",
			"EncryptionKey": "", "Round": 0, "Rows": 0, "StreamBlobs": null, "PipeFiles": null
		}
	}`

	cp, err := decodeCheckpoint(json.RawMessage(persisted), "00000000-ffffffff")
	require.NoError(t, err)
	require.Len(t, cp, 1)
	require.Equal(t, "MERGE INTO a", cp["a.v1"].Query)
	require.Equal(t, "dir-a", cp["a.v1"].StagedDir)
}

// Encoding, applying the patch to persisted state, and decoding under the same
// range yields the working checkpoint that was encoded.
func TestCheckpointRoundTrip(t *testing.T) {
	var d = &transactor{
		rangeKey: "00000000-ffffffff",
		cp: checkpoint{
			"a.v1": {Table: "A", Query: "MERGE INTO a", StagedDir: "dir-a", Version: "v", Round: 3, Rows: 7},
			"b.v1": {Table: "B", StreamV2: streamV2Checkpoint{fullKeyRange: {ChannelName: "chan", Routed: 42}}},
		},
	}

	patch, err := d.encodeCheckpoint()
	require.NoError(t, err)
	state, err := jsonpatch.MergePatch([]byte(`{}`), patch)
	require.NoError(t, err)

	var got = &transactor{rangeKey: d.rangeKey}
	require.NoError(t, got.UnmarshalState(state))
	require.Equal(t, d.cp, got.cp)
}

func TestSpecification(t *testing.T) {
	var resp, err = NewDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func TestValidHost(t *testing.T) {
	for _, tt := range []struct {
		host string
		want error
	}{
		{"orgname-accountname.snowflakecomputing.com", nil},
		{"identifer.snowflakecomputing.com", nil},
		{"ORGNAME-accountname.snowFLAKEcomputing.coM", nil},
		{"orgname-accountname.aws.us-east-2.snowflakecomputing.com", nil},
		{"http://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "http://orgname-accountname.snowflakecomputing.com")},
		{"https://orgname-accountname.snowflakecomputing.com", fmt.Errorf("invalid host %q (must not include a protocol)", "https://orgname-accountname.snowflakecomputing.com")},
		{"orgname-accountname.snowflakecomputin.com", fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", "orgname-accountname.snowflakecomputin.com")},
	} {
		t.Run(tt.host, func(t *testing.T) {
			require.Equal(t, tt.want, validHost(tt.host))
		})
	}
}

// A checkpoint item of staged work leaves a prior streaming v2 item's state in
// place, because the runtime applies each checkpoint as an RFC 7396 merge patch
// and the staged item omits StreamV2. Acknowledge drains such an item all the
// same, and its clearing removes the streaming v2 state with it.
func TestAcknowledgeDrainsStagedWorkBesideStreamV2(t *testing.T) {
	var persisted, err = json.Marshal(checkpoint{
		"a_table.v1": {Table: "a_table", StreamV2: streamV2Checkpoint{
			fullKeyRange: {ChannelName: "chan", Routed: 42},
		}},
	})
	require.NoError(t, err)

	// The first flush of the snowpipe_streaming path after a downgrade.
	var token = "base:1"
	patch, err := json.Marshal(checkpoint{
		"a_table.v1": {StreamBlobs: []*blobMetadata{{Path: "blob", Chunks: []uploadChunkMetadata{{
			Database: "D", Schema: "S", Table: "T",
			Channels: []uploadChunkChannelMetadata{{ChannelName: "C", OffsetToken: token}},
		}}}}, EncryptionKey: "key"},
	})
	require.NoError(t, err)

	var before, after any
	require.NoError(t, json.Unmarshal(persisted, &before))
	require.NoError(t, json.Unmarshal(patch, &after))
	merged, err := json.Marshal(boilerplate.ApplyMergePatch(before, after))
	require.NoError(t, err)

	// The channel already holds the blob's token, so the drain registers nothing
	// and only asks Snowflake whether that token is persisted.
	var mux = http.NewServeMux()
	mux.HandleFunc("POST /v1/streaming/channels/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"message":"Success","status_code":0,"channels":[{"persisted_offset_token":%q,"status_code":0}]}`, token)
	})
	var ts = httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	pkey, err := rsa.GenerateKey(rand.Reader, 1024)
	require.NoError(t, err)

	var d = &transactor{
		bindings: []*binding{{target: sql.Table{TableShape: sql.TableShape{Path: []string{"a_table"}}, StateKey: "a_table.v1"}, streaming: true}},
		be:       m.NewBindingEvents(),
		snowpipeStreaming: &streamManager{
			c: &streamClient{
				r:        resty.New().SetBaseURL(ts.URL + "/v1/streaming").SetDisableWarn(true),
				key:      pkey,
				user:     "TEST_USER",
				database: "D",
				account:  "TEST_ACCOUNT",
			},
			tableStreams: map[int]*tableStream{
				0: {channel: &channel{Schema: "S", Table: "T", ChannelName: "C", OffsetToken: &token}},
			},
		},
	}
	require.NoError(t, d.UnmarshalState(merged))
	require.NotEmpty(t, d.cp["a_table.v1"].StreamV2, "the recovered item carries both")

	state, err := d.Acknowledge(context.Background(), nil, []string{"a_table.v1"})
	require.NoError(t, err)
	require.NotNil(t, state)
	require.JSONEq(t, `{"a_table.v1":null}`, string(state.UpdatedJson))
	require.Nil(t, d.cp["a_table.v1"])
}
