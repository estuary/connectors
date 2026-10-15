package connector

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	snowflake_auth "github.com/estuary/connectors/go/auth/snowflake"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
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

// Draining staged work removes this shard's range entry, the bare staged
// fields a build predating range scoping wrote, and the streaming v2 entries
// of this shard's range. Another shard's range entry and streaming v2 entries
// are untouched, because they are that shard's to drain. An item of every
// other write path is patched to null whole.
func TestCheckpointEncodeClearRemovesOnlyThisShardsWork(t *testing.T) {
	var d = &transactor{
		rangeKey: "00000000-7fffffff",
		cp: checkpoint{
			"a.v1": {Table: "A", Query: "MERGE INTO a", StreamV2: streamV2Checkpoint{
				{keyBegin: 0x00000000, keyEnd: 0x3fffffff}: {ChannelName: "chan-0"},
				{keyBegin: 0x40000000, keyEnd: 0x7fffffff}: {ChannelName: "chan-1"},
				{keyBegin: 0x80000000, keyEnd: 0xffffffff}: {ChannelName: "chan-2"},
			}},
			"c.v1": {StreamBlobs: []*blobMetadata{{Path: "blob"}}},
		},
	}

	out, err := d.encodeCheckpointClear([]string{"a.v1", "c.v1"})
	require.NoError(t, err)
	require.JSONEq(t, `{
		"a.v1": {
			"00000000-7fffffff": null,
			"Table": null, "Query": null, "StagedDir": null, "Version": null, "Round": null, "Rows": null,
			"StreamV2": {"00000000-3fffffff": null, "40000000-7fffffff": null}
		},
		"c.v1": null
	}`, string(out))
}

// One shard's clearing patch leaves the other shard's staged work pending,
// even when the runtime applies it after that shard's next checkpoint.
func TestCheckpointClearLeavesPeerStagedWork(t *testing.T) {
	var lower = &transactor{rangeKey: "00000000-7fffffff", cp: checkpoint{
		"a.v1": {Table: "A", Query: "MERGE INTO a (lower)", StagedDir: "dir-lower"},
	}}
	var upper = &transactor{rangeKey: "80000000-ffffffff", cp: checkpoint{
		"a.v1": {Table: "A", Query: "MERGE INTO a (upper)", StagedDir: "dir-upper"},
	}}

	var state = []byte(`{}`)
	for _, d := range []*transactor{lower, upper} {
		patch, err := d.encodeCheckpoint()
		require.NoError(t, err)
		state, err = jsonpatch.MergePatch(state, patch)
		require.NoError(t, err)
	}

	cleared, err := lower.encodeCheckpointClear([]string{"a.v1"})
	require.NoError(t, err)
	state, err = jsonpatch.MergePatch(state, cleared)
	require.NoError(t, err)

	cp, err := decodeCheckpoint(state, lower.rangeKey)
	require.NoError(t, err)
	require.Empty(t, cp)

	cp, err = decodeCheckpoint(state, upper.rangeKey)
	require.NoError(t, err)
	require.Equal(t, "MERGE INTO a (upper)", cp["a.v1"].Query)
	require.Equal(t, "dir-upper", cp["a.v1"].StagedDir)
}

// Staged work persisted before range scoping is drained by clearing the bare
// fields that hold it.
func TestCheckpointClearDrainsLegacyStagedWork(t *testing.T) {
	var state = []byte(`{"a.v1": {"Table": "A", "Query": "MERGE INTO a", "StagedDir": "dir-a", "Version": "v", "Round": 3, "Rows": 7}}`)

	var d = &transactor{rangeKey: "00000000-ffffffff"}
	require.NoError(t, d.UnmarshalState(state))
	require.Equal(t, "MERGE INTO a", d.cp["a.v1"].Query)

	cleared, err := d.encodeCheckpointClear([]string{"a.v1"})
	require.NoError(t, err)
	state, err = jsonpatch.MergePatch(state, cleared)
	require.NoError(t, err)

	cp, err := decodeCheckpoint(state, d.rangeKey)
	require.NoError(t, err)
	require.Empty(t, cp)
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

// A split range marks the shard as scaled out only on the v2 runtime, where
// the runtime aggregates every shard's connector state into one document.
func TestIsScaledOut(t *testing.T) {
	var v2 = testStreamingSpec(t, "", true)
	var v1 = testStreamingSpec(t, "", false)
	var full = &pf.RangeSpec{KeyEnd: math.MaxUint32}
	var lower = &pf.RangeSpec{KeyEnd: 0x7fffffff}
	var upper = &pf.RangeSpec{KeyBegin: 0x80000000, KeyEnd: math.MaxUint32}

	require.False(t, isScaledOut(v2, full))
	require.True(t, isScaledOut(v2, lower))
	require.True(t, isScaledOut(v2, upper))
	require.False(t, isScaledOut(v1, lower))
	require.False(t, isScaledOut(v2, nil))
}

// A scaled-out shard refuses a binding that would take the snowpipe_streaming
// write path, since that path persists its staged blobs at the bare state key
// and another shard would read them as its own.
func TestAddBindingRejectsStreamingWhenScaledOut(t *testing.T) {
	var d = &transactor{
		scaledOut:    true,
		cfg:          config{Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT}},
		featureFlags: map[string]bool{flagSnowpipeStreaming: true},
		ep:           &sql.Endpoint[config]{Dialect: snowflakeDialect("", "", nil)},
	}
	var err = d.addBinding(context.Background(), sql.Table{
		TableShape: sql.TableShape{Path: []string{"tbl"}, DeltaUpdates: true},
		Identifier: "tbl",
	}, checkpointItem{})
	require.ErrorContains(t, err, flagSnowpipeStreaming)
	require.ErrorContains(t, err, "more than one shard")
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

func TestStreamingChannelQuotesSchema(t *testing.T) {
	for _, tc := range []struct {
		schema string
		want   string
	}{
		{schema: "simple_schema", want: "SIMPLE_SCHEMA"},
		{schema: "hyphen-schema", want: `"hyphen-schema"`},
		{schema: "table", want: `"table"`},
	} {
		t.Run(tc.schema, func(t *testing.T) {
			var gotSchema, gotTable string
			var mux = http.NewServeMux()
			mux.HandleFunc("POST /v1/streaming/channels/open", func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Schema string `json:"schema"`
					Table  string `json:"table"`
				}
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				gotSchema, gotTable = body.Schema, body.Table
				w.Header().Set("Content-Type", "application/json")
				fmt.Fprint(w, `{"message":"Success","status_code":0}`)
			})
			var ts = httptest.NewServer(mux)
			t.Cleanup(ts.Close)
			pkey, err := rsa.GenerateKey(rand.Reader, 1024)
			require.NoError(t, err)

			var d = &transactor{
				cfg:          config{Credentials: &snowflake_auth.CredentialConfig{AuthType: snowflake_auth.JWT}},
				featureFlags: map[string]bool{flagSnowpipeStreaming: true},
				ep:           &sql.Endpoint[config]{Dialect: snowflakeDialect("PUBLIC", timestampTypeLTZ, nil)},
				snowpipeStreaming: &streamManager{
					c: &streamClient{
						r:        resty.New().SetBaseURL(ts.URL + "/v1/streaming").SetDisableWarn(true),
						key:      pkey,
						user:     "TEST_USER",
						database: "D",
						account:  "TEST_ACCOUNT",
					},
					tableStreams: make(map[int]*tableStream),
				},
			}
			var target = sql.Table{TableShape: sql.TableShape{Path: []string{tc.schema, "my-table"}, DeltaUpdates: true}}
			require.NoError(t, d.addBinding(context.Background(), target, checkpointItem{}))
			require.Equal(t, tc.want, gotSchema)
			require.Equal(t, `"my-table"`, gotTable)
		})
	}
}
