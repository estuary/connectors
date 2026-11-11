package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

const (
	fullRangeKey  = "00000000-ffffffff"
	lowerRangeKey = "00000000-7fffffff"
	upperRangeKey = "80000000-ffffffff"
)

// recordingSnowflake is a database/sql driver that records every statement
// it is asked to run and answers MERGE INTO and COPY INTO with the result
// rows Snowflake returns for them.
type recordingSnowflake struct{}

type recordingSnowflakeConn struct{}

var recorded struct {
	mu         sync.Mutex
	statements []string
}

// recordedArgs holds each recorded statement's bound arguments, by position.
var recordedArgs [][]any

func (recordingSnowflake) Open(string) (driver.Conn, error) { return recordingSnowflakeConn{}, nil }

func (recordingSnowflakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not implemented")
}
func (recordingSnowflakeConn) Close() error { return nil }
func (recordingSnowflakeConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin is not implemented")
}

func (c recordingSnowflakeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if _, err := c.QueryContext(ctx, query, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (recordingSnowflakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	recorded.statements = append(recorded.statements, query)
	var values []any
	for _, a := range args {
		values = append(values, a.Value)
	}
	recordedArgs = append(recordedArgs, values)

	switch {
	case strings.HasPrefix(query, "\nMERGE INTO"):
		return &resultRows{
			cols: []string{"number of rows inserted", "number of rows updated", "number of rows deleted"},
			rows: [][]driver.Value{{"1", "0", "0"}},
		}, nil
	case strings.HasPrefix(query, "\nCOPY INTO"):
		return &resultRows{cols: []string{"rows_loaded"}, rows: [][]driver.Value{{"1"}}}, nil
	default:
		return &resultRows{}, nil
	}
}

type resultRows struct {
	cols []string
	rows [][]driver.Value
	next int
}

func (r *resultRows) Columns() []string { return r.cols }
func (r *resultRows) Close() error      { return nil }
func (r *resultRows) Next(dest []driver.Value) error {
	if r.next >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.next])
	r.next++
	return nil
}

var registerRecordingSnowflake = sync.OnceFunc(func() {
	stdsql.Register("recordingsnowflake", recordingSnowflake{})
})

// recordedStatements returns the statements run since the recorder was reset.
func recordedStatements() []string {
	recorded.mu.Lock()
	defer recorded.mu.Unlock()
	return append([]string(nil), recorded.statements...)
}

// stagedTable resolves a standard-updates table with an integer key and one
// string value, under the given state key.
func stagedTable(t *testing.T, stateKey string) sql.Table {
	t.Helper()
	var name = strings.Split(stateKey, ".")[0]
	table, err := sql.ResolveTable(sql.TableShape{
		Path: sql.TablePath{name},
		Keys: []sql.Projection{{Projection: pf.Projection{
			Field:        "id",
			Ptr:          "/id",
			IsPrimaryKey: true,
			Inference:    pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
		}}},
		Values: []sql.Projection{{Projection: pf.Projection{
			Field:     "value",
			Ptr:       "/value",
			Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST},
		}}},
	}, testDialect)
	require.NoError(t, err)
	table.StateKey = stateKey
	return table
}

// stagedTransactor builds a transactor for the shard covering rangeKey, with
// a standard-updates binding per state key and a recording database.
func stagedTransactor(t *testing.T, rangeKey string, stateKeys ...string) *transactor {
	t.Helper()
	registerRecordingSnowflake()
	recorded.mu.Lock()
	recorded.statements = nil
	recordedArgs = nil
	recorded.mu.Unlock()

	db, err := stdsql.Open("recordingsnowflake", "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	var d = &transactor{
		primary:      strings.HasPrefix(rangeKey, "00000000-"),
		rangeKey:     rangeKey,
		cp:           make(checkpoint),
		staged:       make(stagedCheckpoint),
		legacyStaged: make(checkpoint),
		be:           m.NewBindingEvents(),
		db:           db,
		templates:    testTemplates,
		version:      "v1",
	}
	d.store.conn = conn
	d.profiler = boilerplate.NewQueryProfiler(nil, time.Hour, nil, nil, nil)
	t.Cleanup(d.profiler.Stop)
	d.commitProfile = d.profiler.NewProfile()
	for _, sk := range stateKeys {
		d.bindings = append(d.bindings, &binding{target: stagedTable(t, sk)})
	}
	return d
}

// reduce applies a state update to a JSON state document the way the runtime
// consolidates connector state, returning the resulting document.
func reduce(t *testing.T, doc string, patch json.RawMessage) string {
	t.Helper()
	var before, after any
	require.NoError(t, json.Unmarshal([]byte(doc), &before))
	require.NoError(t, json.Unmarshal(patch, &after))
	out, err := json.Marshal(boilerplate.ApplyMergePatch(before, after))
	require.NoError(t, err)
	return string(out)
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// mergeEntry is the staged entry of a MERGE INTO over one shard's directory.
func mergeEntry(table, dir, version string, lower, upper string) *checkpointItem {
	return &checkpointItem{
		Table:     table,
		Query:     "\nMERGE INTO " + table + " USING " + dir,
		StagedDir: dir,
		Version:   version,
		Bounds:    []mergeBoundLiterals{{Lower: lower, Upper: upper}},
		Rows:      1,
	}
}

// copyEntry is the staged entry of a COPY INTO from one shard's directory.
func copyEntry(table, dir string) *checkpointItem {
	return &checkpointItem{
		Table:     table,
		Query:     "\nCOPY INTO " + table + " FROM " + dir,
		StagedDir: dir,
		Version:   "v1",
		Rows:      1,
	}
}

func TestUnmarshalStagedState(t *testing.T) {
	// A flat streaming v2 item and a flat staged item, as a connector without
	// range-keyed entries persisted them, overlaid with range-keyed entries.
	var state = reduce(t,
		string(mustJSON(t, checkpoint{
			"a_table.v1": {Table: "a_table", StreamV2: streamV2Checkpoint{
				fullKeyRange: {ChannelName: "chan", Routed: 42},
			}},
			"b_table.v1": {Table: "b_table", Query: "\nMERGE INTO b_table", StagedDir: "@flow_v1/b0"},
		})),
		mustJSON(t, map[string]map[string]*checkpointItem{
			"a_table.v1": {
				lowerRangeKey: mergeEntry("a_table", "@flow_v1/al", "v1", "1", "5"),
				upperRangeKey: mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20"),
			},
		}),
	)

	t.Run("primary recovers every shard's entries", func(t *testing.T) {
		var d = stagedTransactor(t, lowerRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(state)))

		require.Len(t, d.staged, 1)
		require.Len(t, d.staged["a_table.v1"], 2)
		require.Len(t, d.legacyStaged, 1)
		require.Equal(t, "@flow_v1/al", d.staged["a_table.v1"][lowerRangeKey].StagedDir)
		require.Equal(t, "@flow_v1/au", d.staged["a_table.v1"][upperRangeKey].StagedDir)
		require.Equal(t, "@flow_v1/b0", d.legacyStaged["b_table.v1"].StagedDir)

		require.Equal(t, int64(42), d.cp["a_table.v1"].StreamV2[fullKeyRange].Routed)
		require.Empty(t, d.cp["a_table.v1"].Query)
		require.NotContains(t, d.cp, "b_table.v1")
	})

	t.Run("non-primary recovers only its streaming state", func(t *testing.T) {
		var d = stagedTransactor(t, upperRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(state)))

		require.Empty(t, d.staged)
		require.Empty(t, d.legacyStaged)
		require.Equal(t, int64(42), d.cp["a_table.v1"].StreamV2[fullKeyRange].Routed)
		require.NotContains(t, d.cp, "b_table.v1")
	})

	t.Run("drained entries are not recovered", func(t *testing.T) {
		var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1":{}}`)))
		require.Empty(t, d.staged)
		require.Empty(t, d.cp)

		state, err := d.Acknowledge(context.Background(), nil, nil)
		require.NoError(t, err)
		require.Nil(t, state)
		require.Empty(t, recordedStatements())
	})
}

func TestUnmarshalStateRejectsUnknownFields(t *testing.T) {
	for _, state := range []string{
		`{"a_table.v1": {"Unknown": 1}}`,
		`{"a_table.v1": {"Table": "a_table", "Unknown": 1}}`,
		`{"a_table.v1": {"00000000-ffffffff": {"Unknown": 1}}}`,
		`{"a_table.v1": {"80000000-ffffffff": {"Query": "\nMERGE INTO a_table", "Unknown": 1}}}`,
		`{"00000000-ffffffff": {"a_table.v1": {"Query": "\nMERGE INTO a_table"}}}`,
	} {
		for _, rangeKey := range []string{fullRangeKey, upperRangeKey} {
			var d = stagedTransactor(t, rangeKey, "a_table.v1")
			require.Error(t, d.UnmarshalState(json.RawMessage(state)), "state %s on range %s", state, rangeKey)
		}
	}

	var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
	var _, err = d.Acknowledge(context.Background(),
		[]json.RawMessage{json.RawMessage(`{"a_table.v1": {"80000000-ffffffff": {"Unknown": 1}}}`)}, nil)
	require.Error(t, err)
}

// TestUnmarshalLegacyFlatState recovers a state document of flat items, as a
// connector without range-keyed entries persisted it.
func TestUnmarshalLegacyFlatState(t *testing.T) {
	var truncateBefore = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	var state = mustJSON(t, checkpoint{
		"a_table.v1": {Table: "a_table", Query: "\nMERGE INTO a_table", StagedDir: "@flow_v1/a0", Version: "v0", Round: 3, Rows: 2},
		"b_table.v1": {Table: "b_table", TruncateBefore: &truncateBefore, Round: 4},
		"c_table.v1": {Table: "c_table", PipeName: "pipe_c", PipeFiles: []fileRecord{{Path: "c0", Size: 10}}, Version: "v0"},
		"d_table.v1": {Table: "d_table", StreamBlobs: []*blobMetadata{{Path: "d0", MD5: "abc", BDECVersion: 3}}, EncryptionKey: "key"},
		"e_table.v1": {Table: "e_table", StreamV2: streamV2Checkpoint{fullKeyRange: {ChannelName: "chan", Routed: 7}}},
	})

	var d = stagedTransactor(t, fullRangeKey, "a_table.v1", "b_table.v1", "c_table.v1", "d_table.v1", "e_table.v1")
	require.NoError(t, d.UnmarshalState(state))

	require.Equal(t, "@flow_v1/a0", d.legacyStaged["a_table.v1"].StagedDir)
	require.Equal(t, 3, d.legacyStaged["a_table.v1"].Round)
	require.Equal(t, truncateBefore, *d.legacyStaged["b_table.v1"].TruncateBefore)
	require.Len(t, d.legacyStaged, 2)
	require.Empty(t, d.staged)

	require.Equal(t, []fileRecord{{Path: "c0", Size: 10}}, d.cp["c_table.v1"].PipeFiles)
	require.Equal(t, "d0", d.cp["d_table.v1"].StreamBlobs[0].Path)
	require.Equal(t, "key", d.cp["d_table.v1"].EncryptionKey)
	require.Equal(t, int64(7), d.cp["e_table.v1"].StreamV2[fullKeyRange].Routed)
	require.Len(t, d.cp, 3)
}

func TestStartCommitStateEmitsOwnEntries(t *testing.T) {
	var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
	d.cp["b_table.v1"] = &checkpointItem{Table: "b_table", StreamV2: streamV2Checkpoint{
		fullKeyRange: {ChannelName: "chan", Routed: 42},
	}}
	d.staged.add("a_table.v1", lowerRangeKey, mergeEntry("a_table", "@flow_v1/al", "v1", "1", "5"))
	// A peer's entry, as the primary holds after recovery.
	d.staged.add("a_table.v1", upperRangeKey, mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20"))

	patch, err := d.startCommitState()
	require.NoError(t, err)

	var expect = reduce(t,
		string(mustJSON(t, checkpoint{"b_table.v1": d.cp["b_table.v1"]})),
		mustJSON(t, map[string]map[string]*checkpointItem{
			"a_table.v1": {lowerRangeKey: d.staged["a_table.v1"][lowerRangeKey]},
		}),
	)
	require.JSONEq(t, reduce(t, `{}`, json.RawMessage(expect)), reduce(t, `{}`, patch))
}

func TestAcknowledgeCoalescesShardMerges(t *testing.T) {
	var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
	var own = mergeEntry("a_table", "@flow_v1/al", "v1", "1", "5")
	var peer = mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20")
	d.staged.add("a_table.v1", lowerRangeKey, own)

	var peerPatch = mustJSON(t, map[string]map[string]*checkpointItem{
		"a_table.v1": {lowerRangeKey: own, upperRangeKey: peer},
	})
	var persisted = string(peerPatch)

	update, err := d.Acknowledge(context.Background(), []json.RawMessage{peerPatch}, nil)
	require.NoError(t, err)
	require.NotNil(t, update)

	var statements = recordedStatements()
	var merges []string
	var removes []string
	for _, s := range statements {
		if strings.HasPrefix(s, "\nMERGE INTO") {
			merges = append(merges, s)
		} else if strings.HasPrefix(s, "REMOVE ") {
			removes = append(removes, s)
		}
	}
	require.Len(t, merges, 1, "one MERGE covers every shard's files: %v", statements)
	require.Contains(t, merges[0], "FROM @flow_v1 (PATTERN => '.*(al|au)/.*')")
	require.Contains(t, merges[0], "LEAST(")
	require.Contains(t, merges[0], "GREATEST(")
	require.ElementsMatch(t, []string{"REMOVE @flow_v1/al", "REMOVE @flow_v1/au"}, removes)

	require.JSONEq(t, `{"a_table.v1":{}}`, reduce(t, persisted, update.UpdatedJson))
	require.Empty(t, d.staged)
}

func TestAcknowledgeRunsUncoalescibleEntriesIndividually(t *testing.T) {
	for _, tc := range []struct {
		name      string
		own, peer *checkpointItem
	}{
		{
			// The peer's entry was staged under an earlier spec, whose columns
			// may not match the binding's.
			name: "spec versions differ",
			own:  mergeEntry("a_table", "@flow_v1/al", "v1", "1", "5"),
			peer: mergeEntry("a_table", "@flow_v1/au", "v0", "10", "20"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
			d.staged.add("a_table.v1", lowerRangeKey, tc.own)
			var patch = mustJSON(t, map[string]map[string]*checkpointItem{
				"a_table.v1": {lowerRangeKey: tc.own, upperRangeKey: tc.peer},
			})

			update, err := d.Acknowledge(context.Background(), []json.RawMessage{patch}, nil)
			require.NoError(t, err)

			var queries []string
			for _, s := range recordedStatements() {
				if !strings.HasPrefix(s, "REMOVE ") {
					queries = append(queries, s)
				}
			}
			require.Equal(t, []string{tc.own.Query, tc.peer.Query}, queries)
			require.JSONEq(t, `{"a_table.v1":{}}`, reduce(t, string(patch), update.UpdatedJson))
		})
	}
}

func TestAcknowledgeCoalescesShardCopies(t *testing.T) {
	var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
	var own = copyEntry("a_table", "@flow_v1/al")
	var peer = mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20")
	peer.CopyQuery, peer.CopyStagedDir = "\nCOPY INTO a_table FROM @flow_v1/auc", "@flow_v1/auc"
	d.staged.add("a_table.v1", lowerRangeKey, own)
	var patch = mustJSON(t, map[string]map[string]*checkpointItem{
		"a_table.v1": {lowerRangeKey: own, upperRangeKey: peer},
	})

	update, err := d.Acknowledge(context.Background(), []json.RawMessage{patch}, nil)
	require.NoError(t, err)

	var queries []string
	for _, s := range recordedStatements() {
		if !strings.HasPrefix(s, "REMOVE ") {
			queries = append(queries, s)
		}
	}
	require.Len(t, queries, 2, "the peer's MERGE, then one COPY of every shard's new rows")
	require.Equal(t, peer.Query, queries[0])
	require.True(t, strings.HasPrefix(queries[1], "\nCOPY INTO a_table"), queries[1])
	require.Contains(t, queries[1], "FROM @flow_v1\n) PATTERN = '.*(al|auc)/.*';")
	require.JSONEq(t, `{"a_table.v1":{}}`, reduce(t, string(patch), update.UpdatedJson))
}

func TestAcknowledgeNonPrimaryExecutesNothing(t *testing.T) {
	var d = stagedTransactor(t, upperRangeKey, "a_table.v1")
	var own = mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20")
	d.staged.add("a_table.v1", upperRangeKey, own)
	var patch = mustJSON(t, map[string]map[string]*checkpointItem{
		"a_table.v1": {lowerRangeKey: mergeEntry("a_table", "@flow_v1/al", "v1", "1", "5"), upperRangeKey: own},
	})

	update, err := d.Acknowledge(context.Background(), []json.RawMessage{patch}, nil)
	require.NoError(t, err)
	require.Nil(t, update)
	require.Empty(t, recordedStatements())
	require.Empty(t, d.staged)
}

// A flat entry staged before entries were keyed by shard range is cleared by
// nulling its fields, so a range-keyed entry merged onto the same state key
// survives the clearing.
func TestAcknowledgeClearsLegacyEntryByField(t *testing.T) {
	var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
	var persisted = string(mustJSON(t, checkpoint{
		"a_table.v1": {Table: "a_table", Query: "\nMERGE INTO a_table", StagedDir: "@flow_v1/a0", Version: "v0", Round: 3, Rows: 2},
	}))
	require.NoError(t, d.UnmarshalState(json.RawMessage(persisted)))

	update, err := d.Acknowledge(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"\nMERGE INTO a_table", "REMOVE @flow_v1/a0"}, recordedStatements())

	var peer = mustJSON(t, map[string]map[string]*checkpointItem{
		"a_table.v1": {upperRangeKey: mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20")},
	})
	var after = reduce(t, reduce(t, persisted, peer), update.UpdatedJson)
	require.JSONEq(t, reduce(t, `{}`, peer), after)
}

// Clearing a flat entry's staged work keeps the streaming v2 channel state
// that shares its state key.
func TestAcknowledgeClearsLegacyEntryKeepingStreamV2(t *testing.T) {
	var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
	var persisted = string(mustJSON(t, checkpoint{
		"a_table.v1": {Table: "a_table", Query: "\nMERGE INTO a_table", StagedDir: "@flow_v1/a0", Version: "v0",
			StreamV2: streamV2Checkpoint{fullKeyRange: {ChannelName: "chan", Routed: 42}}},
	}))
	require.NoError(t, d.UnmarshalState(json.RawMessage(persisted)))

	update, err := d.Acknowledge(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"\nMERGE INTO a_table", "REMOVE @flow_v1/a0"}, recordedStatements())

	var streamV2 = mustJSON(t, streamV2Checkpoint{fullKeyRange: {ChannelName: "chan", Routed: 42}})
	require.JSONEq(t, `{"a_table.v1":{"StreamV2":`+string(streamV2)+`}}`, reduce(t, persisted, update.UpdatedJson))
}

func TestAcknowledgeLeavesEntriesOfRemovedBindings(t *testing.T) {
	var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
	d.staged.add("gone_table.v1", fullRangeKey, mergeEntry("gone_table", "@flow_v1/g", "v1", "1", "5"))

	update, err := d.Acknowledge(context.Background(), nil, nil)
	require.NoError(t, err)
	require.Nil(t, update)
	require.Empty(t, recordedStatements())
	require.NotNil(t, d.staged["gone_table.v1"][fullRangeKey])
}

// Every shard records a completed backfill's boundary, and the primary deletes
// the rows published before it once, after every shard's stores.
func TestAcknowledgeTruncatesOnceAfterEveryShardsStores(t *testing.T) {
	var early = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	var late = early.Add(time.Second)

	var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
	d.bindings[0].truncateSQL = "DELETE FROM a_table WHERE flow_published_at < ?;"
	var own = copyEntry("a_table", "@flow_v1/al")
	own.TruncateBefore = &early
	var peer = &checkpointItem{Table: "a_table", TruncateBefore: &late}
	d.staged.add("a_table.v1", lowerRangeKey, own)
	var patch = mustJSON(t, map[string]map[string]*checkpointItem{
		"a_table.v1": {lowerRangeKey: own, upperRangeKey: peer},
	})

	update, err := d.Acknowledge(context.Background(), []json.RawMessage{patch}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{own.Query, d.bindings[0].truncateSQL, "REMOVE @flow_v1/al"}, recordedStatements())
	require.Equal(t, []any{late.UTC().Format(time.RFC3339Nano)}, recordedArgs[1])
	require.JSONEq(t, `{"a_table.v1":{}}`, reduce(t, string(patch), update.UpdatedJson))
}

// A flat item that shares its state key with no range-keyed entry is emitted
// exactly as it marshals, so its field order is stable across commits.
func TestStartCommitStateEmitsFlatItemsVerbatim(t *testing.T) {
	var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
	var item = &checkpointItem{
		StreamBlobs:   []*blobMetadata{{Path: "blob"}},
		EncryptionKey: "key",
		Round:         1,
		Rows:          2,
	}
	d.cp["a_table.v1"] = item

	patch, err := d.startCommitState()
	require.NoError(t, err)
	require.Equal(t, `{"a_table.v1":`+string(mustJSON(t, item))+`}`, string(patch))
}

func TestStartCommitStateClearsSweptStreamV2Item(t *testing.T) {
	var d = stagedTransactor(t, fullRangeKey, "a_table.v1")
	var state = string(mustJSON(t, map[string]*checkpointItem{"a_table.v1": {
		StreamV2: streamV2Checkpoint{{keyBegin: 0, keyEnd: math.MaxUint32}: {ChannelName: "channel"}},
	}}))

	// Open absorbs the state twice, and the binding's channels are swept between.
	require.NoError(t, d.UnmarshalState(json.RawMessage(state)))
	d.forgetStreamV2("a_table.v1")
	require.NoError(t, d.UnmarshalState(json.RawMessage(state)))

	patch, err := d.startCommitState()
	require.NoError(t, err)
	require.Equal(t, `{"a_table.v1":{}}`, reduce(t, state, patch))
}

func TestTwoShardsApplyEveryEntryOnce(t *testing.T) {
	const sk = "a_table.v1"
	var ctx = context.Background()
	var primary = stagedTransactor(t, lowerRangeKey, sk)
	var peer = stagedTransactor(t, upperRangeKey, sk)
	primary.bindings[0].truncateSQL = "DELETE FROM a_table WHERE flow_published_at < ?;"
	var boundary = time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)

	var state = "{}"
	for txn := range 3 {
		var lowerDir, upperDir = fmt.Sprintf("@flow_v1/l%d", txn), fmt.Sprintf("@flow_v1/u%d", txn)
		var own = mergeEntry("a_table", lowerDir, "v1", "1", "5")
		var theirs = mergeEntry("a_table", upperDir, "v1", "10", "20")
		if txn == 1 {
			own.TruncateBefore, theirs.TruncateBefore = &boundary, &boundary
		}
		primary.staged.add(sk, lowerRangeKey, own)
		peer.staged.add(sk, upperRangeKey, theirs)

		primaryPatch, err := primary.startCommitState()
		require.NoError(t, err)
		peerPatch, err := peer.startCommitState()
		require.NoError(t, err)
		state = reduce(t, reduce(t, state, primaryPatch), peerPatch)

		var before = len(recordedStatements())
		var patches = []json.RawMessage{primaryPatch, peerPatch}
		primaryUpdate, err := primary.Acknowledge(ctx, patches, nil)
		require.NoError(t, err)
		peerUpdate, err := peer.Acknowledge(ctx, patches, nil)
		require.NoError(t, err)
		require.Nil(t, peerUpdate)

		var merges, truncations, removes []string
		for _, s := range recordedStatements()[before:] {
			switch {
			case strings.HasPrefix(s, "\nMERGE INTO"):
				merges = append(merges, s)
			case s == primary.bindings[0].truncateSQL:
				truncations = append(truncations, s)
			case strings.HasPrefix(s, "REMOVE "):
				removes = append(removes, s)
			default:
				t.Fatalf("transaction %d ran an unexpected statement: %q", txn, s)
			}
		}
		require.Len(t, merges, 1, "transaction %d", txn)
		require.Contains(t, merges[0], "PATTERN => '"+stagedDirsPattern([]string{lowerDir, upperDir})+"'")
		require.Len(t, truncations, map[bool]int{true: 1}[txn == 1], "transaction %d", txn)
		require.ElementsMatch(t, []string{"REMOVE " + lowerDir, "REMOVE " + upperDir}, removes)

		state = reduce(t, state, primaryUpdate.UpdatedJson)
		require.JSONEq(t, `{"a_table.v1":{}}`, state, "transaction %d", txn)
		require.Empty(t, primary.staged)
		require.Empty(t, peer.staged)
	}
}

func TestAcknowledgeRunsLegacyEntryBeforeRangeEntries(t *testing.T) {
	var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
	var legacy = &checkpointItem{Table: "a_table", Query: "\nMERGE INTO a_table USING @flow_v1/a0", StagedDir: "@flow_v1/a0", Version: "v0"}
	var fields = make(map[string]any)
	require.NoError(t, json.Unmarshal(mustJSON(t, legacy), &fields))
	fields[lowerRangeKey] = mergeEntry("a_table", "@flow_v1/al", "v1", "1", "5")
	fields[upperRangeKey] = mergeEntry("a_table", "@flow_v1/au", "v1", "10", "20")
	var persisted = string(mustJSON(t, map[string]any{"a_table.v1": fields}))
	require.NoError(t, d.UnmarshalState(json.RawMessage(persisted)))

	update, err := d.Acknowledge(context.Background(), nil, nil)
	require.NoError(t, err)

	var statements = recordedStatements()
	require.GreaterOrEqual(t, len(statements), 2)
	require.Equal(t, legacy.Query, statements[0])
	require.True(t, strings.HasPrefix(statements[1], "\nMERGE INTO"), statements[1])
	require.Contains(t, statements[1], "FROM @flow_v1 (PATTERN => '.*(al|au)/.*')")
	require.JSONEq(t, `{"a_table.v1":{}}`, reduce(t, persisted, update.UpdatedJson))
}

func TestCombineBounds(t *testing.T) {
	var keys = stagedTable(t, "a_table.v1").Keys
	var bounded = func(lower, upper string) *checkpointItem {
		return &checkpointItem{Bounds: []mergeBoundLiterals{{Lower: lower, Upper: upper}}}
	}

	var got = combineBounds(keys, []*checkpointItem{bounded("1", "5"), bounded("10", "20"), bounded("1", "20")})
	require.Equal(t, "LEAST(1::INTEGER, 10::INTEGER)", got[0].LiteralLower)
	require.Equal(t, "GREATEST(5::INTEGER, 20::INTEGER)", got[0].LiteralUpper)

	got = combineBounds(keys, []*checkpointItem{bounded("1", "5"), bounded("1", "20")})
	require.Equal(t, "1", got[0].LiteralLower)
	require.Equal(t, "GREATEST(5::INTEGER, 20::INTEGER)", got[0].LiteralUpper)

	got = combineBounds(keys, []*checkpointItem{bounded("1", "5"), {Bounds: []mergeBoundLiterals{{}}}})
	require.Empty(t, got[0].LiteralLower)
	require.Empty(t, got[0].LiteralUpper)
}

func TestAcknowledgeRejectsResetPatch(t *testing.T) {
	var d = stagedTransactor(t, lowerRangeKey, "a_table.v1")
	_, err := d.Acknowledge(context.Background(), []json.RawMessage{json.RawMessage("null")}, nil)
	require.ErrorContains(t, err, "unexpected state reset patch")
	require.Empty(t, recordedStatements())
}

func TestAcknowledgeNonPrimaryIgnoresPeerPatches(t *testing.T) {
	var d = stagedTransactor(t, upperRangeKey, "a_table.v1")
	update, err := d.Acknowledge(context.Background(), []json.RawMessage{json.RawMessage("null")}, nil)
	require.NoError(t, err)
	require.Nil(t, update)
}

func TestCombineBoundsOfStagedKeys(t *testing.T) {
	table, err := sql.ResolveTable(sql.TableShape{
		Path: sql.TablePath{"a_table"},
		Keys: []sql.Projection{
			{Projection: pf.Projection{Field: "name", Ptr: "/name", IsPrimaryKey: true,
				Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MUST}}},
			{Projection: pf.Projection{Field: "ts", Ptr: "/ts", IsPrimaryKey: true,
				Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}, Exists: pf.Inference_MUST}}},
		},
	}, testDialect)
	require.NoError(t, err)

	// staged is the entry of a shard that stored the given keys.
	var staged = func(keys ...[]any) *checkpointItem {
		var builder = sql.NewMergeBoundsBuilder(table.Keys, testDialect.Literal)
		for _, key := range keys {
			var converted []any
			for i, v := range key {
				c, err := table.Keys[i].Converter(v)
				require.NoError(t, err)
				converted = append(converted, c)
			}
			builder.NextKey(converted)
		}
		return &checkpointItem{Bounds: boundsLiterals(builder.Build())}
	}

	var got = combineBounds(table.Keys, []*checkpointItem{
		staged([]any{"a", "2026-01-01T00:00:00Z"}, []any{"c", "2026-01-02T00:00:00Z"}),
		staged([]any{"m", "2026-02-01T00:00:00Z"}, []any{"z", "2026-02-02T00:00:00Z"}),
	})
	require.Equal(t, []string{"LEAST('a'::TEXT, 'm'::TEXT)", "GREATEST('c'::TEXT, 'z'::TEXT)"},
		[]string{got[0].LiteralLower, got[0].LiteralUpper})
	require.Equal(t, []string{"LEAST('2025-12-31'::TEXT, '2026-01-31'::TEXT)", "GREATEST('2026-01-04'::TEXT, '2026-02-04'::TEXT)"},
		[]string{got[1].LiteralLower, got[1].LiteralUpper})
}
