package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

const (
	fullRangeKey  = "00000000-ffffffff"
	lowerRangeKey = "00000000-7fffffff"
	upperRangeKey = "80000000-ffffffff"
)

func testTransactor(scaleOut bool, rangeKey string, stateKeys ...string) *transactor {
	var d = &transactor{
		cp:                    make(checkpoint),
		peerShardsCheckpoints: make(rangeCheckpoints),
		scaleOut:              scaleOut,
		primary:               rangeKey == fullRangeKey || rangeKey == lowerRangeKey,
		rangeKey:              rangeKey,
		be:                    &m.BindingEvents{},
	}
	for _, sk := range stateKeys {
		d.bindings = append(d.bindings, &binding{target: sql.Table{
			TableShape: sql.TableShape{Path: sql.TablePath{"schema", sk}},
			StateKey:   sk,
		}})
	}
	return d
}

func item(query string, toDelete ...string) *checkpointItem {
	return &checkpointItem{Queries: []string{query}, ToDelete: toDelete}
}

// keys builds the non-nil restricted-processing predicate of Acknowledge's
// stateKeys contract; allKeys is the nil process-everything form.
func keys(stateKeys ...string) func(string) bool {
	return m.StateKeyFilter(stateKeys)
}

var allKeys = m.StateKeyFilter(nil)

// recordingDriver is a database/sql driver whose connections record every
// executed statement (or fail with a configured error), so Acknowledge tests
// run against a real *sql.DB without a Databricks connection.
type recordingDriver struct{}

type recordingConn struct{}

var recording struct {
	executed []string
	failWith error
}

func (recordingDriver) Open(string) (driver.Conn, error) { return recordingConn{}, nil }

func (recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not implemented")
}
func (recordingConn) Close() error              { return nil }
func (recordingConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("begin is not implemented") }

func (recordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	if recording.failWith != nil {
		return nil, recording.failWith
	}
	recording.executed = append(recording.executed, query)
	return driver.RowsAffected(0), nil
}

var registerRecordingDriver = sync.OnceFunc(func() {
	stdsql.Register("recording", recordingDriver{})
})

// recordingDB resets the recorder and returns a *sql.DB whose statements it
// captures. A non-nil failWith fails every statement.
func recordingDB(t *testing.T, failWith error) *stdsql.DB {
	t.Helper()
	registerRecordingDriver()

	recording.executed, recording.failWith = nil, failWith
	db, err := stdsql.Open("recording", "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestUnmarshalStateRouting(t *testing.T) {
	var legacyState = json.RawMessage(`{
		"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]},
		"b_table.v1": {"Queries": ["Q2"], "ToDelete": []}
	}`)
	var mixedState = json.RawMessage(`{
		"00000000-7fffffff": {"a_table.v1": {"Queries": ["QL"], "ToDelete": []}},
		"80000000-ffffffff": {"a_table.v1": {"Queries": ["QU"], "ToDelete": []}},
		"a_table.v1": {"Queries": ["Q0"], "ToDelete": []}
	}`)

	t.Run("flag off legacy document", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, d.UnmarshalState(legacyState))
		require.True(t, d.cpRecovery)
		require.Len(t, d.cp, 2)
		require.Equal(t, []string{"Q1"}, d.cp["a_table.v1"].Queries)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("flag off routes range buckets after downgrade", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.True(t, d.cpRecovery)
		require.Equal(t, []string{"Q0"}, d.cp["a_table.v1"].Queries)
		require.Len(t, d.peerShardsCheckpoints, 2)
		require.Equal(t, []string{"QL"}, d.peerShardsCheckpoints[lowerRangeKey]["a_table.v1"].Queries)
	})

	t.Run("flag on primary routes own range to cp", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.True(t, d.cpRecovery)
		require.Equal(t, []string{"QL"}, d.cp["a_table.v1"].Queries)
		require.Equal(t, []string{"QU"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
		require.Equal(t, []string{"Q0"}, d.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Queries)
	})

	t.Run("flag on non-primary discards everything", func(t *testing.T) {
		var d = testTransactor(true, upperRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.False(t, d.cpRecovery)
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("unknown fields error", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1")
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {"Unknown": 1}}`)))
	})
}

func TestStartCommitState(t *testing.T) {
	t.Run("flag off is a full state replacement", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("Q1", "f1")

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.False(t, state.MergePatch)
		require.JSONEq(t, `{"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]}}`, string(state.UpdatedJson))
	})

	t.Run("flag on is a range-scoped merge patch", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("Q1", "f1")

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{"00000000-7fffffff": {"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]}}}`, string(state.UpdatedJson))
	})
}

func TestMergePeerStatePatches(t *testing.T) {
	var patches = func(ps ...string) []json.RawMessage {
		var out []json.RawMessage
		for _, p := range ps {
			out = append(out, json.RawMessage(p))
		}
		return out
	}

	t.Run("no-op when flag off or non-primary", func(t *testing.T) {
		for _, d := range []*transactor{
			testTransactor(false, fullRangeKey),
			testTransactor(true, upperRangeKey),
		} {
			require.NoError(t, d.mergePeerStatePatches(patches(`{"80000000-ffffffff": {"a_table.v1": {"Queries": ["Q"], "ToDelete": []}}}`)))
			require.Empty(t, d.peerShardsCheckpoints)
		}
	})

	t.Run("own contribution is skipped, peers merged", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(patches(
			`{"00000000-7fffffff": {"a_table.v1": {"Queries": ["OWN"], "ToDelete": []}}}`,
			`{"80000000-ffffffff": {"a_table.v1": {"Queries": ["PEER"], "ToDelete": ["pf1"]}}}`,
		)))
		require.Empty(t, d.cp) // own patch is not folded back
		require.Len(t, d.peerShardsCheckpoints, 1)
		require.Equal(t, []string{"PEER"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
	})

	t.Run("state reset patch errors", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.Error(t, d.mergePeerStatePatches(patches(`null`, `{"a": 1}`)))
	})

	t.Run("empty patches are a no-op", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(nil))
	})
}

func TestAcknowledge(t *testing.T) {
	t.Run("flag off clearing patch matches legacy shape", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp["a_table.v1"] = item("Q1")

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"Q1"}, recording.executed)
		require.True(t, state.MergePatch)
		require.Equal(t, `{"a_table.v1":null}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
	})

	t.Run("flag on primary executes own and peer entries", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("OWN")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{"a_table.v1": item("PEER")}
		d.peerShardsCheckpoints[legacyRangeKey] = checkpoint{"a_table.v1": item("LEGACY")}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"OWN", "LEGACY", "PEER"}, recording.executed)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"80000000-ffffffff": {"a_table.v1": null},
			"a_table.v1": null
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("removed binding entries are retained and not cleared", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1":       item("PEER"),
			"removed_table.v1": item("REMOVED"),
		}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"PEER"}, recording.executed)
		require.JSONEq(t, `{
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.peerShardsCheckpoints[upperRangeKey]["removed_table.v1"])
	})

	t.Run("subset drain executes only requested state keys", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp["a_table.v1"] = item("QA")
		d.cp["b_table.v1"] = item("QB")

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), keys("a_table.v1"))
		require.NoError(t, err)
		require.Equal(t, []string{"QA"}, recording.executed)
		require.Equal(t, `{"a_table.v1":null}`, string(state.UpdatedJson))
		require.NotNil(t, d.cp["b_table.v1"]) // untouched, still pending
	})

	t.Run("subset drain spans all range buckets", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1", "b_table.v1")
		d.cp["a_table.v1"] = item("OWN-A")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": item("PEER-A"),
			"b_table.v1": item("PEER-B"),
		}
		d.peerShardsCheckpoints[legacyRangeKey] = checkpoint{"a_table.v1": item("LEGACY-A")}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), keys("a_table.v1"))
		require.NoError(t, err)
		require.Equal(t, []string{"OWN-A", "LEGACY-A", "PEER-A"}, recording.executed)
		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"a_table.v1": null,
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.peerShardsCheckpoints[upperRangeKey]["b_table.v1"])
	})

	t.Run("nothing to drain returns nil state", func(t *testing.T) {
		var d = testTransactor(false, fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp["b_table.v1"] = item("QB")

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), keys("a_table.v1"))
		require.NoError(t, err)
		require.Nil(t, state)
		require.NotNil(t, d.cp["b_table.v1"]) // untouched, still pending
	})

	t.Run("flag on non-primary does no work", func(t *testing.T) {
		var d = testTransactor(true, upperRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("SHOULD NOT RUN")
		d.cpRecovery = true

		// Acknowledge itself is callable here since the non-primary path
		// never opens a database connection.
		state, err := d.Acknowledge(context.Background(), nil, nil)
		require.NoError(t, err)
		require.Nil(t, state)
		require.Empty(t, d.cp)
		require.False(t, d.cpRecovery)
	})

	t.Run("recovery tolerates deleted paths", func(t *testing.T) {
		var d = testTransactor(true, lowerRangeKey, "a_table.v1")
		d.cp["a_table.v1"] = item("Q1")
		d.cpRecovery = true

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.NoError(t, err)
		require.False(t, d.cpRecovery)

		d.cp["a_table.v1"] = item("Q1")
		_, err = d.acknowledgeApply(context.Background(), recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.Error(t, err) // no longer a recovery apply
	})
}

// renderableTable builds a table realistic enough for the commit query
// templates to render: quoted identifiers and mapped types on every column.
func renderableTable() sql.Table {
	var col = func(identifier, ddl, bareDDL string) sql.Column {
		return sql.Column{
			Identifier: identifier,
			MappedType: sql.MappedType{DDL: ddl, BareDDL: bareDDL},
		}
	}
	var doc = col("flow_document", "STRING", "STRING")

	return sql.Table{
		TableShape: sql.TableShape{Path: sql.TablePath{"schema", "a_table"}},
		Identifier: "`schema`.`a_table`",
		Keys: []sql.Column{
			col("id", "LONG NOT NULL", "LONG"),
			col("ts", "TIMESTAMP NOT NULL", "TIMESTAMP"),
		},
		Values:   []sql.Column{col("val", "STRING", "STRING")},
		Document: &doc,
		StateKey: "a_table.v1",
	}
}

// renderingTransactor is a primary-shard transactor whose single binding can
// render coalesced commit queries.
func renderingTransactor(scaleOut bool, rangeKey string) *transactor {
	var d = testTransactor(scaleOut, rangeKey)
	d.templates = testTemplates
	d.bindings = append(d.bindings, &binding{
		target:          renderableTable(),
		rootStagingPath: "/Volumes/cat/schema/flow_staging/flow_temp_tables",
	})
	return d
}

func bound(lower, upper string) mergeBoundLiterals {
	return mergeBoundLiterals{Lower: lower, Upper: upper}
}

func structuredItem(needsMerge bool, bounds []mergeBoundLiterals, files ...string) *checkpointItem {
	return &checkpointItem{
		ToDelete:    nil, // deleteFiles requires a workspace client
		StagedFiles: files,
		Bounds:      bounds,
		NeedsMerge:  needsMerge,
	}
}

func TestAcknowledgeCoalescesShardEntries(t *testing.T) {
	t.Run("entries of all shards coalesce into a single merge", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(true,
			[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
			"own.json")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": structuredItem(false,
				[]mergeBoundLiterals{bound("5", "50"), bound("'2024-01-01T12:00:00Z'", "'2024-01-03T00:00:00Z'")},
				"peer.json"),
		}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)

		require.Len(t, recording.executed, 1)
		var query = recording.executed[0]
		require.Contains(t, query, "MERGE INTO `schema`.`a_table`")
		require.Contains(t, query, "/Volumes/cat/schema/flow_staging/flow_temp_tables/own.json")
		require.Contains(t, query, "/Volumes/cat/schema/flow_staging/flow_temp_tables/peer.json")
		require.Contains(t, query, "l.id >= LEAST(1::LONG, 5::LONG)")
		require.Contains(t, query, "l.id <= GREATEST(10::LONG, 50::LONG)")
		require.Contains(t, query, "l.ts >= LEAST('2024-01-01T00:00:00Z'::TIMESTAMP, '2024-01-01T12:00:00Z'::TIMESTAMP)")
		require.Contains(t, query, "l.ts <= GREATEST('2024-01-02T00:00:00Z'::TIMESTAMP, '2024-01-03T00:00:00Z'::TIMESTAMP)")

		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("one merging entry is enough to merge the coalesced files", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(false, nil, "own.json")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": structuredItem(true, nil, "peer.json"),
		}

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "MERGE INTO `schema`.`a_table`")
	})

	t.Run("entries with no merging coalesce into a single copy", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(false, nil, "own.json")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": structuredItem(false, nil, "peer.json"),
		}

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		var query = recording.executed[0]
		require.Contains(t, query, "COPY INTO `schema`.`a_table`")
		require.Contains(t, query, "FILES = ('own.json','peer.json')")
	})

	t.Run("recovery coalesces exactly like steady state", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(true, nil, "own.json")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": structuredItem(true, nil, "peer.json"),
		}
		d.cpRecovery = true

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "MERGE INTO `schema`.`a_table`")
		require.Contains(t, recording.executed[0], "own.json")
		require.Contains(t, recording.executed[0], "peer.json")
		require.False(t, d.cpRecovery)
	})

	t.Run("recovery tolerates an already-applied group whose files are gone", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(true, nil, "own.json")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": structuredItem(true, nil, "peer.json"),
		}
		d.cpRecovery = true

		// The coalesced query fails on deleted staged files, which during
		// recovery means the whole group was applied by a previous session
		// whose state clearing didn't commit: it is skipped and cleared.
		state, err := d.acknowledgeApply(context.Background(),
			recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.NoError(t, err)
		require.Empty(t, recording.executed)
		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("non-recovery missing objects are fatal", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(true, nil, "own.json")

		_, err := d.acknowledgeApply(context.Background(),
			recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.Error(t, err)
	})

	t.Run("entries of older versions run pre-rendered alongside the coalesced query", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		d.cp["a_table.v1"] = structuredItem(true, nil, "own.json")
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{
			"a_table.v1": item("OLD-PEER"),
		}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 2)
		require.Equal(t, "OLD-PEER", recording.executed[0])
		require.Contains(t, recording.executed[1], "MERGE INTO `schema`.`a_table`")
		require.JSONEq(t, `{
			"00000000-7fffffff": {"a_table.v1": null},
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
	})

	t.Run("a single entry renders its bounds verbatim", func(t *testing.T) {
		var d = renderingTransactor(false, fullRangeKey)
		d.cp["a_table.v1"] = structuredItem(true,
			[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
			"own.json")

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "l.id >= 1 AND l.id <= 10")
		require.Contains(t, recording.executed[0], "l.ts >= '2024-01-01T00:00:00Z' AND l.ts <= '2024-01-02T00:00:00Z'")
	})

	t.Run("coalesced files chunk into batched queries", func(t *testing.T) {
		var d = renderingTransactor(true, lowerRangeKey)
		var files []string
		for i := 0; i < queryBatchSize+1; i++ {
			files = append(files, fmt.Sprintf("f%d.json", i))
		}
		d.cp["a_table.v1"] = structuredItem(true, nil, files...)

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 2)
	})
}

func TestCombineBounds(t *testing.T) {
	var keys = renderableTable().Keys

	t.Run("identical bounds pass through", func(t *testing.T) {
		var items = []*checkpointItem{
			{Bounds: []mergeBoundLiterals{bound("1", "10"), bound("'a'", "'b'")}},
			{Bounds: []mergeBoundLiterals{bound("1", "10"), bound("'a'", "'b'")}},
		}
		var out = combineBounds(keys, items)
		require.Equal(t, "1", out[0].LiteralLower)
		require.Equal(t, "10", out[0].LiteralUpper)
		require.Equal(t, "'a'", out[1].LiteralLower)
	})

	t.Run("differing bounds combine with casts", func(t *testing.T) {
		var items = []*checkpointItem{
			{Bounds: []mergeBoundLiterals{bound("1", "10"), bound("'a'", "'b'")}},
			{Bounds: []mergeBoundLiterals{bound("5", "50"), bound("'a'", "'c'")}},
		}
		var out = combineBounds(keys, items)
		require.Equal(t, "LEAST(1::LONG, 5::LONG)", out[0].LiteralLower)
		require.Equal(t, "GREATEST(10::LONG, 50::LONG)", out[0].LiteralUpper)
		require.Equal(t, "'a'", out[1].LiteralLower)
		require.Equal(t, "GREATEST('b'::TIMESTAMP, 'c'::TIMESTAMP)", out[1].LiteralUpper)
	})

	t.Run("a missing bound drops that column's bound", func(t *testing.T) {
		var items = []*checkpointItem{
			{Bounds: []mergeBoundLiterals{bound("1", "10"), bound("", "")}},
			{Bounds: []mergeBoundLiterals{bound("5", "50"), bound("'a'", "'b'")}},
		}
		var out = combineBounds(keys, items)
		require.Equal(t, "LEAST(1::LONG, 5::LONG)", out[0].LiteralLower)
		require.Empty(t, out[1].LiteralLower)
		require.Empty(t, out[1].LiteralUpper)
	})

	t.Run("a length mismatch drops all bounds of that entry's columns", func(t *testing.T) {
		var items = []*checkpointItem{
			{Bounds: []mergeBoundLiterals{bound("1", "10"), bound("'a'", "'b'")}},
			{Bounds: nil},
		}
		var out = combineBounds(keys, items)
		require.Empty(t, out[0].LiteralLower)
		require.Empty(t, out[1].LiteralLower)
		require.Equal(t, keys[0].Identifier, out[0].Identifier)
	})
}

func TestValidateShardRange(t *testing.T) {
	// A shard covering the full keyspace is a single-shard task and is fine in
	// either mode; a partial-range shard requires scale_out.
	require.NoError(t, validateShardRange(false, 0, 0xffffffff))
	require.NoError(t, validateShardRange(true, 0, 0xffffffff))
	require.NoError(t, validateShardRange(true, 0, 0x7fffffff))
	require.NoError(t, validateShardRange(true, 0x80000000, 0xffffffff))

	require.ErrorContains(t, validateShardRange(false, 0, 0x7fffffff), "scale_out")
	require.ErrorContains(t, validateShardRange(false, 0x80000000, 0xffffffff), "80000000-ffffffff")
}
