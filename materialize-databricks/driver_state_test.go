package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
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

func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var d = &transactor{
		cp:                    make(connectorState),
		peerShardsCheckpoints: make(rangeCheckpoints),
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

// reduce applies a state update to a JSON state document the way the runtime
// consolidates connector state, returning the resulting document.
func reduce(t *testing.T, doc string, update *pf.ConnectorState) string {
	t.Helper()
	var before, patch any
	require.NoError(t, json.Unmarshal([]byte(doc), &before))
	require.NoError(t, json.Unmarshal(update.UpdatedJson, &patch))
	if !update.MergePatch {
		before = nil
	}
	var out, err = json.Marshal(boilerplate.ApplyMergePatch(before, patch))
	require.NoError(t, err)
	return string(out)
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
	// failFirst limits failWith to the first failFirst statements when
	// non-zero; otherwise failWith fails every statement.
	failFirst int
	attempts  int
}

func (recordingDriver) Open(string) (driver.Conn, error) { return recordingConn{}, nil }

func (recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not implemented")
}
func (recordingConn) Close() error              { return nil }
func (recordingConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("begin is not implemented") }

func (recordingConn) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	recording.attempts++
	if recording.failWith != nil && (recording.failFirst == 0 || recording.attempts <= recording.failFirst) {
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

	recording.executed, recording.failWith, recording.failFirst, recording.attempts = nil, failWith, 0, 0
	db, err := stdsql.Open("recording", "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestUnmarshalStateRouting(t *testing.T) {
	var flatState = json.RawMessage(`{
		"a_table.v1": {"Queries": ["Q1"], "ToDelete": ["f1"]},
		"b_table.v1": {"Queries": ["Q2"], "ToDelete": []}
	}`)
	var rangeFirstState = json.RawMessage(`{
		"00000000-7fffffff": {"a_table.v1": {"Queries": ["QL"], "ToDelete": []}},
		"80000000-ffffffff": {"a_table.v1": {"Queries": ["QU"], "ToDelete": []}}
	}`)
	var currentState = json.RawMessage(`{
		"a_table.v1": {
			"00000000-7fffffff": {"Queries": ["QL"], "ToDelete": []},
			"80000000-ffffffff": {"Queries": ["QU"], "ToDelete": []}
		}
	}`)
	// All three layouts under one state key: a flat item's fields merged with
	// current range entries, plus a range-first bucket.
	var mixedState = json.RawMessage(`{
		"a_table.v1": {
			"Queries": ["Q0"], "ToDelete": ["f0"],
			"00000000-7fffffff": {"Queries": ["QL"], "ToDelete": []}
		},
		"80000000-ffffffff": {"a_table.v1": {"Queries": ["QU"], "ToDelete": []}}
	}`)

	t.Run("current layout", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(currentState))
		require.True(t, d.cpRecovery)
		require.Equal(t, []string{"QL"}, d.cp["a_table.v1"][lowerRangeKey].Queries)
		require.Equal(t, []string{"QU"}, d.cp["a_table.v1"][upperRangeKey].Queries)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("flat entries route to the legacy bucket", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		require.NoError(t, d.UnmarshalState(flatState))
		require.True(t, d.cpRecovery)
		require.Empty(t, d.cp)
		require.Len(t, d.peerShardsCheckpoints[legacyRangeKey], 2)
		require.Equal(t, []string{"Q1"}, d.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Queries)
		require.Equal(t, []string{"f1"}, d.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].ToDelete)
		require.Equal(t, []string{"Q2"}, d.peerShardsCheckpoints[legacyRangeKey]["b_table.v1"].Queries)
	})

	t.Run("range-first entries route to their range bucket, own range included", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(rangeFirstState))
		require.Empty(t, d.cp)
		require.Equal(t, []string{"QL"}, d.peerShardsCheckpoints[lowerRangeKey]["a_table.v1"].Queries)
		require.Equal(t, []string{"QU"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
	})

	t.Run("all layouts under one state key", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.Len(t, d.cp["a_table.v1"], 1)
		require.Equal(t, []string{"QL"}, d.cp["a_table.v1"][lowerRangeKey].Queries)
		require.Equal(t, []string{"Q0"}, d.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].Queries)
		require.Equal(t, []string{"f0"}, d.peerShardsCheckpoints[legacyRangeKey]["a_table.v1"].ToDelete)
		require.Equal(t, []string{"QU"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
	})

	t.Run("an emptied bucket holds nothing", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {}}`)))
		require.Empty(t, d.cp)
	})

	t.Run("non-primary discards everything", func(t *testing.T) {
		var d = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(mixedState))
		require.False(t, d.cpRecovery)
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("unknown fields error", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1")
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {"Unknown": 1}}`)))
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {"00000000-ffffffff": {"Unknown": 1}}}`)))
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"00000000-ffffffff": {"a_table.v1": {"Unknown": 1}}}`)))
	})

	t.Run("non-object entries error", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1")
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": 1}`)))
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"00000000-ffffffff": 1}`)))
	})
}

func TestStartCommitState(t *testing.T) {
	t.Run("single shard emits a full-range merge patch", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1")
		d.cp.add("a_table.v1", fullRangeKey, item("Q1", "f1"))

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{"a_table.v1": {"00000000-ffffffff": {"Queries": ["Q1"], "ToDelete": ["f1"]}}}`, string(state.UpdatedJson))
	})

	t.Run("multi-shard emits a range-scoped merge patch of only its own entries", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		d.cp.add("a_table.v1", lowerRangeKey, item("Q1", "f1"))
		d.cp.add("a_table.v1", upperRangeKey, item("PEER"))

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{"a_table.v1": {"00000000-7fffffff": {"Queries": ["Q1"], "ToDelete": ["f1"]}}}`, string(state.UpdatedJson))
	})

	t.Run("nothing staged emits an empty patch", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")

		state, err := d.startCommitState()
		require.NoError(t, err)
		require.JSONEq(t, `{}`, string(state.UpdatedJson))
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

	t.Run("no-op when non-primary", func(t *testing.T) {
		var d = testTransactor(upperRangeKey)
		require.NoError(t, d.mergePeerStatePatches(patches(`{"a_table.v1": {"00000000-7fffffff": {"Queries": ["Q"], "ToDelete": []}}}`)))
		require.Empty(t, d.cp)
	})

	t.Run("own contribution is skipped, peers merged", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(patches(
			`{"a_table.v1": {"00000000-7fffffff": {"Queries": ["OWN"], "ToDelete": []}}}`,
			`{"a_table.v1": {"80000000-ffffffff": {"Queries": ["PEER"], "ToDelete": ["pf1"]}}}`,
		)))
		require.Len(t, d.cp["a_table.v1"], 1) // own patch is not folded back
		require.Equal(t, []string{"PEER"}, d.cp["a_table.v1"][upperRangeKey].Queries)
	})

	t.Run("range-first peer patches are accepted", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(patches(
			`{"80000000-ffffffff": {"a_table.v1": {"Queries": ["PEER"], "ToDelete": []}}}`,
		)))
		require.Empty(t, d.cp)
		require.Equal(t, []string{"PEER"}, d.peerShardsCheckpoints[upperRangeKey]["a_table.v1"].Queries)
	})

	t.Run("state reset patch errors", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.Error(t, d.mergePeerStatePatches(patches(`null`, `{"a": 1}`)))
	})

	t.Run("empty patches are a no-op", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.mergePeerStatePatches(nil))
	})
}

func TestExecQueriesRetriesRetriableErrors(t *testing.T) {
	var origDelay = queryRetryDelay
	queryRetryDelay = func(int) time.Duration { return 0 }
	t.Cleanup(func() { queryRetryDelay = origDelay })

	var conflict = fmt.Errorf("databricks: execution error: [%s] Duplicated files were committed in a concurrent COPY INTO operation. Please try again later.", retriableErrorClasses[0])
	var d = testTransactor(fullRangeKey)

	t.Run("succeeds once the concurrent copy has finished", func(t *testing.T) {
		var db = recordingDB(t, conflict)
		recording.failFirst = 2

		require.NoError(t, d.execQueries(context.Background(), db, []string{"COPY"}, false))
		require.Equal(t, 3, recording.attempts)
		require.Equal(t, []string{"COPY"}, recording.executed)
	})

	t.Run("gives up after the retry budget", func(t *testing.T) {
		var db = recordingDB(t, conflict)

		var err = d.execQueries(context.Background(), db, []string{"COPY"}, false)
		require.ErrorIs(t, err, conflict)
		require.Equal(t, maxQueryRetries+1, recording.attempts)
	})

	t.Run("other errors are not retried", func(t *testing.T) {
		var db = recordingDB(t, fmt.Errorf("[DELTA_CONCURRENT_APPEND] something else"))

		require.Error(t, d.execQueries(context.Background(), db, []string{"COPY"}, false))
		require.Equal(t, 1, recording.attempts)
	})

	t.Run("cancellation ends the wait", func(t *testing.T) {
		queryRetryDelay = func(int) time.Duration { return time.Hour }
		t.Cleanup(func() { queryRetryDelay = func(int) time.Duration { return 0 } })
		var db = recordingDB(t, conflict)
		var ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		var err = d.execQueries(ctx, db, []string{"COPY"}, false)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, recording.attempts)
	})
}

func TestAcknowledge(t *testing.T) {
	t.Run("single shard clears own entries nested, legacy entries by field", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp.add("a_table.v1", fullRangeKey, item("Q1"))
		d.peerShardsCheckpoints[legacyRangeKey] = checkpoint{"b_table.v1": item("Q2")}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"Q1", "Q2"}, recording.executed)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-ffffffff": null},
			"b_table.v1": {
				"Query": null, "Queries": null, "ToDelete": null,
				"StagedFiles": null, "Bounds": null, "NeedsMerge": null
			}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("primary executes own and peer entries", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		d.cp.add("a_table.v1", lowerRangeKey, item("OWN"))
		d.cp.add("a_table.v1", upperRangeKey, item("PEER"))
		d.peerShardsCheckpoints[upperRangeKey] = checkpoint{"a_table.v1": item("OLD-PEER")}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"OWN", "PEER", "OLD-PEER"}, recording.executed)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null},
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
		require.Empty(t, d.peerShardsCheckpoints)
	})

	t.Run("each layout clears at its own path", func(t *testing.T) {
		var recovered = `{
			"a_table.v1": {
				"Queries": ["FLAT"], "ToDelete": [],
				"00000000-7fffffff": {"Queries": ["OWN"], "ToDelete": []}
			},
			"80000000-ffffffff": {
				"a_table.v1": {"Queries": ["OLD-PEER"], "ToDelete": []},
				"b_table.v1": {"Queries": ["OLD-PEER-B"], "ToDelete": []}
			}
		}`
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(json.RawMessage(recovered)))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"OWN", "FLAT", "OLD-PEER"}, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {
				"Query": null, "Queries": null, "ToDelete": null,
				"StagedFiles": null, "Bounds": null, "NeedsMerge": null,
				"00000000-7fffffff": null
			},
			"80000000-ffffffff": {"a_table.v1": null}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp["a_table.v1"])

		// Reduced into the recovered document, only the unbound b_table entry
		// survives.
		require.JSONEq(t, `{
			"a_table.v1": {},
			"80000000-ffffffff": {"b_table.v1": {"Queries": ["OLD-PEER-B"], "ToDelete": []}}
		}`, reduce(t, recovered, state))
	})

	t.Run("clearing a flat entry spares a current entry merged onto it", func(t *testing.T) {
		// The flat entry was recovered and executed on its own; a peer's
		// current entry landed under the same state key in the meantime.
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		d.peerShardsCheckpoints[legacyRangeKey] = checkpoint{"a_table.v1": item("FLAT")}

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.JSONEq(t, `{
			"a_table.v1": {"80000000-ffffffff": {"Queries": ["PEER"], "ToDelete": []}}
		}`, reduce(t, `{
			"a_table.v1": {
				"Queries": ["FLAT"], "ToDelete": [],
				"80000000-ffffffff": {"Queries": ["PEER"], "ToDelete": []}
			}
		}`, state))
	})

	t.Run("removed binding entries are retained and not cleared", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		d.cp.add("a_table.v1", upperRangeKey, item("PEER"))
		d.cp.add("removed_table.v1", upperRangeKey, item("REMOVED"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Equal(t, []string{"PEER"}, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {"80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.cp["removed_table.v1"][upperRangeKey])
	})

	t.Run("subset drain executes only requested state keys", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp.add("a_table.v1", fullRangeKey, item("QA"))
		d.cp.add("b_table.v1", fullRangeKey, item("QB"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), keys("a_table.v1"))
		require.NoError(t, err)
		require.Equal(t, []string{"QA"}, recording.executed)
		require.JSONEq(t, `{"a_table.v1": {"00000000-ffffffff": null}}`, string(state.UpdatedJson))
		require.NotNil(t, d.cp["b_table.v1"][fullRangeKey]) // untouched, still pending
	})

	t.Run("subset drain spans all range buckets", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
		d.peerShardsCheckpoints[legacyRangeKey] = checkpoint{"a_table.v1": item("LEGACY-A")}
		d.cp.add("a_table.v1", lowerRangeKey, item("OWN-A"))
		d.cp.add("a_table.v1", upperRangeKey, item("PEER-A"))
		d.cp.add("b_table.v1", upperRangeKey, item("PEER-B"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), keys("a_table.v1"))
		require.NoError(t, err)
		require.Equal(t, []string{"OWN-A", "PEER-A", "LEGACY-A"}, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {
				"Query": null, "Queries": null, "ToDelete": null,
				"StagedFiles": null, "Bounds": null, "NeedsMerge": null,
				"00000000-7fffffff": null,
				"80000000-ffffffff": null
			}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.cp["b_table.v1"][upperRangeKey])
	})

	t.Run("nothing to drain returns nil state", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		d.cp.add("b_table.v1", fullRangeKey, item("QB"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), keys("a_table.v1"))
		require.NoError(t, err)
		require.Nil(t, state)
		require.NotNil(t, d.cp["b_table.v1"][fullRangeKey]) // untouched, still pending
	})

	t.Run("non-primary does no work", func(t *testing.T) {
		var d = testTransactor(upperRangeKey, "a_table.v1")
		d.cp.add("a_table.v1", upperRangeKey, item("SHOULD NOT RUN"))
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
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		d.cp.add("a_table.v1", lowerRangeKey, item("Q1"))
		d.cpRecovery = true

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.NoError(t, err)
		require.False(t, d.cpRecovery)

		d.cp.add("a_table.v1", lowerRangeKey, item("Q1"))
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
func renderingTransactor(rangeKey string) *transactor {
	var d = testTransactor(rangeKey)
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
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(true,
			[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
			"own.json"))
		d.cp.add("a_table.v1", upperRangeKey, structuredItem(false,
			[]mergeBoundLiterals{bound("5", "50"), bound("'2024-01-01T12:00:00Z'", "'2024-01-03T00:00:00Z'")},
			"peer.json"))

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
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
	})

	t.Run("one merging entry is enough to merge the coalesced files", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(false, nil, "own.json"))
		d.cp.add("a_table.v1", upperRangeKey, structuredItem(true, nil, "peer.json"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "MERGE INTO `schema`.`a_table`")
	})

	t.Run("entries with no merging coalesce into a single copy", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(false, nil, "own.json"))
		d.cp.add("a_table.v1", upperRangeKey, structuredItem(false, nil, "peer.json"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		var query = recording.executed[0]
		require.Contains(t, query, "COPY INTO `schema`.`a_table`")
		require.Contains(t, query, "FILES = ('own.json','peer.json')")
	})

	t.Run("recovery coalesces exactly like steady state", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))
		d.cp.add("a_table.v1", upperRangeKey, structuredItem(true, nil, "peer.json"))
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
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))
		d.cp.add("a_table.v1", upperRangeKey, structuredItem(true, nil, "peer.json"))
		d.cpRecovery = true

		// The coalesced query fails on deleted staged files, which during
		// recovery means the whole group was applied by a previous session
		// whose state clearing didn't commit: it is skipped and cleared.
		state, err := d.acknowledgeApply(context.Background(),
			recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.NoError(t, err)
		require.Empty(t, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.Empty(t, d.cp)
	})

	t.Run("non-recovery missing objects are fatal", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))

		_, err := d.acknowledgeApply(context.Background(),
			recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), allKeys)
		require.Error(t, err)
	})

	t.Run("entries of older versions run pre-rendered alongside the coalesced query", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))
		d.cp.add("a_table.v1", upperRangeKey, item("OLD-PEER"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 2)
		require.Equal(t, "OLD-PEER", recording.executed[0])
		require.Contains(t, recording.executed[1], "MERGE INTO `schema`.`a_table`")
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
	})

	t.Run("a single entry renders its bounds verbatim", func(t *testing.T) {
		var d = renderingTransactor(fullRangeKey)
		d.cp.add("a_table.v1", fullRangeKey, structuredItem(true,
			[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
			"own.json"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), allKeys)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "l.id >= 1 AND l.id <= 10")
		require.Contains(t, recording.executed[0], "l.ts >= '2024-01-01T00:00:00Z' AND l.ts <= '2024-01-02T00:00:00Z'")
	})

	t.Run("coalesced files chunk into batched queries", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		var files []string
		for i := 0; i < queryBatchSize+1; i++ {
			files = append(files, fmt.Sprintf("f%d.json", i))
		}
		d.cp.add("a_table.v1", lowerRangeKey, structuredItem(true, nil, files...))

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
