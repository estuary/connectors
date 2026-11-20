package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/databricks/databricks-sdk-go/service/files"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const (
	fullRangeKey  = "00000000-ffffffff"
	lowerRangeKey = "00000000-7fffffff"
	upperRangeKey = "80000000-ffffffff"
)

func testState(rangeKey string, stateKeys ...string) *boilerplate.ShardedState[*checkpointItem] {
	var keyBegin, keyEnd uint32
	if _, err := fmt.Sscanf(rangeKey, "%08x-%08x", &keyBegin, &keyEnd); err != nil {
		panic(err)
	}
	return boilerplate.NewShardedState[*checkpointItem](keyBegin, keyEnd, stateKeys)
}

func testTransactor(rangeKey string, stateKeys ...string) *transactor {
	var d = &transactor{
		state:    testState(rangeKey, stateKeys...),
		be:       &m.BindingEvents{},
		profiler: boilerplate.NewQueryProfiler(nil, profileInterval, nil, nil, nil),
	}
	for _, sk := range stateKeys {
		d.bindings = append(d.bindings, &binding{target: sql.Table{
			TableShape: sql.TableShape{Path: sql.TablePath{"schema", sk}},
			StateKey:   sk,
		}})
	}
	return d
}

func noReport(m.RowStats) {}

func item(query string, toDelete ...string) *checkpointItem {
	return &checkpointItem{Queries: []string{query}, ToDelete: toDelete}
}

// stage records an entry as pending: this shard's own as StartCommit does, a
// peer's as its state patch.
func stage(t *testing.T, d *transactor, stateKey, rangeKey string, item *checkpointItem) {
	t.Helper()
	if rangeKey == d.state.RangeKey() {
		_, err := d.state.StartCommit(map[string]*checkpointItem{stateKey: item})
		require.NoError(t, err)
		return
	}
	patch, err := json.Marshal(map[string]map[string]*checkpointItem{stateKey: {rangeKey: item}})
	require.NoError(t, err)
	_, err = d.state.Acknowledge([]json.RawMessage{patch}, []string{}, nil)
	require.NoError(t, err)
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

// recordingDriver is a database/sql driver whose connections record every
// executed statement (or fail with a configured error), so Acknowledge tests
// run against a real *sql.DB without a Databricks connection.
type recordingDriver struct{}

type recordingConn struct{}

var recording struct {
	mu       sync.Mutex
	executed []string
	failWith error
	// failFirst limits failWith to the first failFirst statements when
	// non-zero; otherwise failWith fails every statement.
	failFirst int
	// failMatching limits failWith to statements containing it when set.
	failMatching string
	attempts     int
}

func (recordingDriver) Open(string) (driver.Conn, error) { return recordingConn{}, nil }

func (recordingConn) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare is not implemented")
}
func (recordingConn) Close() error              { return nil }
func (recordingConn) Begin() (driver.Tx, error) { return nil, fmt.Errorf("begin is not implemented") }

func (c recordingConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if _, err := c.QueryContext(ctx, query, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

func (recordingConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	recording.mu.Lock()
	defer recording.mu.Unlock()
	recording.attempts++
	if recording.failWith != nil && (recording.failFirst == 0 || recording.attempts <= recording.failFirst) &&
		(recording.failMatching == "" || strings.Contains(query, recording.failMatching)) {
		return nil, recording.failWith
	}
	recording.executed = append(recording.executed, query)
	return noRows{}, nil
}

// noRows is the empty result of a recorded statement.
type noRows struct{}

func (noRows) Columns() []string              { return nil }
func (noRows) Close() error                   { return nil }
func (noRows) Next(dest []driver.Value) error { return io.EOF }

var registerRecordingDriver = sync.OnceFunc(func() {
	stdsql.Register("recording", recordingDriver{})
})

// recordingDB resets the recorder and returns a *sql.DB whose statements it
// captures. A non-nil failWith fails every statement.
func recordingDB(t *testing.T, failWith error) *stdsql.DB {
	t.Helper()
	registerRecordingDriver()

	recording.executed, recording.failWith, recording.failFirst, recording.failMatching, recording.attempts = nil, failWith, 0, "", 0
	db, err := stdsql.Open("recording", "")
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestUnmarshalState(t *testing.T) {
	var state = json.RawMessage(`{
		"a_table.v1": {
			"00000000-7fffffff": {"Queries": ["QL"], "ToDelete": []},
			"80000000-ffffffff": {"Queries": ["QU"], "ToDelete": []}
		}
	}`)

	t.Run("the primary keeps every shard's entries", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		require.True(t, d.cpRecovery)
		require.Equal(t, []string{"QL"}, d.state.Pending()["a_table.v1"][lowerRangeKey].Queries)
		require.Equal(t, []string{"QU"}, d.state.Pending()["a_table.v1"][upperRangeKey].Queries)
	})

	t.Run("non-primary discards everything", func(t *testing.T) {
		var d = testTransactor(upperRangeKey, "a_table.v1")
		require.NoError(t, d.UnmarshalState(state))
		require.False(t, d.cpRecovery)
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("unknown fields error", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1")
		require.Error(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {"00000000-ffffffff": {"Unknown": 1}}}`)))
	})
}

func TestStartCommitState(t *testing.T) {
	t.Run("single shard emits a full-range merge patch", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1")

		state, err := d.state.StartCommit(map[string]*checkpointItem{"a_table.v1": item("Q1", "f1")})
		require.NoError(t, err)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{"a_table.v1": {"00000000-ffffffff": {"Queries": ["Q1"], "ToDelete": ["f1"]}}}`, string(state.UpdatedJson))
	})

	t.Run("multi-shard emits a range-scoped merge patch of only its own entries", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		stage(t, d, "a_table.v1", upperRangeKey, item("PEER"))

		state, err := d.state.StartCommit(map[string]*checkpointItem{"a_table.v1": item("Q1", "f1")})
		require.NoError(t, err)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{"a_table.v1": {"00000000-7fffffff": {"Queries": ["Q1"], "ToDelete": ["f1"]}}}`, string(state.UpdatedJson))
	})
}

func TestExecQueriesRetriesRetriableErrors(t *testing.T) {
	var conflict = fmt.Errorf("databricks: execution error: [%s] Duplicated files were committed in a concurrent COPY INTO operation. Please try again later.", retriableErrorClasses[0])
	var d = testTransactor(fullRangeKey)

	t.Run("succeeds once the concurrent copy has finished", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var db = recordingDB(t, conflict)
			recording.failFirst = 2

			require.NoError(t, d.execQueries(t.Context(), db, d.profiler.NewProfile(), 0, []string{"COPY"}, false, noReport))
			require.Equal(t, 3, recording.attempts)
			require.Equal(t, []string{"COPY"}, recording.executed)
		})
	})

	t.Run("waits minutes for the concurrent copy before giving up", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var db = recordingDB(t, conflict)

			var start = time.Now()
			var err = d.execQueries(t.Context(), db, d.profiler.NewProfile(), 0, []string{"COPY"}, false, noReport)
			require.ErrorIs(t, err, conflict)
			require.Equal(t, commitQueryRetry.MaxAttempts, recording.attempts)
			require.Greater(t, time.Since(start), 3*time.Minute)
		})
	})

	t.Run("other errors are not retried", func(t *testing.T) {
		var db = recordingDB(t, fmt.Errorf("[DELTA_CONCURRENT_APPEND] something else"))

		require.Error(t, d.execQueries(t.Context(), db, d.profiler.NewProfile(), 0, []string{"COPY"}, false, noReport))
		require.Equal(t, 1, recording.attempts)
	})

	t.Run("cancellation ends the wait", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var db = recordingDB(t, conflict)
			var ctx, cancel = context.WithCancel(t.Context())
			time.AfterFunc(time.Second, cancel)

			var err = d.execQueries(ctx, db, d.profiler.NewProfile(), 0, []string{"COPY"}, false, noReport)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, recording.attempts)
		})
	})
}

func TestAcknowledge(t *testing.T) {
	t.Run("single shard clears its entries", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		stage(t, d, "a_table.v1", fullRangeKey, item("Q1"))
		stage(t, d, "b_table.v1", fullRangeKey, item("Q2"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{"Q1", "Q2"}, recording.executed)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-ffffffff": null},
			"b_table.v1": {"00000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("primary executes own and peer entries", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		stage(t, d, "a_table.v1", lowerRangeKey, item("OWN"))
		stage(t, d, "a_table.v1", upperRangeKey, item("PEER"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"OWN", "PEER"}, recording.executed)
		require.True(t, state.MergePatch)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("removed binding entries are retained and not cleared", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		stage(t, d, "a_table.v1", upperRangeKey, item("PEER"))
		stage(t, d, "removed_table.v1", upperRangeKey, item("REMOVED"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.Equal(t, []string{"PEER"}, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {"80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.state.Pending()["removed_table.v1"][upperRangeKey])
	})

	t.Run("subset drain executes only requested state keys", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		stage(t, d, "a_table.v1", fullRangeKey, item("QA"))
		stage(t, d, "b_table.v1", fullRangeKey, item("QB"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, []string{"a_table.v1"})
		require.NoError(t, err)
		require.Equal(t, []string{"QA"}, recording.executed)
		require.JSONEq(t, `{"a_table.v1": {"00000000-ffffffff": null}}`, string(state.UpdatedJson))
		require.NotNil(t, d.state.Pending()["b_table.v1"][fullRangeKey]) // untouched, still pending
	})

	t.Run("subset drain spans every shard's entries", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1", "b_table.v1")
		stage(t, d, "a_table.v1", lowerRangeKey, item("OWN-A"))
		stage(t, d, "a_table.v1", upperRangeKey, item("PEER-A"))
		stage(t, d, "b_table.v1", upperRangeKey, item("PEER-B"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, []string{"a_table.v1"})
		require.NoError(t, err)
		require.Equal(t, []string{"OWN-A", "PEER-A"}, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.NotNil(t, d.state.Pending()["b_table.v1"][upperRangeKey])
	})

	t.Run("nothing to drain returns nil state", func(t *testing.T) {
		var d = testTransactor(fullRangeKey, "a_table.v1", "b_table.v1")
		stage(t, d, "b_table.v1", fullRangeKey, item("QB"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, []string{"a_table.v1"})
		require.NoError(t, err)
		require.Nil(t, state)
		require.NotNil(t, d.state.Pending()["b_table.v1"][fullRangeKey]) // untouched, still pending
	})

	t.Run("non-primary does no work", func(t *testing.T) {
		var d = testTransactor(upperRangeKey, "a_table.v1")
		stage(t, d, "a_table.v1", upperRangeKey, item("SHOULD NOT RUN"))

		// Acknowledge itself is callable here since the non-primary path
		// never opens a database connection.
		state, err := d.Acknowledge(context.Background(), nil, nil)
		require.NoError(t, err)
		require.Nil(t, state)
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("recovery tolerates deleted paths", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		stage(t, d, "a_table.v1", lowerRangeKey, item("Q1"))
		d.cpRecovery = true

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), nil, nil)
		require.NoError(t, err)
		require.False(t, d.cpRecovery)

		stage(t, d, "a_table.v1", lowerRangeKey, item("Q1"))
		_, err = d.acknowledgeApply(context.Background(), recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), nil, nil)
		require.Error(t, err) // no longer a recovery apply
	})

	t.Run("recovery tolerates a staging table without its transaction log", func(t *testing.T) {
		var d = testTransactor(lowerRangeKey, "a_table.v1")
		stage(t, d, "a_table.v1", lowerRangeKey, item("Q1"))
		d.cpRecovery = true

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, fmt.Errorf("[DELTA_MISSING_TRANSACTION_LOG] Incompatible format detected")), nil, nil)
		require.NoError(t, err)
		require.False(t, d.cpRecovery)
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
	d.state = testState(rangeKey, "a_table.v1")
	d.templates = testTemplates
	d.files = &fakeFiles{}
	d.bindings = append(d.bindings, &binding{
		target:          renderableTable(),
		rootStagingPath: "/Volumes/cat/schema/flow_staging/flow_temp_tables",
	})
	return d
}

func bound(lower, upper string) mergeBoundLiterals {
	return mergeBoundLiterals{Lower: lower, Upper: upper}
}

// structuredItem is an entry written before staged files had a directory per
// transaction.
func structuredItem(needsMerge bool, bounds []mergeBoundLiterals, files ...string) *checkpointItem {
	return &checkpointItem{
		StagedFiles: files,
		Bounds:      bounds,
		NeedsMerge:  needsMerge,
	}
}

func dirItem(needsMerge bool, bounds []mergeBoundLiterals, dir string) *checkpointItem {
	return &checkpointItem{
		Directory:  dir,
		Bounds:     bounds,
		NeedsMerge: needsMerge,
	}
}

func TestAcknowledgeCoalescesShardEntries(t *testing.T) {
	t.Run("entries of all shards coalesce into a single merge", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true,
			[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
			"own.json"))
		stage(t, d, "a_table.v1", upperRangeKey, structuredItem(false,
			[]mergeBoundLiterals{bound("5", "50"), bound("'2024-01-01T12:00:00Z'", "'2024-01-03T00:00:00Z'")},
			"peer.json"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
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
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("one merging entry is enough to merge the coalesced files", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(false, nil, "own.json"))
		stage(t, d, "a_table.v1", upperRangeKey, structuredItem(true, nil, "peer.json"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "MERGE INTO `schema`.`a_table`")
	})

	t.Run("entries with no merging coalesce into a single copy", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(false, nil, "own.json"))
		stage(t, d, "a_table.v1", upperRangeKey, structuredItem(false, nil, "peer.json"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		var query = recording.executed[0]
		require.Contains(t, query, "COPY INTO `schema`.`a_table`")
		require.Contains(t, query, "FILES = ('own.json','peer.json')")
	})

	t.Run("recovery coalesces exactly like steady state", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))
		stage(t, d, "a_table.v1", upperRangeKey, structuredItem(true, nil, "peer.json"))
		d.cpRecovery = true

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.Len(t, recording.executed, 1)
		require.Contains(t, recording.executed[0], "MERGE INTO `schema`.`a_table`")
		require.Contains(t, recording.executed[0], "own.json")
		require.Contains(t, recording.executed[0], "peer.json")
		require.False(t, d.cpRecovery)
	})

	t.Run("recovery tolerates an already-applied group whose files are gone", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))
		stage(t, d, "a_table.v1", upperRangeKey, structuredItem(true, nil, "peer.json"))
		d.cpRecovery = true

		// The coalesced query fails on deleted staged files, which during
		// recovery means the whole group was applied by a previous session
		// whose state clearing didn't commit: it is skipped and cleared.
		state, err := d.acknowledgeApply(context.Background(),
			recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), nil, nil)
		require.NoError(t, err)
		require.Empty(t, recording.executed)
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
		}`, string(state.UpdatedJson))
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("non-recovery missing objects are fatal", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))

		_, err := d.acknowledgeApply(context.Background(),
			recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), nil, nil)
		require.Error(t, err)
	})

	t.Run("entries of older versions run pre-rendered alongside the coalesced query", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true, nil, "own.json"))
		stage(t, d, "a_table.v1", upperRangeKey, item("OLD-PEER"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
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
		stage(t, d, "a_table.v1", fullRangeKey, structuredItem(true,
			[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
			"own.json"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
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
		stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true, nil, files...))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)
		require.Len(t, recording.executed, 2)
	})
}

// TestCrashRecovery proves that pending work survives a crash at either risky
// point of the post-commit-apply cycle: after a StartedCommit checkpoint is
// durable but before Acknowledge's clearing patch lands, and again during the
// retry that follows. It is applied at most once and converges to an empty
// checkpoint.
func TestCrashRecovery(t *testing.T) {
	var doc = fmt.Sprintf(`{"a_table.v1": {%q: {"StagedFiles": ["f.json"], "NeedsMerge": true, "Bounds": [{"Lower":"1","Upper":"10"},{"Lower":"'2024-01-01T00:00:00Z'","Upper":"'2024-01-02T00:00:00Z'"}]}}}`, fullRangeKey)

	// Crash 1: the StartedCommit checkpoint is durable, then the process dies
	// before Acknowledge's clearing patch is persisted.
	var d1 = renderingTransactor(fullRangeKey)
	require.NoError(t, d1.UnmarshalState(json.RawMessage(doc)))
	require.True(t, d1.cpRecovery)

	state1, err := d1.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
	require.NoError(t, err)
	require.Len(t, recording.executed, 1) // the one real side effect
	require.Contains(t, recording.executed[0], "MERGE INTO `schema`.`a_table`")
	require.NotNil(t, state1) // the clear patch the runtime failed to persist

	// Crash 2: the process dies again before the retry's clearing patch
	// persists, so a second restart recovers the same pending entry. Its
	// staged file was deleted after crash 1's MERGE, so this retry's query
	// hits a missing-file error, which recovery tolerates as already applied.
	var d2 = renderingTransactor(fullRangeKey)
	require.NoError(t, d2.UnmarshalState(json.RawMessage(doc)))
	require.True(t, d2.cpRecovery)

	state2, err := d2.acknowledgeApply(context.Background(),
		recordingDB(t, fmt.Errorf("some PATH_NOT_FOUND error")), nil, nil)
	require.NoError(t, err)
	require.Empty(t, recording.executed) // no second real side effect
	require.NotNil(t, state2)

	// This time the clearing patch lands, so a third restart finds no work.
	var d3 = renderingTransactor(fullRangeKey)
	require.NoError(t, d3.UnmarshalState(json.RawMessage(reduce(t, doc, state2))))
	require.Zero(t, d3.state.Pending().Len())
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

	t.Run("ranges union across entries", func(t *testing.T) {
		var split = bound("'2026-03-01T00:00:00Z'", "'2026-10-26T12:15:00Z'")
		split.Ranges = []mergeRangeLiterals{
			{"'2026-03-01T00:00:00Z'", "'2026-03-01T00:00:00Z'"},
			{"'2026-10-26T12:00:00Z'", "'2026-10-26T12:15:00Z'"},
		}
		var items = []*checkpointItem{
			{Bounds: []mergeBoundLiterals{bound("1", "10"), split}},
			{Bounds: []mergeBoundLiterals{bound("5", "50"), bound("'2026-10-26T12:10:00Z'", "'2026-10-26T12:30:00Z'")}},
			{Bounds: []mergeBoundLiterals{bound("5", "50"), split}},
		}
		var out = combineBounds(keys, items)
		require.Nil(t, out[0].Ranges)
		require.Equal(t, "GREATEST('2026-10-26T12:15:00Z'::TIMESTAMP, '2026-10-26T12:30:00Z'::TIMESTAMP)", out[1].LiteralUpper)
		require.Equal(t, []sql.MergeRange{
			{LiteralLower: "'2026-03-01T00:00:00Z'", LiteralUpper: "'2026-03-01T00:00:00Z'"},
			{LiteralLower: "'2026-10-26T12:00:00Z'", LiteralUpper: "'2026-10-26T12:15:00Z'"},
			{LiteralLower: "'2026-10-26T12:10:00Z'", LiteralUpper: "'2026-10-26T12:30:00Z'"},
		}, out[1].Ranges)
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

// Root-level files from a checkpoint written before staging directories existed
// merge in their own query, ahead of the directories.
// A recovered entry's bound ranges survive the persisted state and bound its
// MERGE.
func TestAcknowledgeMergesWithRecoveredRanges(t *testing.T) {
	var d = renderingTransactor(fullRangeKey)
	require.NoError(t, d.UnmarshalState(json.RawMessage(`{"a_table.v1": {"00000000-ffffffff": {
		"Directory": "txn-1",
		"NeedsMerge": true,
		"Bounds": [
			{"Lower": "1", "Upper": "10"},
			{"Lower": "'2026-03-01T00:00:00Z'", "Upper": "'2026-10-26T12:15:00Z'", "Ranges": [
				{"Lower": "'2026-03-01T00:00:00Z'", "Upper": "'2026-03-01T00:00:00Z'"},
				{"Lower": "'2026-10-26T12:00:00Z'", "Upper": "'2026-10-26T12:15:00Z'"}
			]}
		]
	}}}`)))

	_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
	require.NoError(t, err)
	require.Len(t, recording.executed, 1)
	require.Contains(t, recording.executed[0], "l.id = r.id AND l.id >= 1 AND l.id <= 10")
	require.Contains(t, recording.executed[0], "l.ts = r.ts AND ("+
		"(l.ts >= '2026-03-01T00:00:00Z' AND l.ts <= '2026-03-01T00:00:00Z') OR "+
		"(l.ts >= '2026-10-26T12:00:00Z' AND l.ts <= '2026-10-26T12:15:00Z'))")
}

func TestAcknowledgeMergesRootFilesWithDirectories(t *testing.T) {
	var d = renderingTransactor(lowerRangeKey)
	stage(t, d, "a_table.v1", lowerRangeKey, structuredItem(true,
		[]mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")},
		"old.json.gz"))
	stage(t, d, "a_table.v1", upperRangeKey, dirItem(true,
		[]mergeBoundLiterals{bound("5", "50"), bound("'2024-01-01T12:00:00Z'", "'2024-01-03T00:00:00Z'")},
		"txn-2"))

	state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
	require.NoError(t, err)

	require.Len(t, recording.executed, 2)
	var rootQuery, dirQuery = recording.executed[0], recording.executed[1]
	require.Contains(t, rootQuery, "FROM json.`/Volumes/cat/schema/flow_staging/flow_temp_tables/old.json.gz`")
	require.NotContains(t, rootQuery, "read_files(")
	require.Contains(t, dirQuery, "read_files('/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-2', format => 'json', schema => ")
	require.Equal(t, 1, strings.Count(dirQuery, "read_files("))
	require.NotContains(t, dirQuery, "old.json.gz")
	require.Equal(t, []string{"/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-2"}, deletedFiles(d))
	for _, query := range recording.executed {
		require.Contains(t, query, "MERGE INTO `schema`.`a_table`")
		require.Contains(t, query, "l.id >= LEAST(1::LONG, 5::LONG)")
		require.Contains(t, query, "l.id <= GREATEST(10::LONG, 50::LONG)")
	}

	require.JSONEq(t, `{
		"a_table.v1": {"00000000-7fffffff": null, "80000000-ffffffff": null}
	}`, string(state.UpdatedJson))
	require.Zero(t, d.state.Pending().Len())
}

func TestAcknowledgeCommitsDirectoriesOfAllShards(t *testing.T) {
	t.Run("merge reads every directory in one query", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, dirItem(true, []mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")}, "txn-1"))
		stage(t, d, "a_table.v1", upperRangeKey, dirItem(false, []mergeBoundLiterals{bound("5", "50"), bound("'2024-01-01T12:00:00Z'", "'2024-01-03T00:00:00Z'")}, "txn-2"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)

		require.Len(t, recording.executed, 1)
		var query = recording.executed[0]
		require.Contains(t, query, "MERGE INTO `schema`.`a_table`")
		require.Contains(t, query, "read_files('/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-1'")
		require.Contains(t, query, "read_files('/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-2'")
		require.Contains(t, query, "l.id >= LEAST(1::LONG, 5::LONG)")
		require.ElementsMatch(t, []string{
			"/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-1",
			"/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-2",
		}, deletedFiles(d))
	})

	t.Run("copy reads each directory whole", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		stage(t, d, "a_table.v1", lowerRangeKey, dirItem(false, nil, "txn-1"))
		stage(t, d, "a_table.v1", upperRangeKey, dirItem(false, nil, "txn-2"))

		_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)

		require.Len(t, recording.executed, 2)
		for i, dir := range []string{"txn-1", "txn-2"} {
			require.Contains(t, recording.executed[i], "COPY INTO `schema`.`a_table`")
			require.Contains(t, recording.executed[i], "FROM '/Volumes/cat/schema/flow_staging/flow_temp_tables/"+dir+"'")
			require.NotContains(t, recording.executed[i], "FILES")
		}
	})
}

func TestAcknowledgeMergesStagingTables(t *testing.T) {
	var d = renderingTransactor(lowerRangeKey)
	var item = dirItem(true, []mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")}, "txn-1")
	item.StagingTable = "txn-1_delta"
	stage(t, d, "a_table.v1", lowerRangeKey, item)
	stage(t, d, "a_table.v1", upperRangeKey, dirItem(false,
		[]mergeBoundLiterals{bound("5", "50"), bound("'2024-01-01T12:00:00Z'", "'2024-01-03T00:00:00Z'")},
		"txn-2"))

	_, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
	require.NoError(t, err)

	require.Len(t, recording.executed, 1)
	var query = recording.executed[0]
	require.Contains(t, query, "MERGE INTO `schema`.`a_table`")
	require.Contains(t, query, "FROM delta.`/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-1_delta`")
	require.Contains(t, query, "read_files('/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-2', format => 'json'")
	require.NotContains(t, query, "flow_temp_tables/txn-1'")
	require.ElementsMatch(t, []string{
		"/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-1_delta",
		"/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-1",
		"/Volumes/cat/schema/flow_staging/flow_temp_tables/txn-2",
	}, deletedFiles(d))
	require.Zero(t, d.state.Pending().Len())
}

func TestCopyIntoStagingTable(t *testing.T) {
	for _, tc := range []struct {
		needsMerge bool
		bytes      int // written before the test's one row
		last       bool
		copies     bool
	}{
		{needsMerge: false, bytes: minStagingTableBytes, copies: false},
		{needsMerge: true, bytes: 0, copies: false},
		{needsMerge: true, bytes: minStagingTableBytes, copies: true},
		{needsMerge: true, bytes: minStagingTableBytes, last: true, copies: false},
	} {
		var db = recordingDB(t, nil)
		var d = renderingTransactor(lowerRangeKey)
		var b = d.bindings[0]
		b.storeFile = newStagedFile(config{}, b.rootStagingPath, "abc", []string{"id", "ts", "_flow_delete"}, d.files)
		b.storeFile.dir = filepath.Join(t.TempDir(), "local")
		b.needsMerge = tc.needsMerge

		var ctx = context.Background()
		require.NoError(t, b.storeFile.start(ctx, db))
		b.storeFile.uncompressed = tc.bytes
		require.NoError(t, b.storeFile.writeRow([]any{1, "2024-01-01T00:00:00Z", false}))
		require.NoError(t, d.copyIntoStagingTable(ctx, db, 0, b, tc.last))

		if !tc.copies {
			require.Len(t, recording.executed, 1)
			require.Empty(t, b.stagingTable)
			continue
		}
		var dir = b.storeFile.remoteDir()
		require.Len(t, recording.executed, 3)
		require.True(t, strings.HasPrefix(recording.executed[0], "PUT "), recording.executed[0])
		require.Contains(t, recording.executed[1], "CREATE TABLE IF NOT EXISTS delta.`"+dir+"_delta`")
		require.Contains(t, recording.executed[2], "COPY INTO delta.`"+dir+"_delta`")
		require.Contains(t, recording.executed[2], "FROM '"+dir+"'")
		require.NotContains(t, recording.executed[2], "FILES")
		require.Equal(t, b.storeFile.txnDir+"_delta", b.stagingTable)
	}
}

// deletedFiles waits for the transactor's background cleanups and returns what
// they deleted.
func deletedFiles(d *transactor) []string {
	d.cleanups.Wait()
	return d.files.(*fakeFiles).deleted
}

// v7At returns a version 7 UUID created at t.
func v7At(t time.Time) string {
	var id = uuid.Must(uuid.NewV7())
	var ms = uint64(t.UnixMilli())
	for i := 0; i < 6; i++ {
		id[i] = byte(ms >> (40 - 8*i))
	}
	return id.String()
}

func TestStagedBefore(t *testing.T) {
	var now = time.Now()
	var old, fresh = v7At(now.Add(-48 * time.Hour)), v7At(now)
	var cutoff = now.Add(-sweepAge)

	require.True(t, stagedBefore("abc", "abc_"+old, cutoff))
	require.True(t, stagedBefore("abc", "abc_"+old+"_delta", cutoff))
	require.False(t, stagedBefore("abc", "abc_"+fresh, cutoff))
	require.False(t, stagedBefore("abc", "xyz_"+old, cutoff), "another task's directory")
	require.False(t, stagedBefore("abc", old, cutoff), "a directory of an earlier version")
	require.False(t, stagedBefore("abc", "abc_"+uuid.NewString(), cutoff), "not a v7 UUID")
}

func TestSweepStaging(t *testing.T) {
	var d = renderingTransactor(lowerRangeKey)
	d.stagingPrefix = "abc"
	var root = d.bindings[0].rootStagingPath
	var old = func() string { return "abc_" + v7At(time.Now().Add(-48*time.Hour)) }
	var stale, pending, fresh, foreign = old(), old(), "abc_" + v7At(time.Now()), "xyz_" + v7At(time.Now().Add(-48*time.Hour))
	stage(t, d, "a_table.v1", lowerRangeKey, dirItem(true, nil, pending))

	var dir = func(name string) files.DirectoryEntry {
		return files.DirectoryEntry{Name: name, Path: root + "/" + name, IsDirectory: true}
	}
	d.files.(*fakeFiles).entries = map[string][]files.DirectoryEntry{
		root: {dir(stale), dir(stale + "_delta"), dir(pending), dir(fresh), dir(foreign), {Name: "legacy.json.gz", Path: root + "/legacy.json.gz"}},
	}

	d.sweepStaging(context.Background())
	require.Eventually(t, func() bool {
		d.files.(*fakeFiles).mu.Lock()
		defer d.files.(*fakeFiles).mu.Unlock()
		return len(d.files.(*fakeFiles).deleted) == 2
	}, 5*time.Second, 10*time.Millisecond)
	require.ElementsMatch(t, []string{root + "/" + stale, root + "/" + stale + "_delta"}, deletedFiles(d))
}

func TestDeleteDirectoryRecurses(t *testing.T) {
	var d = renderingTransactor(lowerRangeKey)
	d.files.(*fakeFiles).entries = map[string][]files.DirectoryEntry{
		"/t":            {{Path: "/t/part-0.parquet"}, {Path: "/t/_delta_log", IsDirectory: true}},
		"/t/_delta_log": {{Path: "/t/_delta_log/0.json"}},
	}
	require.NoError(t, d.deleteDirectory(context.Background(), "/t"))
	var deleted = d.files.(*fakeFiles).deleted
	require.ElementsMatch(t, []string{"/t/part-0.parquet", "/t/_delta_log/0.json", "/t/_delta_log", "/t"}, deleted)
	require.Equal(t, "/t", deleted[len(deleted)-1], "a directory is deleted after its contents")
}

// secondBinding adds a second renderable binding, b_table, to a rendering
// transactor.
func secondBinding(d *transactor) {
	var target = renderableTable()
	target.TableShape.Path = sql.TablePath{"schema", "b_table"}
	target.Identifier = "`schema`.`b_table`"
	target.StateKey = "b_table.v1"
	d.bindings = append(d.bindings, &binding{target: target, rootStagingPath: d.bindings[0].rootStagingPath})
	d.state = testState(d.state.RangeKey(), "a_table.v1", "b_table.v1")
}

func TestAcknowledgeCommitsBindingsConcurrently(t *testing.T) {
	t.Run("every binding's entries commit and clear", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		secondBinding(d)
		stage(t, d, "a_table.v1", lowerRangeKey, dirItem(true, []mergeBoundLiterals{bound("1", "10"), bound("'2024-01-01T00:00:00Z'", "'2024-01-02T00:00:00Z'")}, "txn-a"))
		stage(t, d, "b_table.v1", lowerRangeKey, dirItem(false, nil, "txn-b"))

		state, err := d.acknowledgeApply(context.Background(), recordingDB(t, nil), nil, nil)
		require.NoError(t, err)

		require.Len(t, recording.executed, 2)
		var queries = strings.Join(recording.executed, "\n")
		require.Contains(t, queries, "MERGE INTO `schema`.`a_table`")
		require.Contains(t, queries, "COPY INTO `schema`.`b_table`")
		require.JSONEq(t, `{
			"a_table.v1": {"00000000-7fffffff": null},
			"b_table.v1": {"00000000-7fffffff": null}
		}`, string(state.UpdatedJson))
		require.Zero(t, d.state.Pending().Len())
	})

	t.Run("a failed binding keeps its entries pending", func(t *testing.T) {
		var d = renderingTransactor(lowerRangeKey)
		secondBinding(d)
		stage(t, d, "a_table.v1", lowerRangeKey, dirItem(false, nil, "txn-a"))
		stage(t, d, "b_table.v1", lowerRangeKey, dirItem(false, nil, "txn-b"))

		var db = recordingDB(t, fmt.Errorf("b_table is locked"))
		recording.failMatching = "`b_table`"
		_, err := d.acknowledgeApply(context.Background(), db, nil, nil)
		require.ErrorContains(t, err, "b_table is locked")

		// The other binding may or may not have committed before the failure
		// cancelled it; either way nothing is cleared, so recovery re-applies
		// both.
		require.NotNil(t, d.state.Pending()["a_table.v1"])
		require.NotNil(t, d.state.Pending()["b_table.v1"])
	})
}
