package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/sijms/go-ora/v2/network"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
)

func TestStoreDeletes(t *testing.T) {
	for _, tc := range []struct {
		name       string
		doc        string
		hardDelete bool
		delta      bool
		exists     bool
		deleted    bool
		wantDelete bool
		wantSkip   bool
	}{
		{name: "insert", doc: `{"id":2,"status":"shipped"}`, hardDelete: true},
		{name: "update", doc: `{"id":2,"status":"shipped"}`, hardDelete: true, exists: true},
		{name: "protocol delete", doc: `{"id":2}`, hardDelete: true, exists: true, deleted: true, wantDelete: true},
		{name: "protocol delete missing", doc: `{"id":2}`, hardDelete: true, deleted: true, wantSkip: true},
		{name: "source delete", doc: `{"id":2,"_meta":{"op":"d"}}`, hardDelete: true, exists: true, wantDelete: true},
		{name: "source delete missing", doc: `{"id":2,"_meta":{"op":"d"}}`, hardDelete: true, wantDelete: true},
		{name: "escaped source delete", doc: `{"id":2,"_meta":{"op":"\u0064"}}`, hardDelete: true, exists: true, wantDelete: true},
		{name: "escaped metadata key", doc: `{"id":2,"_\u006deta":{"o\u0070":"d"}}`, hardDelete: true, exists: true, wantDelete: true},
		{name: "source update", doc: `{"id":2,"_meta":{"op":"u"}}`, hardDelete: true, exists: true},
		{name: "unrelated op", doc: `{"id":2,"op":"d","nested":{"_meta":{"op":"d"}}}`, hardDelete: true, exists: true},
		{name: "non-string op", doc: `{"id":2,"_meta":{"op":true}}`, hardDelete: true, exists: true},
		{name: "null metadata", doc: `{"id":2,"_meta":null}`, hardDelete: true, exists: true},
		{name: "soft source delete", doc: `{"id":2,"_meta":{"op":"d"}}`, exists: true},
		{name: "soft protocol delete", doc: `{"id":2}`, exists: true, deleted: true},
		{name: "delta source delete", doc: `{"id":2,"_meta":{"op":"d"}}`, hardDelete: true, delta: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transactor, recording := newStoreTest(t)
			transactor.cfg.HardDelete = tc.hardDelete
			transactor.bindings[0].target.DeltaUpdates = tc.delta
			values := tuple.Tuple{"2026-11-11T10:00:00Z", "shipped"}
			if tc.wantDelete {
				// A key-only tombstone must not need valid non-key values.
				values = tuple.Tuple{"not a timestamp", nil}
			}
			start, err := transactor.Store(storeIterator(t, storeRequest(tc.doc, values, tc.exists, tc.deleted)))
			require.NoError(t, err)
			if tc.wantSkip {
				require.Equal(t, []string{"begin"}, recording.queries())
			} else {
				require.Equal(t, []string{"begin", "stage"}, recording.queries())
				args := recording.calls[1].args
				require.Len(t, args, 5)
				key, err := args[0].Value.(*go_ora.Number).String()
				require.NoError(t, err)
				require.Equal(t, "2", key)
				if tc.wantDelete {
					for _, arg := range args[1:4] {
						require.Nil(t, arg.Value)
					}
					require.Equal(t, int64(1), args[4].Value)
				} else {
					require.IsType(t, go_ora.TimeStampTZ{}, args[1].Value)
					require.Equal(t, "shipped", args[2].Value)
					require.Equal(t, tc.doc, args[3].Value)
					require.Equal(t, int64(0), args[4].Value)
				}
			}

			checkpoint := &protocol.Checkpoint{AckIntents: map[pb.Journal][]byte{"test/journal": []byte("ack")}}
			state, future := start(t.Context(), checkpoint)
			require.Nil(t, state)
			require.Nil(t, future)
			want := []string{"begin"}
			if !tc.wantSkip {
				want = append(want, "stage")
				if tc.delta {
					want = append(want, "insert")
				} else {
					want = append(want, "merge")
					if tc.hardDelete {
						want = append(want, "delete")
					}
				}
			}
			want = append(want, "checkpoint", "commit")
			require.Equal(t, want, recording.queries())
			encoded, err := checkpoint.Marshal()
			require.NoError(t, err)
			require.Equal(t, encoded, transactor.fence.Checkpoint)
			args := recording.calls[len(recording.calls)-2].args
			require.Equal(t, go_ora.Clob{String: base64.StdEncoding.EncodeToString(encoded), Valid: true}, args[0].Value)
			require.Equal(t, "test/materialization", args[1].Value)
			require.Equal(t, transactor.fence.KeyBegin, args[2].Value)
			require.Equal(t, transactor.fence.KeyEnd, args[3].Value)
			require.Equal(t, transactor.fence.Fence, args[4].Value)
		})
	}
}

func TestStoreRollback(t *testing.T) {
	for _, failAt := range []string{"stage", "merge", "delete", "checkpoint", "fenced"} {
		t.Run(failAt, func(t *testing.T) {
			transactor, recording := newStoreTest(t)
			transactor.cfg.HardDelete = true
			recording.failAt = failAt
			start, err := transactor.Store(storeIterator(t, storeRequest(`{"id":2}`, tuple.Tuple{"2026-11-11T10:00:00Z", "shipped"}, true, false)))
			if failAt == "stage" {
				require.ErrorContains(t, err, "staging store")
				require.Nil(t, start)
			} else {
				require.NoError(t, err)
				_, future := start(t.Context(), &protocol.Checkpoint{})
				require.NotNil(t, future)
				if failAt == "fenced" {
					require.ErrorContains(t, future.Err(), "fenced off")
				} else {
					require.ErrorContains(t, future.Err(), "injected failure")
				}
			}
			queries := recording.queries()
			require.Equal(t, "rollback", queries[len(queries)-1])
			require.NotContains(t, queries, "commit")
			require.Equal(t, pm.ExplicitZeroCheckpoint, transactor.fence.Checkpoint)
		})
	}
}

func TestStoreDeletesWithoutFlowDocument(t *testing.T) {
	for _, tc := range []struct {
		name       string
		op         any
		ptr        string
		softDelete bool
		delta      bool
		missing    bool
		deleted    bool
		wantDelete bool
	}{
		{name: "source delete", op: "d", wantDelete: true},
		{name: "source delete missing", op: "d", missing: true, wantDelete: true},
		{name: "source update", op: "u"},
		{name: "no operation"},
		{name: "non-string operation", op: true},
		{name: "unrelated operation", op: "d", ptr: "/op"},
		{name: "nested operation", op: "d", ptr: "/nested/_meta/op"},
		{name: "protocol delete", deleted: true, wantDelete: true},
		{name: "soft source delete", op: "d", softDelete: true},
		{name: "soft protocol delete", deleted: true, softDelete: true},
		{name: "delta source delete", op: "d", delta: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transactor, recording := newStoreTest(t)
			transactor.cfg.HardDelete = !tc.softDelete
			transactor.cfg.Advanced.NoFlowDocument = true
			ptr := tc.ptr
			if ptr == "" {
				ptr = "/_meta/op"
			}
			shape := transactor.bindings[0].target.TableShape
			shape.Document = nil
			shape.DeltaUpdates = tc.delta
			shape.Values = append(shape.Values, sql.Projection{Projection: pf.Projection{
				Field: "source_operation", Ptr: ptr,
				Inference: pf.Inference{Types: []string{"boolean", "null", "string"}},
			}})
			target, err := sql.ResolveTable(shape, testDialect)
			require.NoError(t, err)
			transactor.bindings[0].target = target
			values := tuple.Tuple{"2026-11-11T10:00:00Z", "shipped", tc.op}
			if tc.wantDelete {
				values[0] = "not a timestamp"
			}

			// Flow omits DocJson when no root document is selected. The source
			// operation is carried only in the selected value tuple in this case.
			start, err := transactor.Store(storeIterator(t, storeRequest("", values, !tc.missing, tc.deleted)))
			require.NoError(t, err)
			require.Equal(t, []string{"begin", "stage"}, recording.queries())
			args := recording.calls[1].args
			require.Len(t, args, 5)
			if tc.wantDelete {
				for _, arg := range args[1:4] {
					require.Nil(t, arg.Value)
				}
				require.Equal(t, int64(1), args[4].Value)
			} else {
				require.IsType(t, go_ora.TimeStampTZ{}, args[1].Value)
				require.Equal(t, "shipped", args[2].Value)
				require.Equal(t, int64(0), args[4].Value)
			}
			_, future := start(t.Context(), &protocol.Checkpoint{})
			require.Nil(t, future)
			want := []string{"begin", "stage"}
			if tc.delta {
				want = append(want, "insert")
			} else {
				want = append(want, "merge")
				if !tc.softDelete {
					want = append(want, "delete")
				}
			}
			require.Equal(t, append(want, "checkpoint", "commit"), recording.queries())
		})
	}
}

func TestStagingTables(t *testing.T) {
	transactor, recording := newStoreTest(t)
	transactor.cfg.User = "test"
	target := transactor.bindings[0].target
	require.NoError(t, transactor.addBinding(t.Context(), testDialect, testTemplates, "test/materialization", target, testInfoSchema()))
	created := recording.queries()
	require.Len(t, created, 2)
	require.Contains(t, created[0], "CREATE GLOBAL TEMPORARY TABLE TEST.FLOW_LOAD_")
	require.Contains(t, created[1], "CREATE GLOBAL TEMPORARY TABLE TEST.FLOW_STORE_")

	// A later session reuses the tables that an earlier session created.
	recording.calls = nil
	recording.failAt, recording.failErr = created[0], network.NewOracleError(955)
	require.NoError(t, transactor.addBinding(t.Context(), testDialect, testTemplates, "test/materialization", target, testInfoSchema()))
	require.Equal(t, created, recording.queries())
	recording.failErr = network.NewOracleError(1031)
	require.ErrorContains(t, transactor.addBinding(t.Context(), testDialect, testTemplates, "test/materialization", target, testInfoSchema()), "creating staging table")

	// Changed columns get their own tables.
	recording.calls, recording.failAt = nil, ""
	shape := target.TableShape
	shape.Values = shape.Values[:1]
	changed, err := sql.ResolveTable(shape, testDialect)
	require.NoError(t, err)
	require.NoError(t, transactor.addBinding(t.Context(), testDialect, testTemplates, "test/materialization", changed, testInfoSchema()))
	require.NotEqual(t, created, recording.queries())

	recording.calls = nil
	transactor.Destroy()
	require.NotContains(t, strings.Join(recording.queries(), "\n"), "DROP")
}

func TestLoadUUIDWithoutFlowDocument(t *testing.T) {
	// Both reconstructed metadata and the required root-document fallback retain the UUID.
	for _, document := range []bool{false, true} {
		t.Run(fmt.Sprintf("document=%t", document), func(t *testing.T) {
			transactor, recording := newStoreTest(t)
			transactor.cfg.User = "test"
			transactor.cfg.Advanced.NoFlowDocument = true
			shape := transactor.bindings[0].target.TableShape
			if !document {
				shape.Document = nil
				shape.Values = append(shape.Values, sql.Projection{Projection: pf.Projection{
					Field: "metadata", Ptr: "/_meta", Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MAY},
				}})
			}
			target, err := sql.ResolveTable(shape, testDialect)
			require.NoError(t, err)
			require.NoError(t, transactor.addBinding(t.Context(), testDialect, testTemplates, "test/materialization", target, testInfoSchema()))
			b := transactor.bindings[len(transactor.bindings)-1]
			if document {
				require.Contains(t, b.loadQuery, "SELECT r.FLOW_DOCUMENT FROM")
				require.Empty(t, b.nullFieldsToStrip)
			} else {
				require.Contains(t, b.loadQuery, "'_meta' VALUE r.METADATA FORMAT JSON")
			}
			recording.rows = []string{`{"id":2,"status":"shipped","_meta":{"uuid":"849f4a66-be21-11f1-8001-000000000001"}}`}
			txn, err := transactor.storeConn.BeginTx(t.Context(), nil)
			require.NoError(t, err)
			defer txn.Rollback()
			var loaded []json.RawMessage
			require.NoError(t, loadBinding(t.Context(), txn, 0, b, func(_ int, doc json.RawMessage) error {
				loaded = append(loaded, doc)
				return nil
			}))
			require.Len(t, loaded, 1)
			require.Equal(t, "849f4a66-be21-11f1-8001-000000000001", gjson.GetBytes(loaded[0], "_meta.uuid").String())
		})
	}
}

func TestRestoreEmptyStrings(t *testing.T) {
	doc, err := restoreEmptyStrings(json.RawMessage(`{"id":1,"note":null,"status":null}`), []string{"note"})
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1,"note":"","status":null}`, string(doc))
	doc, err = restoreEmptyStrings(json.RawMessage(`{"id":1,"note":"kept"}`), []string{"note"})
	require.NoError(t, err)
	require.Equal(t, `{"id":1,"note":"kept"}`, string(doc))
}

func newStoreTest(t *testing.T) (*transactor, *recordingSQL) {
	t.Helper()
	recording := new(recordingSQL)
	db := stdsql.OpenDB(recording)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	conn, err := db.Conn(t.Context())
	require.NoError(t, err)
	target, err := sql.ResolveTable(sql.TableShape{
		Path: []string{"test", "orders"},
		Keys: []sql.Projection{{Projection: pf.Projection{
			Field: "id", Ptr: "/id", IsPrimaryKey: true,
			Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
		}}},
		Values: []sql.Projection{
			{Projection: pf.Projection{Field: "flow_published_at", Ptr: "/_meta/uuid", Inference: pf.Inference{Types: []string{"string"}, String_: &pf.Inference_String{Format: "date-time"}}}},
			{Projection: pf.Projection{Field: "status", Ptr: "/status", Inference: pf.Inference{Types: []string{"string"}}}},
		},
		Document: &sql.Projection{Projection: pf.Projection{Field: "flow_document", Inference: pf.Inference{Types: []string{"object"}}}},
	}, testDialect)
	require.NoError(t, err)
	transactor := &transactor{
		storeConn:     conn,
		stagingTables: make(map[string]bool),
		fence:         sql.Fence{Materialization: "test/materialization", KeyEnd: 0xffffffff, Fence: 4, Checkpoint: pm.ExplicitZeroCheckpoint},
		fenceSQL:      "checkpoint",
		bindings: []*binding{{
			target: target, storeInsert: "stage", mergeInto: "merge", deleteFrom: "delete", directCopy: "insert",
		}},
	}
	t.Cleanup(transactor.Destroy)
	return transactor, recording
}

func storeRequest(doc string, values tuple.Tuple, exists, deleted bool) pm.Request {
	return pm.Request{Store: &pm.Request_Store{
		KeyPacked: tuple.Tuple{int64(2)}.Pack(), ValuesPacked: values.Pack(), DocJson: json.RawMessage(doc), Exists: exists, Delete: deleted,
	}}
}

func storeIterator(t *testing.T, requests ...pm.Request) *m.StoreIterator {
	t.Helper()
	stream := &requestStream{requests: append(requests, pm.Request{StartCommit: &pm.Request_StartCommit{}})}
	return m.NewStoreIterator(t.Context(), stream, &pm.Request{Flush: &pm.Request_Flush{}})
}

type requestStream struct {
	requests []pm.Request
}

func (s *requestStream) Send(*pm.Response) error { return nil }

func (s *requestStream) RecvMsg(request *pm.Request) error {
	if len(s.requests) == 0 {
		return io.EOF
	}
	*request, s.requests = s.requests[0], s.requests[1:]
	return nil
}

type sqlCall struct {
	query string
	args  []driver.NamedValue
}

// recordingSQL exercises database/sql transactions without a database server.
type recordingSQL struct {
	calls   []sqlCall
	failAt  string
	failErr error
	// rows are the single-column rows returned by every query.
	rows []string
}

type recordingRows struct{ values []string }

func (r *recordingRows) Columns() []string { return []string{"value"} }
func (r *recordingRows) Close() error      { return nil }
func (r *recordingRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	dest[0], r.values = r.values[0], r.values[1:]
	return nil
}

func (c *recordingSQL) Connect(context.Context) (driver.Conn, error) { return c, nil }
func (c *recordingSQL) Driver() driver.Driver                        { return nil }
func (c *recordingSQL) Close() error                                 { return nil }
func (c *recordingSQL) CheckNamedValue(*driver.NamedValue) error     { return nil }
func (c *recordingSQL) Prepare(string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected Prepare")
}
func (c *recordingSQL) Begin() (driver.Tx, error) {
	c.calls = append(c.calls, sqlCall{query: "begin"})
	return c, nil
}
func (c *recordingSQL) Commit() error {
	c.calls = append(c.calls, sqlCall{query: "commit"})
	return nil
}
func (c *recordingSQL) Rollback() error {
	c.calls = append(c.calls, sqlCall{query: "rollback"})
	return nil
}
func (c *recordingSQL) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.calls = append(c.calls, sqlCall{query: query, args: append([]driver.NamedValue(nil), args...)})
	if query == c.failAt && c.failErr != nil {
		return nil, c.failErr
	} else if query == c.failAt {
		return nil, fmt.Errorf("injected failure")
	} else if query == "checkpoint" && c.failAt == "fenced" {
		return driver.RowsAffected(0), nil
	}
	return driver.RowsAffected(1), nil
}
func (c *recordingSQL) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.calls = append(c.calls, sqlCall{query: query, args: append([]driver.NamedValue(nil), args...)})
	if query == c.failAt {
		return nil, fmt.Errorf("injected failure")
	}
	return &recordingRows{values: c.rows}, nil
}
func (c *recordingSQL) queries() []string {
	var out []string
	for _, call := range c.calls {
		out = append(out, call.query)
	}
	return out
}

func TestDropStaleStagingTables(t *testing.T) {
	transactor, recording := newStoreTest(t)
	transactor.cfg.User = "test"
	target := transactor.bindings[0].target
	require.NoError(t, transactor.addBinding(t.Context(), testDialect, testTemplates, "test/materialization", target, testInfoSchema()))
	var current []string
	for name := range transactor.stagingTables {
		current = append(current, name)
	}
	require.Len(t, current, 2)
	prefix := strings.ToUpper(stagingPrefix("test/materialization"))
	other := strings.ToUpper(stagingPrefix("test/other"))

	recording.calls = nil
	recording.failAt = "DROP TABLE TEST.FLOW_STORE_" + prefix + "_OLD"
	recording.rows = append(current, "FLOW_LOAD_"+prefix+"_OLD", "FLOW_STORE_"+prefix+"_OLD", "FLOW_LOAD_"+other+"_OLD", "FLOW_LOAD_"+prefix+"OLD")
	transactor.dropStaleStagingTables(t.Context(), testDialect, "test/materialization")
	queries := recording.queries()
	require.Len(t, queries, 3)
	require.Equal(t, []any{"FLOW_LOAD_" + prefix + "%", "FLOW_STORE_" + prefix + "%"}, []any{recording.calls[0].args[0].Value, recording.calls[0].args[1].Value})
	// Only this task's unused tables are dropped, and a failed drop is left
	// for a later session.
	require.Equal(t, []string{"DROP TABLE TEST.FLOW_LOAD_" + prefix + "_OLD", "DROP TABLE TEST.FLOW_STORE_" + prefix + "_OLD"}, queries[1:])

	// A failure to list tables does not fail the session.
	recording.calls, recording.failAt = nil, queries[0]
	transactor.dropStaleStagingTables(t.Context(), testDialect, "test/materialization")
	require.Equal(t, queries[:1], recording.queries())
}

func TestStoreTruncation(t *testing.T) {
	boundary := time.Date(2026, 11, 11, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		stored bool
		want   []string
	}{
		{name: "with stores", stored: true, want: []string{"begin", "stage", "merge", "delete", "truncate", "checkpoint", "commit"}},
		{name: "without stores", want: []string{"begin", "truncate", "checkpoint", "commit"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transactor, recording := newStoreTest(t)
			transactor.cfg.HardDelete = true
			transactor.truncateAfterBackfill = true
			transactor.bindings[0].truncateSQL = "truncate"
			require.NoError(t, transactor.Flush(t.Context(), map[int]time.Time{0: boundary}))
			var requests []pm.Request
			if tc.stored {
				requests = append(requests, storeRequest(`{"id":2}`, tuple.Tuple{"2026-11-11T10:00:00Z", "shipped"}, true, false))
			}
			start, err := transactor.Store(storeIterator(t, requests...))
			require.NoError(t, err)
			_, future := start(t.Context(), &protocol.Checkpoint{})
			require.Nil(t, future)
			require.Equal(t, tc.want, recording.queries())
			truncate := recording.calls[len(tc.want)-3]
			require.Equal(t, go_ora.TimeStampTZ(boundary), truncate.args[0].Value)
		})
	}

	transactor, recording := newStoreTest(t)
	transactor.truncateAfterBackfill = true
	transactor.bindings[0].truncateSQL = "truncate"
	recording.failAt = "truncate"
	require.NoError(t, transactor.Flush(t.Context(), map[int]time.Time{0: boundary}))
	start, err := transactor.Store(storeIterator(t))
	require.NoError(t, err)
	_, future := start(t.Context(), &protocol.Checkpoint{})
	require.ErrorContains(t, future.Err(), "truncating TEST.ORDERS after backfill")
	require.NotContains(t, recording.queries(), "commit")
}

func testInfoSchema(fields ...boilerplate.ExistingField) *boilerplate.InfoSchema {
	is := boilerplate.NewInfoSchema(func(path []string) []string {
		loc := testDialect.TableLocator(path)
		return []string{loc.TableSchema, loc.TableName}
	}, testDialect.SchemaLocator, testDialect.ColumnLocator, false, false)
	is.PushNamespace("TEST")
	if fields != nil {
		res := is.PushResource("TEST", "ORDERS")
		for _, field := range fields {
			res.PushField(field)
		}
	}
	return is
}
