package connector

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

const (
	backfillQuery  = `ALTER WAREHOUSE "WH" SET WAREHOUSE_SIZE = X4LARGE;`
	streamingQuery = `ALTER WAREHOUSE "WH" SET WAREHOUSE_SIZE = XSMALL;`
)

// recorder is a database driver that appends the statements executed on it to
// queries, and fails them with err if it is set.
type recorder struct {
	queries *[]string
	err     *error
}

func recordingDB(queries *[]string, err *error) *stdsql.DB {
	return stdsql.OpenDB(recorder{queries: queries, err: err})
}

func (r recorder) Connect(context.Context) (driver.Conn, error) { return r, nil }
func (r recorder) Driver() driver.Driver                        { return nil }
func (r recorder) Close() error                                 { return nil }
func (r recorder) Begin() (driver.Tx, error)                    { return nil, errors.ErrUnsupported }
func (r recorder) Prepare(string) (driver.Stmt, error)          { return nil, errors.ErrUnsupported }

func (r recorder) ExecContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Result, error) {
	*r.queries = append(*r.queries, query)
	if r.err != nil && *r.err != nil {
		return nil, *r.err
	}
	return driver.RowsAffected(0), nil
}

// sizedTransactor returns a transactor that resizes its warehouse, with the
// bindings "a" and "b" that store through the warehouse and the Snowpipe
// Streaming binding "s". The statements it executes are appended to queries.
func sizedTransactor(queries *[]string, backfilling map[string]bool) *transactor {
	var newBinding = func(stateKey string) *binding {
		var b = &binding{target: sql.Table{StateKey: stateKey, Identifier: stateKey}}
		b.store.mergeStage = &stagedFile{}
		b.store.copyStage = &stagedFile{}
		return b
	}
	var streaming = newBinding("s")
	streaming.streaming = true

	var d = &transactor{
		bindings:    []*binding{newBinding("a"), newBinding("b"), streaming},
		cp:          make(checkpoint),
		db:          recordingDB(queries, nil),
		warehouse:   `"WH"`,
		backfilling: backfilling,
	}
	d.cfg.Advanced.StreamingWarehouseSize = "XSMALL"
	d.cfg.Advanced.BackfillWarehouseSize = "X4LARGE"
	return d
}

func TestWarehouseSizeFollowsBackfills(t *testing.T) {
	var ctx = t.Context()
	var at = time.Date(2026, 11, 19, 1, 0, 0, 0, time.UTC)

	var queries []string
	var d = sizedTransactor(&queries, map[string]bool{})

	// The recovery Acknowledge sets the size that the recovered state calls for.
	_, err := d.Acknowledge(ctx, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []string{streamingQuery}, queries)

	for _, tc := range []struct {
		name              string
		begins, completes map[int]time.Time
		// flush and ack are the statements of the Flush and of the Acknowledge.
		flush, ack []string
		// checkpoint is the Backfilling of the transaction's checkpoint.
		checkpoint map[string]bool
	}{
		{name: "no signals", checkpoint: map[string]bool{}},
		{name: "a backfill begins", begins: map[int]time.Time{0: at}, flush: []string{backfillQuery}, checkpoint: map[string]bool{"a": true}},
		{name: "no signals during the backfill", checkpoint: map[string]bool{"a": true}},
		{name: "a second begins", begins: map[int]time.Time{1: at}, checkpoint: map[string]bool{"a": true, "b": true}},
		{name: "a streaming binding is ignored", begins: map[int]time.Time{2: at}, completes: map[int]time.Time{2: at}, checkpoint: map[string]bool{"a": true, "b": true}},
		{name: "the first completes", completes: map[int]time.Time{0: at}, checkpoint: map[string]bool{"a": false, "b": true}},
		{name: "the last completes", completes: map[int]time.Time{1: at}, ack: []string{streamingQuery}, checkpoint: map[string]bool{"a": false, "b": false}},
		{name: "begins and completes at once", begins: map[int]time.Time{0: at}, completes: map[int]time.Time{0: at}, checkpoint: map[string]bool{"a": false, "b": false}},
	} {
		queries = nil
		// As RunTransactions does: BackfillsBegan only for begins, then Flush.
		if len(tc.begins) > 0 {
			require.NoError(t, d.BackfillsBegan(ctx, tc.begins), tc.name)
		}
		require.NoError(t, d.Flush(ctx, tc.completes), tc.name)
		require.Equal(t, tc.flush, queries, tc.name)

		cpJSON, err := d.buildDriverCheckpoint(ctx, nil, 0)
		require.NoError(t, err, tc.name)
		var cp checkpoint
		require.NoError(t, json.Unmarshal(cpJSON, &cp), tc.name)
		// An empty Backfilling is omitted from the checkpoint.
		var got = cp[backfillingStateKey].Backfilling
		if got == nil {
			got = map[string]bool{}
		}
		require.Equal(t, tc.checkpoint, got, tc.name)

		queries = nil
		_, err = d.Acknowledge(ctx, nil, nil)
		require.NoError(t, err, tc.name)
		require.Equal(t, tc.ack, queries, tc.name)
	}
}

func TestWarehouseSizeAfterRestart(t *testing.T) {
	var ctx = t.Context()
	var at = time.Now()

	t.Run("mid-backfill", func(t *testing.T) {
		var queries []string
		var d = sizedTransactor(&queries, map[string]bool{"a": true})

		_, err := d.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.Empty(t, queries)

		require.NoError(t, d.Flush(ctx, nil))
		require.Equal(t, []string{backfillQuery}, queries)
	})

	// The transaction that completed the backfill committed, and the connector
	// stopped before its Acknowledge finished.
	t.Run("before the resize back", func(t *testing.T) {
		var queries []string
		var d = sizedTransactor(&queries, map[string]bool{"a": false})

		_, err := d.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.Equal(t, []string{streamingQuery}, queries)
	})

	t.Run("a backfill of a binding that is gone", func(t *testing.T) {
		var queries []string
		var d = sizedTransactor(&queries, map[string]bool{"gone": true})

		require.NoError(t, d.Flush(ctx, map[int]time.Time{0: at}))
		_, err := d.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.Equal(t, []string{streamingQuery}, queries)
	})

	t.Run("an Apply leaves the warehouse alone", func(t *testing.T) {
		var queries []string
		var d = sizedTransactor(&queries, map[string]bool{})

		_, err := d.Acknowledge(ctx, nil, []string{"a"})
		require.NoError(t, err)
		require.Empty(t, queries)
	})
}

func TestWarehouseSizeUnconfigured(t *testing.T) {
	var d = &transactor{bindings: []*binding{{target: sql.Table{}}}, cp: make(checkpoint)}
	var signals = map[int]time.Time{0: time.Now()}

	require.NoError(t, d.BackfillsBegan(t.Context(), signals))
	require.NoError(t, d.Flush(t.Context(), signals))
	_, err := d.Acknowledge(t.Context(), nil, nil)
	require.NoError(t, err)
	require.Empty(t, d.cp)
}

func TestSetWarehouseSizeError(t *testing.T) {
	var queries []string
	var fail = errors.New("insufficient privileges")
	var d = &transactor{db: recordingDB(&queries, &fail), warehouse: `"WH"`}

	var err = d.setWarehouseSize(t.Context(), "X4LARGE")
	var userErr *cerrors.UserError
	require.ErrorAs(t, err, &userErr)
	require.ErrorContains(t, err, `Unable to set the size of warehouse "WH" to X4LARGE: insufficient privileges`)

	// A failed resize is tried again, and a size already set is not.
	fail = nil
	require.NoError(t, d.setWarehouseSize(t.Context(), "X4LARGE"))
	require.NoError(t, d.setWarehouseSize(t.Context(), "X4LARGE"))
	require.Equal(t, []string{backfillQuery, backfillQuery}, queries)
}
