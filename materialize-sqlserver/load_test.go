package connector

import (
	"context"
	stdsql "database/sql"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	pc "go.gazette.dev/core/consumer/protocol"
)

// scriptedStream hands the transactions loop the requests a test sends, and
// reports each time the loop starts waiting for one.
type scriptedStream struct {
	requests  chan *pm.Request
	waiting   chan struct{}
	responses chan *pm.Response
}

func (s *scriptedStream) RecvMsg(out *pm.Request) error {
	s.waiting <- struct{}{}
	req, ok := <-s.requests
	if !ok {
		return io.EOF
	}
	*out = *req
	return nil
}

func (s *scriptedStream) Send(resp *pm.Response) error {
	var cp = *resp
	s.responses <- &cp
	return nil
}

// openedTransactor is a transactor that is already open.
type openedTransactor struct{ *transactor }

func (o openedTransactor) NewTransactor(_ context.Context, _ pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	o.be = be
	return o.transactor, &pm.Response_Opened{}, nil, nil
}

// TestLoadAfterConnectionLossAwaitingFirstKey verifies that Load succeeds
// when the connection is lost while it waits for the first key of a
// transaction.
func TestLoadAfterConnectionLossAwaitingFirstKey(t *testing.T) {
	startDatabase(t)

	var ctx = context.Background()
	var cfg = testConfig()
	cfg.Advanced.NoFlowDocument = false
	connector, err := cfg.ToSQLConnector(ctx)
	require.NoError(t, err)
	var db = stdsql.OpenDB(connector)
	defer db.Close()

	raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(raw))
	spec.Bindings = spec.Bindings[:1]

	table, err := sql.ResolveTable(
		sql.BuildTableShape(spec.Name.String(), spec.Bindings[0], 0, []string{"load_connection_loss"}, false),
		testDialect,
	)
	require.NoError(t, err)
	var createTable strings.Builder
	require.NoError(t, testTemplates.createTargetTable.Execute(&createTable, &table))
	_, err = db.ExecContext(ctx, createTable.String())
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `
		CREATE TABLE load_connection_loss_checkpoints (
			materialization VARCHAR(256), key_begin BIGINT, key_end BIGINT, fence BIGINT, "checkpoint" VARCHAR(MAX)
		);
		INSERT INTO load_connection_loss_checkpoints VALUES (@p1, 0, 0, 1, '');`, spec.Name.String())
	require.NoError(t, err)

	var d = &transactor{templates: testTemplates, cfg: cfg}
	d.store.fence = sql.Fence{
		TablePath:       sql.TablePath{"load_connection_loss_checkpoints"},
		Materialization: spec.Name,
		Fence:           1,
	}
	d.load.session = &session{db: db}
	require.NoError(t, d.load.session.connect(ctx, nil))
	d.store.session = &session{db: db}
	require.NoError(t, d.store.session.connect(ctx, nil))
	var spid int
	require.NoError(t, d.load.session.conn.QueryRowContext(ctx, "SELECT @@SPID;").Scan(&spid))
	var identity = func(s string) string { return s }
	var is = boilerplate.NewInfoSchema(func(p []string) []string { return p }, identity, identity, false, false)
	require.NoError(t, d.addBinding(ctx, table, is))
	d.load.unionSQL = d.bindings[0].loadQuerySQL + ";"

	var stream = &scriptedStream{
		requests:  make(chan *pm.Request, 16),
		waiting:   make(chan struct{}, 16),
		responses: make(chan *pm.Response, 16),
	}
	var open = &pm.Request_Open{
		Materialization: &spec,
		Range:           &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32},
		Version:         "v1",
	}
	var done = make(chan error, 1)
	go func() { done <- m.RunTransactions(ctx, openedTransactor{d}, stream, open, log.InfoLevel) }()

	// respond returns the loop's next response, or fails if the loop exits.
	var respond = func() *pm.Response {
		select {
		case resp := <-stream.responses:
			return resp
		case err := <-done:
			require.NoError(t, err)
			require.FailNow(t, "the transactions loop exited")
		case <-time.After(time.Minute):
			require.FailNow(t, "timed out waiting for a response")
		}
		return nil
	}

	require.NotNil(t, respond().Opened)
	<-stream.waiting
	stream.requests <- &pm.Request{Acknowledge: &pm.Request_Acknowledge{}}
	require.NotNil(t, respond().Acknowledged)

	// Load is now waiting for its first key.
	<-stream.waiting
	_, err = db.ExecContext(ctx, "KILL "+strconv.Itoa(spid)+";")
	require.NoError(t, err)

	var key = tuple.Tuple{"a-key"}
	stream.requests <- &pm.Request{Load: &pm.Request_Load{KeyPacked: key.Pack()}}
	stream.requests <- &pm.Request{Flush: &pm.Request_Flush{}}
	require.NotNil(t, respond().Flushed)

	stream.requests <- &pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}}
	require.NotNil(t, respond().StartedCommit)
	stream.requests <- &pm.Request{Acknowledge: &pm.Request_Acknowledge{}}
	require.NotNil(t, respond().Acknowledged)

	close(stream.requests)
	require.NoError(t, <-done)
}
