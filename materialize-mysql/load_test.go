package connector

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"io"
	"math"
	"os"
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

type newTransactorFn func(context.Context, pm.Request_Open, *m.BindingEvents) (m.Transactor, error)

func (f newTransactorFn) NewTransactor(ctx context.Context, open pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	t, err := f(ctx, open, be)
	return t, &pm.Response_Opened{}, nil, err
}

// TestLoadAfterConnectionLoss verifies that Load finds a key longer than the
// load table's initial VARCHAR width when the connection is lost while Load
// waits for the first key of a transaction.
func TestLoadAfterConnectionLoss(t *testing.T) {
	startDatabase(t)

	for _, variant := range variants {
		t.Run(variant, func(t *testing.T) {
			var ctx = context.Background()
			var cfg = variantConfig(t, variant)
			cfg.Advanced.NoFlowDocument = false
			ep, err := NewDriver().NewEndpoint(ctx, cfg, nil)
			require.NoError(t, err)
			db, err := stdsql.Open("mysql", cfg.ToURI())
			require.NoError(t, err)
			defer db.Close()

			raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
			require.NoError(t, err)
			var spec pf.MaterializationSpec
			require.NoError(t, spec.Unmarshal(raw))
			spec.Bindings = spec.Bindings[:1]

			table, err := sql.ResolveTable(
				sql.BuildTableShape(spec.Name.String(), spec.Bindings[0], 0, []string{"load_connection_loss"}, false),
				ep.Dialect,
			)
			require.NoError(t, err)
			for _, name := range []string{"load_connection_loss", "load_connection_loss_checkpoints"} {
				_, err = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+name+";")
				require.NoError(t, err)
			}
			var createTable strings.Builder
			require.NoError(t, ep.CreateTableTemplate.Execute(&createTable, &table))
			_, err = db.ExecContext(ctx, createTable.String())
			require.NoError(t, err)

			// Store widens a VARCHAR key column for a long key, as it would
			// have before this key could be loaded. SingleStore keys are TEXT.
			var longKey = strings.Repeat("k", 300)
			if key := table.Keys[0]; strings.Contains(key.DDL, "VARCHAR(256)") {
				_, err = db.ExecContext(ctx, fmt.Sprintf(varcharTableAlter, table.Identifier, key.Identifier, len(longKey)))
				require.NoError(t, err)
			}
			var doc = `{"key":"` + longKey + `"}`
			var cols, args []string
			for _, col := range table.Columns() {
				if v, ok := map[string]string{
					"key":               longKey,
					"requiredString":    "s",
					"requiredInteger":   "1",
					"requiredBoolean":   "1",
					"requiredObject":    "{}",
					"flow_published_at": "2026-01-01 00:00:00",
					"flow_document":     doc,
				}[col.Field]; ok {
					cols = append(cols, col.Identifier)
					args = append(args, ep.Dialect.Literal(v))
				}
			}
			_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);",
				table.Identifier, strings.Join(cols, ", "), strings.Join(args, ", ")))
			require.NoError(t, err)

			_, err = db.ExecContext(ctx, `CREATE TABLE load_connection_loss_checkpoints (
				materialization VARCHAR(256), key_begin BIGINT, key_end BIGINT, fence BIGINT, checkpoint LONGTEXT
			);`)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, "INSERT INTO load_connection_loss_checkpoints VALUES ("+
				ep.Dialect.Literal(spec.Name.String())+", 0, 0, 1, '');")
			require.NoError(t, err)
			var fence = sql.Fence{
				TablePath:       sql.TablePath{"load_connection_loss_checkpoints"},
				Materialization: spec.Name,
				Fence:           1,
			}

			client, err := ep.NewClient(ctx, spec.Name.String(), ep)
			require.NoError(t, err)
			defer client.Close()
			var is = boilerplate.InitInfoSchema(boilerplate.MaterializeCfg{
				Locate:                   sql.ToLocatePathFn(ep.Dialect.TableLocator),
				TranslateNamespace:       ep.Dialect.SchemaLocator,
				TranslateField:           ep.Dialect.ColumnLocator,
				CaseInsensitiveFields:    ep.Dialect.CaseInsensitiveColumns,
				CaseInsensitiveResources: ep.Dialect.CaseInsensitiveResources,
			})
			require.NoError(t, client.PopulateInfoSchema(ctx, is, [][]string{table.Path}))

			var loadConn *stdsql.Conn
			var newTransactor = newTransactorFn(func(ctx context.Context, open pm.Request_Open, be *m.BindingEvents) (m.Transactor, error) {
				tr, err := ep.NewTransactor(ctx, spec.Name.String(), nil, ep, fence, []sql.Table{table}, open, is, be)
				if err == nil {
					loadConn = tr.(*transactor).load.session.conn
				}
				return tr, err
			})

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
			go func() { done <- m.RunTransactions(ctx, newTransactor, stream, open, log.InfoLevel) }()

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
			// send hands the loop req once it waits for a request.
			var send = func(req *pm.Request) {
				<-stream.waiting
				stream.requests <- req
			}
			var load = &pm.Request{Load: &pm.Request_Load{KeyPacked: tuple.Tuple{longKey}.Pack()}}
			// commit finishes a transaction whose load is sent, and checks
			// that the load found the document.
			var commit = func() {
				send(&pm.Request{Flush: &pm.Request_Flush{}})
				var loaded = respond().Loaded
				require.NotNil(t, loaded)
				require.JSONEq(t, doc, string(loaded.DocJson))
				require.NotNil(t, respond().Flushed)
				send(&pm.Request{StartCommit: &pm.Request_StartCommit{RuntimeCheckpoint: &pc.Checkpoint{}}})
				require.NotNil(t, respond().StartedCommit)
				send(&pm.Request{Acknowledge: &pm.Request_Acknowledge{}})
				require.NotNil(t, respond().Acknowledged)
			}

			require.NotNil(t, respond().Opened)
			send(&pm.Request{Acknowledge: &pm.Request_Acknowledge{}})
			require.NotNil(t, respond().Acknowledged)

			send(load)
			commit()

			// Load is now waiting for its first key.
			<-stream.waiting
			killConnection(t, ctx, db, loadConn)
			stream.requests <- load
			commit()

			close(stream.requests)
			require.NoError(t, <-done)
		})
	}
}
