package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/estuary/connectors/go/blob"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	enc "github.com/estuary/connectors/materialize-boilerplate/stream-encode"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/klauspost/compress/gzip"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func newDuckDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-motherduck",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		StartTunnel:      func(ctx context.Context, conf any) error { return nil },
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("could not parse endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"database": cfg.Database,
			}).Info("opening database")

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             duckDialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable([]string{cfg.Database, cfg.Schema}),
				NewClient:           newClient,
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
				ConcurrentApply:     false,
			}, nil
		},
		PreReqs: preReqs,
	}
}

type transactor struct {
	cfg *config

	fence      sql.Fence
	conn       *stdsql.Conn
	bucket     blob.Bucket
	bucketPath string

	storeFiles *boilerplate.StagedFiles
	loadFiles  *boilerplate.StagedFiles
	bindings   []*binding
	be         *boilerplate.BindingEvents
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	is *boilerplate.InfoSchema,
	be *boilerplate.BindingEvents,
) (_ m.Transactor, _ *boilerplate.MaterializeOptions, err error) {
	cfg := ep.Config.(*config)

	db, err := cfg.db(ctx)
	if err != nil {
		return nil, nil, err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("creating connection: %w", err)
	}

	bucket, bucketPath, err := cfg.toBucketAndPath(ctx)
	if err != nil {
		return nil, nil, err
	}

	t := &transactor{
		cfg:        cfg,
		conn:       conn,
		bucket:     bucket,
		bucketPath: bucketPath,
		fence:      fence,
		be:         be,
		loadFiles:  boilerplate.NewStagedFiles(stagedFileClient{}, bucket, enc.DefaultJsonFileSizeLimit, bucketPath, false, false),
		storeFiles: boilerplate.NewStagedFiles(stagedFileClient{}, bucket, enc.DefaultJsonFileSizeLimit, bucketPath, true, false),
	}

	for idx, target := range bindings {
		t.loadFiles.AddBinding(idx, target.KeyNames())
		t.storeFiles.AddBinding(idx, target.ColumnNames())
		t.bindings = append(t.bindings, &binding{
			target:           target,
			loadMergeBounds:  sql.NewMergeBoundsBuilder(target.Keys, duckDialect.Literal),
			storeMergeBounds: sql.NewMergeBoundsBuilder(target.Keys, duckDialect.Literal),
		})
	}

	return t, nil, nil
}

type binding struct {
	target           sql.Table
	mustMerge        bool
	loadMergeBounds  *sql.MergeBoundsBuilder
	storeMergeBounds *sql.MergeBoundsBuilder
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (d *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		b := d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = d.loadFiles.EncodeRow(ctx, it.Binding, converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		} else {
			b.loadMergeBounds.NextKey(converted)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	defer d.loadFiles.CleanupCurrentTransaction(ctx)

	var subqueries []string
	for idx, b := range d.bindings {
		var loadQuery strings.Builder

		if !d.loadFiles.Started(idx) {
			continue
		} else if uris, err := d.loadFiles.Flush(idx); err != nil {
			return fmt.Errorf("flushing load file: %w", err)
		} else if err := tplLoadQuery.Execute(&loadQuery, &queryParams{
			Table:  b.target,
			Bounds: b.loadMergeBounds.Build(),
			Files:  uris,
		}); err != nil {
			return fmt.Errorf("rendering load query: %w", err)
		}

		subqueries = append(subqueries, loadQuery.String())
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}
	loadAllSql := strings.Join(subqueries, "\nUNION ALL\n")

	// The results of the load query will be written to an S3 object, which
	// we'll then read directly from S3. This is done instead of running the
	// query in-process to avoid blowing out the memory usage of the connector,
	// since iterating over the returned rows directly results in the entire
	// query being materialized in-memory.
	loadResKey := path.Join(d.bucketPath, "loaded_"+uuid.NewString()+".json.gz")
	loadResURI := d.bucket.URI(loadResKey)

	// TODO(whb): In the future it may be useful to persist the load query
	// results in the driver checkpoint via the `Flushed` response and use them
	// for re-application of transactions. This will require idempotent runtime
	// transactions though.
	defer func() {
		if err := d.bucket.Delete(ctx, []string{loadResURI}); err != nil {
			log.WithError(err).Warn("failed to delete load results file")
		}
	}()

	d.be.StartedEvaluatingLoads()
	rows, err := d.conn.QueryContext(ctx, fmt.Sprintf("COPY (%s) to '%s';", loadAllSql, loadResURI))
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()
	d.be.FinishedEvaluatingLoads()

	var loadQueryCount int
	for rows.Next() {
		if err := rows.Scan(&loadQueryCount); err != nil {
			return fmt.Errorf("scanning loadQueryCount: %w", err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	// Now read these results back from the s3 file.
	r, err := d.bucket.NewReader(ctx, loadResKey)
	if err != nil {
		return fmt.Errorf("get load results object reader: %w", err)
	}

	gzr, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("get gzip reader for load results: %w", err)
	}

	type bindingDoc struct {
		Binding int
		Doc     json.RawMessage
	}

	dec := json.NewDecoder(gzr)
	loadedCount := 0
	for {
		var doc bindingDoc
		if err := dec.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("scanning loaded document from file: %w", err)
		}
		if err = loaded(doc.Binding, doc.Doc); err != nil {
			return fmt.Errorf("sending loaded document: %w", err)
		}
		loadedCount += 1
	}

	// These counts should always be equal.
	if loadedCount != loadQueryCount {
		return fmt.Errorf("mismatched loadedCount vs loadQueryCount: %d vs %d", loadedCount, loadQueryCount)
	} else if err := gzr.Close(); err != nil {
		return fmt.Errorf("closing gzip reader: %w", err)
	} else if err := r.Close(); err != nil {
		return fmt.Errorf("closing reader: %w", err)
	} else if err := d.loadFiles.CleanupCurrentTransaction(ctx); err != nil {
		return fmt.Errorf("cleaning up load files: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	for it.Next() {
		if d.cfg.HardDelete && it.Delete && !it.Exists {
			// Ignore documents which do not exist and are being deleted.
			continue
		}

		b := d.bindings[it.Binding]
		if it.Exists {
			b.mustMerge = true
		}

		flowDocument := it.RawJSON
		if d.cfg.HardDelete && it.Delete {
			flowDocument = json.RawMessage(`"delete"`)
		}

		if converted, err := b.target.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := d.storeFiles.EncodeRow(ctx, it.Binding, converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		} else {
			b.storeMergeBounds.NextKey(converted[:len(b.target.Keys)])
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var err error
		if d.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, d.fence); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("evaluating fence template: %w", err))
		}

		return nil, m.RunAsyncOperation(func() error {
			defer d.storeFiles.CleanupCurrentTransaction(ctx)

			txn, err := d.conn.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("store BeginTx: %w", err)
			}
			defer txn.Rollback()

			for idx, b := range d.bindings {
				if !d.storeFiles.Started(idx) {
					continue
				}

				uris, err := d.storeFiles.Flush(idx)
				if err != nil {
					return fmt.Errorf("flushing store file for %s: %w", b.target.Path, err)
				}

				params := &queryParams{Table: b.target, Files: uris, Bounds: b.storeMergeBounds.Build()}

				d.be.StartedResourceCommit(b.target.Path)
				if b.mustMerge {
					// In-place updates are accomplished by deleting the
					// existing row and inserting the updated row.
					var storeDeleteQuery strings.Builder
					if err := tplStoreDeleteQuery.Execute(&storeDeleteQuery, params); err != nil {
						return err
					} else if _, err := txn.ExecContext(ctx, storeDeleteQuery.String()); err != nil {
						return fmt.Errorf("executing store delete query %s: %w", b.target.Path, err)
					}

				}

				var storeQuery strings.Builder
				if err := tplStoreQuery.Execute(&storeQuery, params); err != nil {
					return err
				} else if _, err := txn.ExecContext(ctx, storeQuery.String()); err != nil {
					return fmt.Errorf("executing store query for %s: %w", b.target.Path, err)
				}
				d.be.FinishedResourceCommit(b.target.Path)

				// Reset for next round.
				b.mustMerge = false
			}

			if res, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating checkpoints: %w", err)
			} else if rows, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("getting fence update rows affected: %w", err)
			} else if rows != 1 {
				return fmt.Errorf("this instance was fenced off by another")
			} else if err := txn.Commit(); err != nil {
				return fmt.Errorf("committing store transaction: %w", err)
			} else if err := d.storeFiles.CleanupCurrentTransaction(ctx); err != nil {
				return fmt.Errorf("cleaning up store files: %w", err)
			}

			return nil
		})
	}, nil
}

func (d *transactor) Destroy() {}
