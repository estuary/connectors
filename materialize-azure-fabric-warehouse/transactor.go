package main

import (
	"context"
	"fmt"
	"strings"

	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/segmentio/encoding/json"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
)

type binding struct {
	target           sql.Table
	mustMerge        bool
	hasBinaryColumns bool
}

type transactor struct {
	cfg *config

	fence sql.Fence

	storeFiles *boilerplate.StagedFiles[azureBlobObject]
	loadFiles  *boilerplate.StagedFiles[azureBlobObject]
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
) (m.Transactor, *boilerplate.MaterializeOptions, error) {
	cfg := ep.Config.(*config)

	azClient, err := cfg.storageClient()
	if err != nil {
		return nil, nil, fmt.Errorf("creating storage client: %w", err)
	}

	storageClient := newFileClient(azClient, cfg.ContainerName, cfg.Directory)

	t := &transactor{
		cfg:        cfg,
		fence:      fence,
		be:         be,
		loadFiles:  boilerplate.NewStagedFiles(storageClient, fileSizeLimit, false),
		storeFiles: boilerplate.NewStagedFiles(storageClient, fileSizeLimit, true),
	}

	for idx, b := range bindings {
		t.loadFiles.AddBinding(idx, b.KeyNames())
		t.storeFiles.AddBinding(idx, b.ColumnNames())

		hasBinaryColumns := false
		for _, col := range b.Columns() {
			if col.DDL == "VARBINARY(MAX)" {
				hasBinaryColumns = true
			}
		}

		t.bindings = append(t.bindings, &binding{
			target:           b,
			hasBinaryColumns: hasBinaryColumns,
		})
	}

	opts := &boilerplate.MaterializeOptions{
		ExtendedLogging: true,
		AckSchedule: &boilerplate.AckScheduleOption{
			Config: cfg.Schedule,
			Jitter: []byte(cfg.ConnectionString),
		},
		DBTJobTrigger: &cfg.DBTJobTrigger,
	}

	return t, opts, nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	hadLoads := false
	for it.Next() {
		hadLoads = true
		b := t.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = t.loadFiles.EncodeRow(ctx, it.Binding, converted); err != nil {
			return fmt.Errorf("encoding Load key: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	if !hadLoads {
		return nil
	}

	defer t.loadFiles.CleanupCurrentTransaction(ctx)

	t.be.StartedEvaluatingLoads()
	db, err := t.cfg.db()
	if err != nil {
		return fmt.Errorf("creating db: %w", err)
	}
	defer db.Close()

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("load BeginTx: %w", err)
	}
	defer txn.Rollback()

	var unionQueries []string
	var dropQueries []string
	for idx, b := range t.bindings {
		if !t.loadFiles.Started(idx) {
			continue
		}

		uris, err := t.loadFiles.Flush(idx)
		if err != nil {
			return fmt.Errorf("flushing store file: %w", err)
		}

		var createQuery strings.Builder
		if err := tplCreateLoadTable.Execute(&createQuery, queryParams{
			Table:             b.target,
			URIs:              uris,
			StorageAccountKey: t.cfg.StorageAccountKey,
		}); err != nil {
			return fmt.Errorf("rendering create load table: %w", err)
		} else if _, err := txn.ExecContext(ctx, createQuery.String()); err != nil {
			log.WithField(
				"query", redactedQuery(createQuery, t.cfg.StorageAccountKey),
			).Error("create load table query failed")
			return fmt.Errorf("creating load table: %w", err)
		}

		loadQuery, err := sql.RenderTableTemplate(b.target, tplLoadQuery)
		if err != nil {
			return fmt.Errorf("rendering load query: %w", err)
		}

		unionQueries = append(unionQueries, loadQuery)

		dropQuery, err := sql.RenderTableTemplate(b.target, tplDropLoadTable)
		if err != nil {
			return fmt.Errorf("rendering drop load table: %w", err)
		}
		dropQueries = append(dropQueries, dropQuery)
	}

	q := strings.Join(unionQueries, "\nUNION ALL\n")
	rows, err := txn.QueryContext(ctx, q)
	if err != nil {
		log.WithField("query", q).Error("load query failed")
		return fmt.Errorf("querying load documents: %w", err)
	}
	defer rows.Close()
	t.be.FinishedEvaluatingLoads()

	for rows.Next() {
		var binding int
		var document string

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return err
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	for _, q := range dropQueries {
		if _, err := txn.ExecContext(ctx, q); err != nil {
			log.WithField("query", q).Error("drop load table query failed")
			return fmt.Errorf("dropping load table: %w", err)
		}
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("closing connection: %w", err)
	} else if err := db.Close(); err != nil {
		return fmt.Errorf("closing db: %w", err)
	} else if err := t.loadFiles.CleanupCurrentTransaction(ctx); err != nil {
		return fmt.Errorf("cleaning up temporary object files: %w", err)
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	ctx := it.Context()

	for it.Next() {
		if t.cfg.HardDelete && it.Delete && !it.Exists {
			continue
		}

		b := t.bindings[it.Binding]
		if it.Exists {
			b.mustMerge = true
		}

		flowDocument := it.RawJSON
		if t.cfg.HardDelete && it.Delete {
			flowDocument = json.RawMessage(`"delete"`)
		}

		if converted, err := b.target.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err := t.storeFiles.EncodeRow(ctx, it.Binding, converted); err != nil {
			return nil, fmt.Errorf("encoding row for store: %w", err)
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var err error
		if t.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		var fenceUpdate strings.Builder
		if err := tplUpdateFence.Execute(&fenceUpdate, t.fence); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("evaluating fence template: %w", err))
		}

		return nil, m.RunAsyncOperation(func() error {
			defer t.storeFiles.CleanupCurrentTransaction(ctx)

			db, err := t.cfg.db()
			if err != nil {
				return fmt.Errorf("creating db: %w", err)
			}
			defer db.Close()

			txn, err := db.BeginTx(ctx, nil)
			if err != nil {
				return fmt.Errorf("store BeginTx: %w", err)
			}
			defer txn.Rollback()

			for idx, b := range t.bindings {
				if !t.storeFiles.Started(idx) {
					continue
				}

				uris, err := t.storeFiles.Flush(idx)
				if err != nil {
					return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
				}

				params := &queryParams{
					Table:             b.target,
					URIs:              uris,
					StorageAccountKey: t.cfg.StorageAccountKey,
				}

				t.be.StartedResourceCommit(b.target.Path)
				if b.mustMerge {
					var mergeQuery strings.Builder
					if err := tplStoreMergeQuery.Execute(&mergeQuery, params); err != nil {
						return err
					} else if _, err := txn.ExecContext(ctx, mergeQuery.String()); err != nil {
						log.WithField(
							"query", redactedQuery(mergeQuery, t.cfg.StorageAccountKey),
						).Error("merge query failed")
						return fmt.Errorf("executing store merge query for binding[%d]: %w", idx, err)
					}
				} else {
					var copyIntoQuery strings.Builder
					tpl := tplStoreCopyIntoDirectQuery
					if b.hasBinaryColumns {
						tpl = tplStoreCopyIntoFromStagedQuery
					}

					if err := tpl.Execute(&copyIntoQuery, params); err != nil {
						return err
					} else if _, err := txn.ExecContext(ctx, copyIntoQuery.String()); err != nil {
						log.WithField(
							"query", redactedQuery(copyIntoQuery, t.cfg.StorageAccountKey),
						).Error("copy into query failed")
						return fmt.Errorf("executing store copy into query for binding[%d]: %w", idx, err)
					}
				}
				t.be.FinishedResourceCommit(b.target.Path)
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
			} else if err := t.storeFiles.CleanupCurrentTransaction(ctx); err != nil {
				return fmt.Errorf("cleaning up temporary object files: %w", err)
			}

			return nil
		})
	}, nil
}

func (t *transactor) Destroy() {}

func redactedQuery(query strings.Builder, storageAccountKey string) string {
	return strings.ReplaceAll(query.String(), storageAccountKey, "REDACTED")
}
