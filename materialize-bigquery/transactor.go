package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"google.golang.org/api/iterator"
)

type transactor struct {
	fence *sql.Fence

	client     *client
	bucketPath string
	bucket     string

	bindings    []*binding
	updateDelay time.Duration
	round       int
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	cfg := ep.Config.(*config)

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	t := &transactor{
		fence:      &fence,
		client:     client,
		bucketPath: cfg.BucketPath,
		bucket:     cfg.Bucket,
	}

	if t.updateDelay, err = sql.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
		return nil, err
	}

	for _, binding := range bindings {
		// The name of the table itself is always the last element of the path.
		table := binding.TableShape.Path[len(binding.TableShape.Path)-1]

		// Lookup metadata for the table to build the schema for the external file that will be used
		// for loading data. Schema definitions from the actual table columns are queried instead of
		// directly using the dialect's output for JSON schema type to provide some degree in
		// flexibility in changing the dialect and having it still work for existing tables. As long
		// as the JSON encoding of the values is the same they may be used for columns that would
		// have been created differently due to evolution of the dialect's column types.
		meta, err := client.bigqueryClient.DatasetInProject(cfg.ProjectID, cfg.Dataset).Table(translateFlowIdentifier(table)).Metadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting table metadata: %w", err)
		}

		log.WithFields(log.Fields{
			"table":      table,
			"collection": binding.Source.String(),
			"schemaJson": meta.Schema,
		}).Debug("bigquery schema for table")

		fieldSchemas := make(map[string]*bigquery.FieldSchema)
		for _, f := range meta.Schema {
			fieldSchemas[f.Name] = f
		}

		if err = t.addBinding(ctx, binding, fieldSchemas); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	return t, nil
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, fieldSchemas map[string]*bigquery.FieldSchema) error {
	loadSchema, err := schemaForCols(target.KeyPtrs(), fieldSchemas)
	if err != nil {
		return err
	}

	storeSchema, err := schemaForCols(target.Columns(), fieldSchemas)
	if err != nil {
		return err
	}

	b := &binding{
		target:    target,
		loadFile:  newStagedFile(t.client.cloudStorageClient, t.bucket, t.bucketPath, loadSchema),
		storeFile: newStagedFile(t.client.cloudStorageClient, t.bucket, t.bucketPath, storeSchema),
	}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.tempTableName, tplTempTableName},
		{&b.loadQuerySQL, tplLoadQuery},
		{&b.storeInsertSQL, tplStoreInsert},
		{&b.storeUpdateSQL, tplStoreUpdate},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func schemaForCols(cols []*sql.Column, fieldSchemas map[string]*bigquery.FieldSchema) ([]*bigquery.FieldSchema, error) {
	s := make([]*bigquery.FieldSchema, 0, len(cols))

	for _, col := range cols {
		schema, ok := fieldSchemas[translateFlowIdentifier(col.Field)]
		if !ok {
			return nil, fmt.Errorf("could not find metadata for field '%s'", col.Field)
		}
		s = append(s, schema)
	}

	return s, nil
}

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = t.bindings[it.Binding]
		b.loadFile.start()

		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting load key: %w", err)
		}

		if err = b.loadFile.encodeRow(ctx, converted); err != nil {
			return fmt.Errorf("writing normalized key to keyfile: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	// Build the queries of all documents across all bindings that were requested.
	var subqueries []string
	// This is the map of external table references we will populate.
	var edcTableDefs = make(map[string]bigquery.ExternalData)

	for idx, b := range t.bindings {
		if !b.loadFile.started {
			// No loads for this binding.
			continue
		}

		subqueries = append(subqueries, b.loadQuerySQL)

		delete, err := b.loadFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing load file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		edcTableDefs[b.tempTableName] = b.loadFile.edc()
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}

	// Build the query across all tables.
	query := t.client.newQuery(strings.Join(subqueries, "\nUNION ALL\n") + ";")
	query.TableDefinitions = edcTableDefs // Tell bigquery where to get the external references in gcs.

	job, err := t.client.runQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("load query: %w", err)
	}

	bqit, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("load job read: %w", err)
	}

	for {
		var bd bindingDocument
		if err = bqit.Next(&bd); err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("load row read: %w", err)
		}

		if err = loaded(bd.Binding, bd.Document); err != nil {
			return fmt.Errorf("load row loaded: %w", err)
		}
	}

	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var ctx = it.Context()
	t.round++

	for it.Next() {
		var b = t.bindings[it.Binding]
		b.storeFile.start()

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		if err = b.storeFile.encodeRow(ctx, converted); err != nil {
			return nil, fmt.Errorf("encoding Store to scratch file: %w", err)
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var err error
		if t.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		return nil, sql.CommitWithDelay(ctx, t.round, t.updateDelay, it.Total, t.commit)
	}, nil
}

func (t *transactor) commit(ctx context.Context) error {
	// Build the slice of transactions required for a commit.
	var subqueries []string

	subqueries = append(subqueries, `
	BEGIN
	BEGIN TRANSACTION;
	`)

	// First we must validate the fence has not been modified.
	var fenceUpdate strings.Builder
	if err := tplUpdateFence.Execute(&fenceUpdate, t.fence); err != nil {
		return fmt.Errorf("evaluating fence template: %w", err)
	}
	subqueries = append(subqueries, fenceUpdate.String())

	// This is the map of external table references we will populate. Loop through the bindings and
	// append the SQL for that table.
	var edcTableDefs = make(map[string]bigquery.ExternalData)
	for idx, b := range t.bindings {
		if !b.storeFile.started {
			// No stores for this binding.
			continue
		}

		delete, err := b.storeFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}
		defer delete(ctx)

		edcTableDefs[b.tempTableName] = b.storeFile.edc()

		if b.target.DeltaUpdates {
			subqueries = append(subqueries, b.storeInsertSQL)
		} else {
			subqueries = append(subqueries, b.storeUpdateSQL)
		}
	}

	// Complete the transaction and return the appropriate error.
	subqueries = append(subqueries, `
	COMMIT TRANSACTION;
    END;
	`)

	// Build the bigquery query of the combined subqueries.
	query := t.client.newQuery(strings.Join(subqueries, "\n"))
	query.TableDefinitions = edcTableDefs // Tell the query where to get the external references in gcs.

	// This returns a single row with the error status of the query.
	job, err := t.client.runQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("commit query: %w", err)
	}

	if err != nil {
		log.WithFields(log.Fields{
			"job":   job,
			"error": err,
		}).Error("Bigquery job failed")
		return fmt.Errorf("merge error: %s", err)
	} else {
		return nil
	}
}

func (t *transactor) Acknowledge(context.Context) error {
	return nil
}

func (t *transactor) Destroy() {
	_ = t.client.bigqueryClient.Close()
	_ = t.client.cloudStorageClient.Close()
}
