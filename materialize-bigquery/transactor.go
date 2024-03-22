package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/bigquery"
	m "github.com/estuary/connectors/go/protocols/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"google.golang.org/api/iterator"
)

var _ m.DelayedCommitter = (*transactor)(nil)

type transactor struct {
	client     *client
	bucketPath string
	bucket     string

	cp checkpoint

	bindings    []*binding
	updateDelay time.Duration
}

type checkpointItem struct {
	Table  string
	Query  string
	EDC    *bigquery.ExternalDataConfig
	Bucket string
	Files  []string
}

type checkpoint = map[string]*checkpointItem

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	_ sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
) (_ m.Transactor, err error) {
	cfg := ep.Config.(*config)

	client, err := cfg.client(ctx)
	if err != nil {
		return nil, err
	}

	t := &transactor{
		client:     client,
		bucketPath: cfg.BucketPath,
		bucket:     cfg.Bucket,
	}

	if t.updateDelay, err = m.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
		return nil, err
	}

	for _, binding := range bindings {
		// The name of the table itself is always the last element of the path.
		table := binding.TableShape.Path[len(binding.TableShape.Path)-1]

		// The dataset for the table is always the second to last element of the path. Dataset names
		// can only contain letters, numbers, and underscores, so translateFlowIdentifier is not
		// needed when using this part of the path directly.
		dataset := binding.TableShape.Path[len(binding.TableShape.Path)-2]

		// Lookup metadata for the table to build the schema for the external file that will be used
		// for loading data. Schema definitions from the actual table columns are queried instead of
		// directly using the dialect's output for JSON schema type to provide some degree in
		// flexibility in changing the dialect and having it still work for existing tables. As long
		// as the JSON encoding of the values is the same they may be used for columns that would
		// have been created differently due to evolution of the dialect's column types.
		meta, err := client.bigqueryClient.DatasetInProject(cfg.ProjectID, dataset).Table(translateFlowIdentifier(table)).Metadata(ctx)
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

	for idx, col := range cols {
		schema, ok := fieldSchemas[translateFlowIdentifier(col.Field)]
		if !ok {
			return nil, fmt.Errorf("could not find metadata for field '%s'", col.Field)
		}

		// Use a placeholder value instead of the actual field name for the external table schema.
		// This allows for materialized tables to use "Flexible column names", which is not yet
		// supported by external tables. A similar placeholder is used in the generated SQL queries
		// to match this.
		schema.Name = fmt.Sprintf("c%d", idx)

		s = append(s, schema)
	}

	return s, nil
}

func (t *transactor) AckDelay() time.Duration {
	return t.updateDelay
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	log.Info("load: starting encoding and uploading of files")
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
	log.Info("load: finished encoding and uploading files")

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

	log.Info("load: finished loading")

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()

	log.Info("store: starting encoding and uploading of files")

	// Build the slice of transactions required for a commit.
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

	for _, b := range t.bindings {
		if !b.storeFile.started {
			// No stores for this binding.
			continue
		}

		toCleanup, err := b.storeFile.flush(ctx)
		if err != nil {
			return fmt.Errorf("flushing store file for binding[%d]: %w", idx, err)
		}

		t.cp[b.target.StateKey] = &checkpointItem{
			Table:  b.target.Identifier,
			Query:  b.storeUpdateSQL,
			EDC:    b.storeFile.edc(),
			Bucket: b.storeFile.bucket,
			Files:  toCleanup,
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var checkpointJSON, err = json.Marshal(t.cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON, MergePatch: true}, nil
	}, nil
}

func (d *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &d.cp); err != nil {
		return err
	}

	return nil
}

func (d *transactor) hasStateKey(stateKey string) bool {
	for _, b := range d.bindings {
		if b.target.StateKey == stateKey {
			return true
		}
	}

	return false
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	log.Info("store: starting committing changes")

	for stateKey, item := range t.cp {
		// we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if !t.hasStateKey(stateKey) {
			continue
		}

		var query = t.client.newQuery(item.Query)
		query.TableDefinitions = item.EDC

		log.WithField("table", item.Table).Info("store: starting query")
		if _, err := t.client.runQuery(ctx, query); err != nil {
			return nil, fmt.Errorf("query %q failed: %w", item.Query, err)
		}
		log.WithField("table", item.Table).Info("store: finished query")

		for _, f := range item.Files {
			deleteFile(ctx, t.client.cloudStorageClient, item.Bucket, item.Files)
		}
	}

	log.Info("store: finished commit")

	return nil
}

func (t *transactor) Destroy() {
	_ = t.client.bigqueryClient.Close()
	_ = t.client.cloudStorageClient.Close()
}
