package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"cloud.google.com/go/bigquery"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql-v2"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"google.golang.org/api/iterator"
)

type transactor struct {
	fence             *sql.Fence
	cfg               config
	dialect           sql.Dialect
	templates         templates
	objAndArrayAsJson bool

	client     *client
	bucketPath string
	bucket     string

	bindings []*binding
	be       *boilerplate.BindingEvents

	loggedStorageApiMessage bool
}

func prepareNewTransactor(
	dialect sql.Dialect,
	templates templates,
	objAndArrayAsJson bool,
) func(context.Context, *sql.Endpoint[config], sql.Fence, []sql.Table, pm.Request_Open, *boilerplate.InfoSchema, *boilerplate.BindingEvents) (m.Transactor, error) {
	return func(
		ctx context.Context,
		ep *sql.Endpoint[config],
		fence sql.Fence,
		bindings []sql.Table,
		open pm.Request_Open,
		is *boilerplate.InfoSchema,
		be *boilerplate.BindingEvents,
	) (m.Transactor, error) {
		client, err := ep.Config.client(ctx, ep)
		if err != nil {
			return nil, err
		}

		t := &transactor{
			cfg:               ep.Config,
			fence:             &fence,
			dialect:           dialect,
			templates:         templates,
			objAndArrayAsJson: objAndArrayAsJson,
			client:            client,
			bucketPath:        ep.Config.BucketPath,
			bucket:            ep.Config.Bucket,
			be:                be,
		}

		for _, binding := range bindings {
			// Lookup metadata for the table to build the schema for the external file that will be used
			// for loading data. Schema definitions from the actual table columns are used instead of
			// directly using the dialect's output for JSON schema type to provide some degree in
			// flexibility in changing the dialect and having it still work for existing tables. As long
			// as the JSON encoding of the values is the same they may be used for columns that would
			// have been created differently due to evolution of the dialect's column types.
			res := is.GetResource(binding.Path)
			if res == nil {
				return nil, fmt.Errorf("could not get metadata for table %s: verify that the table exists and that the connector service account user is authorized for it", binding.Identifier)
			}

			schema := res.Meta.(bigquery.Schema)

			log.WithFields(log.Fields{
				"table":      binding.Path,
				"collection": binding.Source.String(),
				"schemaJson": schema,
			}).Debug("bigquery schema for table")

			fieldSchemas := make(map[string]*bigquery.FieldSchema)
			for _, f := range schema {
				fieldSchemas[f.Name] = f
			}

			if err = t.addBinding(binding, fieldSchemas, &ep.Dialect); err != nil {
				return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
			}
		}

		return t, nil
	}
}

func (t *transactor) addBinding(target sql.Table, fieldSchemas map[string]*bigquery.FieldSchema, dialect *sql.Dialect) error {
	loadSchema, err := schemaForCols(target.KeyPtrs(), fieldSchemas)
	if err != nil {
		return err
	}

	storeSchema, err := schemaForCols(target.Columns(), fieldSchemas)
	if err != nil {
		return err
	}

	b := &binding{
		target:           target,
		loadFile:         newStagedFile(t.client.cloudStorageClient, t.bucket, t.bucketPath, loadSchema),
		storeFile:        newStagedFile(t.client.cloudStorageClient, t.bucket, t.bucketPath, storeSchema),
		loadMergeBounds:  sql.NewMergeBoundsBuilder(target.Keys, dialect.Literal),
		storeMergeBounds: sql.NewMergeBoundsBuilder(target.Keys, dialect.Literal),
	}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.tempTableName, t.templates.tempTableName},
		{&b.storeInsertSQL, t.templates.storeInsert},
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

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = t.bindings[it.Binding]
		b.loadFile.start()

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting load key: %w", err)
		} else if err = b.loadFile.encodeRow(ctx, converted); err != nil {
			return fmt.Errorf("writing normalized key to keyfile: %w", err)
		} else {
			b.loadMergeBounds.NextKey(converted)
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

		loadQuery, err := renderQueryTemplate(b.target, t.templates.loadQuery, b.loadMergeBounds.Build(), t.objAndArrayAsJson)
		if err != nil {
			return fmt.Errorf("rendering load query template: %w", err)
		}
		subqueries = append(subqueries, loadQuery)

		delete, err := b.loadFile.flush()
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
	queryStr := strings.Join(subqueries, "\nUNION ALL\n") + ";"
	query := t.client.newQuery(queryStr)
	query.TableDefinitions = edcTableDefs // Tell bigquery where to get the external references in gcs.
	ll := log.WithField("query", queryStr)

	t.be.StartedEvaluatingLoads()
	job, err := t.client.runQuery(ctx, query)
	if err != nil {
		ll.WithError(err).Error("client runQuery failed")
		return fmt.Errorf("load query: %w", err)
	}

	bqit, err := job.Read(ctx)
	if err != nil {
		ll.WithError(err).Error("job read failed")
		return fmt.Errorf("load job read: %w", err)
	}
	t.be.FinishedEvaluatingLoads()

	if !bqit.IsAccelerated() && !t.loggedStorageApiMessage {
		log.Warn("not using the storage read API for load queries, performance may not be optimal. see https://go.estuary.dev/materialize-bigquery for more information")
		t.loggedStorageApiMessage = true
	}

	for {
		var bd bindingDocument
		if err = bqit.Next(&bd); err == iterator.Done {
			break
		} else if err != nil {
			ll.WithError(err).Error("query results iterator failed")
			return fmt.Errorf("load row read: %w", err)
		}

		if err = loaded(bd.Binding, bd.Document); err != nil {
			return fmt.Errorf("load row loaded: %w", err)
		}
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()

	cleanupFiles := []func(context.Context){}

	var lastBinding = -1
	for it.Next() {
		if lastBinding == -1 {
			lastBinding = it.Binding
		}

		if lastBinding != it.Binding {
			// Flush the staged file(s) for the binding now that it's stores are
			// fully processed.
			var b = t.bindings[lastBinding]
			// There may be no staged file if the binding has received nothing
			// but hard deletion requests for keys that aren't in the
			// destination table.
			if b.storeFile.started {
				cleanupFn, err := b.storeFile.flush()
				if err != nil {
					return nil, fmt.Errorf("flushing staged files for collection %q: %w", b.target.Source.String(), err)
				}
				cleanupFiles = append(cleanupFiles, cleanupFn)
			}
			lastBinding = it.Binding
		}

		var b = t.bindings[it.Binding]

		if it.Exists {
			b.mustMerge = true
		}

		var flowDocument = it.RawJSON
		if t.cfg.HardDelete && it.Delete {
			if it.Exists {
				flowDocument = json.RawMessage(`"delete"`)
			} else {
				// Ignore items which do not exist and are already deleted
				continue
			}
		}

		b.storeFile.start()
		b.hasData = true
		if converted, err := b.target.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err = b.storeFile.encodeRow(ctx, converted); err != nil {
			return nil, fmt.Errorf("encoding Store to scratch file: %w", err)
		} else {
			b.storeMergeBounds.NextKey(converted[:len(b.target.Keys)])
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	// Flush the final binding.
	if lastBinding != -1 {
		var b = t.bindings[lastBinding]
		cleanupFn, err := b.storeFile.flush()
		if err != nil {
			return nil, fmt.Errorf("final binding flushing staged files for collection %q: %w", b.target.Source.String(), err)
		}
		cleanupFiles = append(cleanupFiles, cleanupFn)
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var err error
		if t.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		return nil, pf.RunAsyncOperation(func() error { return t.commit(ctx, cleanupFiles) })
	}, nil
}

func (t *transactor) commit(ctx context.Context, cleanupFiles []func(context.Context)) error {
	defer func() {
		for _, f := range cleanupFiles {
			f(ctx)
		}
	}()

	// Build the slice of transactions required for a commit.
	var subqueries []string

	subqueries = append(subqueries, `
	BEGIN
	BEGIN TRANSACTION;
	`)

	// First we must validate the fence has not been modified.
	var fenceUpdate strings.Builder
	if err := t.templates.updateFence.Execute(&fenceUpdate, t.fence); err != nil {
		return fmt.Errorf("evaluating fence template: %w", err)
	}
	subqueries = append(subqueries, fenceUpdate.String())

	// This is the map of external table references we will populate. Loop through the bindings and
	// append the SQL for that table.
	var edcTableDefs = make(map[string]bigquery.ExternalData)
	for _, b := range t.bindings {
		if !b.hasData {
			// No stores for this binding.
			continue
		}

		edcTableDefs[b.tempTableName] = b.storeFile.edc()

		if !b.mustMerge {
			subqueries = append(subqueries, b.storeInsertSQL)
		} else {
			mergeQuery, err := renderQueryTemplate(b.target, t.templates.storeUpdate, b.storeMergeBounds.Build(), t.objAndArrayAsJson)
			if err != nil {
				return fmt.Errorf("rendering merge query template: %w", err)
			}
			subqueries = append(subqueries, mergeQuery)
		}

		// Reset for the next round.
		b.hasData = false
		b.mustMerge = false
	}

	// Complete the transaction and return the appropriate error.
	subqueries = append(subqueries, `
	COMMIT TRANSACTION;
    END;
	`)

	// Build the bigquery query of the combined subqueries.
	queryString := strings.Join(subqueries, "\n")
	query := t.client.newQuery(queryString)
	query.TableDefinitions = edcTableDefs // Tell the query where to get the external references in gcs.

	// This returns a single row with the error status of the query.
	if _, err := t.client.runQuery(ctx, query); err != nil {
		log.WithField("query", queryString).Error("query failed")
		return fmt.Errorf("commit query: %w", err)
	}

	return nil
}

func (t *transactor) Destroy() {
	_ = t.client.bigqueryClient.Close()
	_ = t.client.cloudStorageClient.Close()
}
