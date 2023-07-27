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

const (
	// BigQuery imposes a limit of 1500 table operations per day.
	transactionDelay = 2 * time.Minute
)

type transactor struct {
	fence *sql.Fence

	client     *client
	bucketPath string
	bucket     string

	bindings []*binding
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

	for _, binding := range bindings {
		// The name of the table itself is always the last element of the path.
		table := binding.TableShape.Path[len(binding.TableShape.Path)-1]

		// Lookup metadata for the table to build the schema for the external file that will be used
		// for loading data. Schema definitions from the actual table columns are queried instead of
		// directly using the dialect's output for JSON schema type to provide some degree in
		// flexibility in changing the dialect and having it still work for existing tables. As long
		// as the JSON encoding of the values is the same they may be used for columns that would
		// have been created differently due to evolution of the dialect's column types.
		meta, err := client.bigqueryClient.Dataset(cfg.Dataset).Table(translateFlowIdentifier(table)).Metadata(ctx)
		if err != nil {
			return nil, fmt.Errorf("getting table metadata: %w", err)
		}

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
	var b = &binding{target: target}

	var err error
	if b.loadExtDataConfig, err = configForCols(target.KeyPtrs(), fieldSchemas); err != nil {
		return err
	}
	if b.storeExtDataConfig, err = configForCols(target.Columns(), fieldSchemas); err != nil {
		return err
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

func configForCols(cols []*sql.Column, fieldSchemas map[string]*bigquery.FieldSchema) (*bigquery.ExternalDataConfig, error) {
	config := &bigquery.ExternalDataConfig{
		SourceFormat: bigquery.JSON,
		Schema:       make([]*bigquery.FieldSchema, 0, len(cols)),
	}

	for _, col := range cols {
		schema, ok := fieldSchemas[translateFlowIdentifier(col.Field)]
		if !ok {
			return nil, fmt.Errorf("could not find metadata for field '%s'", col.Field)
		}

		config.Schema = append(config.Schema, schema)
	}

	return config, nil
}

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var err error
	var ctx = it.Context()

	// Iterate through all of the Load requests being made.
	for it.Next() {

		var b = t.bindings[it.Binding]

		// Check to see if we have a keyFile open already in gcs for this binding and if not, create one.
		if b.keysFile == nil {
			b.keysFile, err = t.NewExternalDataConnectionFile(
				ctx,
				tmpFileName(),
				b.loadExtDataConfig,
			)
			if err != nil {
				return fmt.Errorf("new external data connection file: %v", err)
			}

			// Clean up the temporary files when load complete (or we error out).
			defer func() {
				if err := b.keysFile.Delete(ctx); err != nil {
					log.Errorf("could not delete load keyfile: %v", err)
				}
				b.keysFile = nil // blank it
			}()
		}

		// Convert our key tuple into a slice of values appropriate for the database.
		convertedKey, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting load key: %w", err)
		}

		// Write our row of keys to the keyFile.
		if err = b.keysFile.WriteRow(convertedKey); err != nil {
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
	for _, b := range t.bindings {
		// If we have a data file for this round.

		if b.keysFile != nil {
			// Flush and close the keyfile.
			if err := b.keysFile.Close(); err != nil {
				return fmt.Errorf("closing external data connection file: %w", err)
			}

			// Setup the tempTableName pointing to our cloud storage external table definition.
			edcTableDefs[b.tempTableName] = b.loadExtDataConfig

			// Append it to the document query list.
			subqueries = append(subqueries, b.loadQuerySQL)
		}
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

	// Fetch the bigquery iterator.
	bqit, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("load job read: %w", err)
	}

	// Load documents.
	for {

		// Fetch and decode from database.
		var bd bindingDocument
		if err = bqit.Next(&bd); err == iterator.Done {
			break
		} else if err != nil {
			return fmt.Errorf("load row read: %w", err)
		}

		// Load the document by sending it back into Flow.
		if err = loaded(bd.Binding, bd.Document); err != nil {
			return fmt.Errorf("load row loaded: %w", err)
		}
	}

	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var ctx = it.Context()

	// Iterate through all the new values to store.
	var err error
	for it.Next() {
		var b = t.bindings[it.Binding]

		// Check to see if we have a mergeFile open already in gcs for this binding and if not, create one.
		if b.mergeFile == nil {
			b.mergeFile, err = t.NewExternalDataConnectionFile(
				ctx,
				tmpFileName(),
				b.storeExtDataConfig,
			)
			if err != nil {
				return nil, fmt.Errorf("new external data connection file: %v", err)
			}
		}

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		if err = b.mergeFile.WriteRow(converted); err != nil {
			return nil, fmt.Errorf("encoding Store to scratch file: %w", err)
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var err error
		if t.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		return nil, sql.CommitWithDelay(ctx, false, transactionDelay, it.Total, t.commit)
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
	for _, b := range t.bindings {
		if b.mergeFile != nil {
			if err := b.mergeFile.Close(); err != nil {
				return fmt.Errorf("mergefile close: %w", err)
			}

			// Setup the tempTableName pointing to our cloud storage external table definition.
			edcTableDefs[b.tempTableName] = b.storeExtDataConfig

			if b.target.DeltaUpdates {
				subqueries = append(subqueries, b.storeInsertSQL)
			} else {
				subqueries = append(subqueries, b.storeUpdateSQL)
			}

			// Clean up the temporary files when store complete (or we error out).
			defer func(defb *binding) {
				if err := defb.mergeFile.Delete(ctx); err != nil {
					log.Errorf("could not delete store mergefile: %v", err)
				}
				defb.mergeFile = nil
			}(b) // b is part of for loop, make sure you're referencing the right b.
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
