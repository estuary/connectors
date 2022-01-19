package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
)

type transactor struct {
	ep       *Endpoint
	fence    *fence
	bindings []*binding
}

func (t *transactor) Load(it *pm.LoadIterator, _, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	var err error
	var ctx = it.Context()

	// Iterate through all of the Load requests being made.
	for it.Next() {

		var b = t.bindings[it.Binding]

		// Check to see if we have a keyFile open already in gcs for this binding and if not, create one.
		if b.load.keysFile == nil {
			b.load.keysFile, err = t.ep.NewExternalDataConnectionFile(
				ctx,
				tmpFileName(),
				b.load.extDataConfig,
			)
			if err != nil {
				return fmt.Errorf("new external data connection file: %v", err)
			}

			// Clean up the temporary files when load complete (or we error out).
			defer func() {
				if err := b.load.keysFile.Delete(ctx); err != nil {
					log.Errorf("could not delete load keyfile: %v", err)
				}
				b.load.keysFile = nil // blank it
			}()
		}

		// Convert our key tuple into a slice of values appropriate for the database.
		convertedKey, err := b.load.paramsConverter.Convert(it.Key)
		if err != nil {
			return fmt.Errorf("converting key: %w", err)
		}

		// Write our row of keys to the keyFile.
		if err = b.load.keysFile.WriteRow(convertedKey); err != nil {
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
		if b.load.keysFile != nil {

			// Flush and close the keyfile.
			if err := b.load.keysFile.Close(); err != nil {
				return fmt.Errorf("closing external data connection file: %w", err)
			}

			// Setup the tempTableName pointing to our cloud storage external table definition.
			edcTableDefs[b.load.tempTableName] = b.load.extDataConfig

			// Append it to the document query list.
			subqueries = append(subqueries, b.load.sql)
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}

	// Build the query across all tables.
	query := t.ep.newQuery(strings.Join(subqueries, "\nUNION ALL\n") + ";")
	query.TableDefinitions = edcTableDefs // Tell bigquery where to get the external references in gcs.
	job, err := t.ep.runQuery(ctx, query)
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

func (t *transactor) Prepare(_ context.Context, prepare pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	// This is triggered to let you know that the loads have completed.
	// It also tells us what checkpoint we are about to store.
	t.fence.checkpoint = prepare.FlowCheckpoint
	return pf.DriverCheckpoint{}, nil
}

func (t *transactor) Store(it *pm.StoreIterator) error {
	var ctx = it.Context()

	// Iterate through all the new values to store.
	var err error
	for it.Next() {
		var b = t.bindings[it.Binding]

		// Check to see if we have a keyFile open already in gcs for this binding and if not, create one.
		if b.store.mergeFile == nil {
			b.store.mergeFile, err = t.ep.NewExternalDataConnectionFile(
				ctx,
				tmpFileName(),
				b.store.extDataConfig,
			)
			if err != nil {
				return fmt.Errorf("new external data connection file: %v", err)
			}
		}

		// Convert all the values to database appropriate ones and store them in the GCS file.
		var vals = append(it.Key, it.Values...) // TODO(wgd): Was destructive reuse of `it.Key` intentional here?
		if b.store.hasRootDocument {
			vals = append(vals, it.RawJSON)
		}
		if converted, err := b.store.paramsConverter.Convert(vals); err != nil {
			return fmt.Errorf("converting Store: %w", err)
		} else if err = b.store.mergeFile.WriteRow(converted); err != nil {
			return fmt.Errorf("encoding Store to scratch file: %w", err)
		}
	}

	return nil
}

func (t *transactor) Commit(ctx context.Context) error {

	// Build the slice of transactions required for a commit.
	var subqueries []string
	var args []interface{}

	// First we must validate the fence has not been modified.
	subqueries = append(subqueries, fmt.Sprintf(`
	DECLARE vCheckpoint STRING DEFAULT %s;
	DECLARE vMaterialization STRING DEFAULT %s;
	DECLARE vKeyBegin INT64 DEFAULT %s;
	DECLARE vKeyEnd INT64 DEFAULT %s;
	DECLARE vFence INT64 DEFAULT %s;

	DECLARE curFence INT64;

	BEGIN

	BEGIN TRANSACTION;

	-- See if the fence exists as expected
	SET curFence = (SELECT fence FROM `+t.ep.flowTables.Checkpoints.Identifier+` WHERE
		materialization=vMaterialization AND key_begin=vKeyBegin AND key_end=vKeyEnd AND fence=vFence
	);
	-- If fence missing/changed, force exception and abort
	IF curFence IS NULL THEN
		RAISE USING MESSAGE = 'fence not found';
	END IF;

	-- Update the checkpoint with the new value
	UPDATE `+t.ep.flowTables.Checkpoints.Identifier+` SET checkpoint=vCheckpoint WHERE
		materialization=vMaterialization AND key_begin=vKeyBegin AND key_end=vKeyEnd AND fence=vFence;
	`,
		t.ep.generator.Placeholder(0),
		t.ep.generator.Placeholder(1),
		t.ep.generator.Placeholder(2),
		t.ep.generator.Placeholder(3),
		t.ep.generator.Placeholder(4),
	))
	args = append(args,
		base64.StdEncoding.EncodeToString(t.fence.checkpoint),
		t.fence.materialization,
		t.fence.keyBegin,
		t.fence.keyEnd,
		t.fence.fence,
	)

	// This is the map of external table references we will populate. Loop through the bindings and
	// append the SQL for that table.
	var edcTableDefs = make(map[string]bigquery.ExternalData)
	for _, b := range t.bindings {

		if b.store.mergeFile != nil {
			if err := b.store.mergeFile.Close(); err != nil {
				return fmt.Errorf("mergefile close: %w", err)
			}

			// Setup the tempTableName pointing to our cloud storage external table definition.
			edcTableDefs[b.store.tempTableName] = b.store.extDataConfig

			subqueries = append(subqueries, b.store.sql)

			// Clean up the temporary files when store complete (or we error out).
			defer func(defb *binding) {
				if err := defb.store.mergeFile.Delete(ctx); err != nil {
					log.Errorf("could not delete store mergefile: %v", err)
				}
				defb.store.mergeFile = nil
			}(b) // b is part of for loop, make sure you're referencing the right b.
		}
	}

	// Complete the transaction and return the appropriate error.
	subqueries = append(subqueries, `
	COMMIT TRANSACTION;

	SELECT '' AS error;

	EXCEPTION WHEN ERROR THEN
		ROLLBACK TRANSACTION;
		SELECT @@error.message AS error;
	END;
	`)

	// Build the bigquery query of the combined subqueries.
	query := t.ep.newQuery(strings.Join(subqueries, "\n"), args...)
	query.TableDefinitions = edcTableDefs // Tell the query where to get the external references in gcs.

	// This returns a single row with the error status of the query.
	job, err := t.ep.runQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("load query: %w", err)
	}

	// Fetch te query status from the transaction
	var queryStatus struct {
		Error string `bigquery:"error"`
	}

	if err := t.ep.fetchOne(ctx, job, &queryStatus); err != nil {
		return fmt.Errorf("fetch one: %w", err)
	}

	if queryStatus.Error != "" {
		return fmt.Errorf("merge error: %s", queryStatus.Error)
	}

	return nil
}

func (t *transactor) Acknowledge(context.Context) error {
	return nil
}

func (t *transactor) Destroy() {
	_ = t.ep.bigQueryClient.Close()
	_ = t.ep.cloudStorageClient.Close()
}
