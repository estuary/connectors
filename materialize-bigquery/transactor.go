package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/blob"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type checkpointItem struct {
	Query      string
	SourceURIs []string
	JobPrefix  string
	// TempTableName is referenced in the persisted query. We need to persist it
	// separately to specify the external data connector table definition.
	TempTableName string
}

type checkpoint = map[string]*checkpointItem

type transactor struct {
	cfg       config
	dialect   sql.Dialect
	templates templates

	client     *client
	storeFiles *boilerplate.StagedFiles
	loadFiles  *boilerplate.StagedFiles

	bindings []*binding
	be       *m.BindingEvents
	cp       checkpoint

	objAndArrayAsJson       bool
	loggedStorageApiMessage bool
}

func prepareNewTransactor(
	templates templates,
) func(context.Context, map[string]bool, *sql.Endpoint[config], sql.Fence, []sql.Table, pm.Request_Open, *boilerplate.InfoSchema, *m.BindingEvents) (m.Transactor, error) {
	return func(
		ctx context.Context,
		featureFlags map[string]bool,
		ep *sql.Endpoint[config],
		fence sql.Fence,
		bindings []sql.Table,
		open pm.Request_Open,
		is *boilerplate.InfoSchema,
		be *m.BindingEvents,
	) (m.Transactor, error) {
		var cfg = ep.Config

		client, err := cfg.client(ctx, ep)
		if err != nil {
			return nil, err
		}

		bucket, err := blob.NewGCSBucket(ctx, cfg.Bucket, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
		if err != nil {
			return nil, fmt.Errorf("creating GCS bucket: %w", err)
		}

		t := &transactor{
			cfg:               cfg,
			dialect:           ep.Dialect,
			templates:         templates,
			objAndArrayAsJson: featureFlags["objects_and_arrays_as_json"],
			client:            client,
			be:                be,
			loadFiles:         boilerplate.NewStagedFiles(stagedFileClient{}, bucket, writer.DefaultJsonFileSizeLimit, cfg.effectiveBucketPath(), false, false),
			storeFiles:        boilerplate.NewStagedFiles(stagedFileClient{}, bucket, writer.DefaultJsonFileSizeLimit, cfg.effectiveBucketPath(), true, false),
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
		loadSchema:       loadSchema,
		storeSchema:      storeSchema,
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

	loadFields := make([]string, 0, len(loadSchema))
	storeFields := make([]string, 0, len(storeSchema))
	for _, field := range loadSchema {
		loadFields = append(loadFields, field.Name)
	}
	for _, field := range storeSchema {
		storeFields = append(storeFields, field.Name)
	}
	t.loadFiles.AddBinding(target.Binding, loadFields)
	t.storeFiles.AddBinding(target.Binding, storeFields)

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

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if err := json.Unmarshal(state, &t.cp); err != nil {
		return fmt.Errorf("unmarshaling checkpoint state: %w", err)
	}

	return nil
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = t.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting load key: %w", err)
		} else if err = t.loadFiles.WriteRow(ctx, it.Binding, converted); err != nil {
			return fmt.Errorf("writing normalized key to keyfile: %w", err)
		} else {
			b.loadMergeBounds.NextKey(converted)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	defer t.loadFiles.CleanupCurrentTransaction(ctx)

	// Build the queries of all documents across all bindings that were requested.
	var subqueries []string
	// This is the map of external table references we will populate.
	var edcTableDefs = make(map[string]bigquery.ExternalData)

	for idx, b := range t.bindings {
		if !t.loadFiles.Started(idx) {
			continue
		} else if uris, err := t.loadFiles.Flush(idx); err != nil {
			return fmt.Errorf("flushing load file: %w", err)
		} else if loadQuery, err := renderQueryTemplate(b.target, t.templates.loadQuery, b.loadMergeBounds.Build(), t.objAndArrayAsJson); err != nil {
			return fmt.Errorf("rendering load query template: %w", err)
		} else {
			subqueries = append(subqueries, loadQuery)
			edcTableDefs[b.tempTableName] = edc(uris, b.loadSchema)
		}
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
		} else if err = loaded(bd.Binding, bd.Document); err != nil {
			return fmt.Errorf("load row loaded: %w", err)
		}
	}

	if err := t.loadFiles.CleanupCurrentTransaction(ctx); err != nil {
		return fmt.Errorf("cleaning up load files: %w", err)
	}

	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()

	for it.Next() {
		if t.cfg.HardDelete && it.Delete && !it.Exists {
			// Ignore documents which do not exist and are being deleted.
			continue
		}

		var b = t.bindings[it.Binding]
		if it.Exists {
			b.mustMerge = true
		}

		var flowDocument = it.RawJSON
		if t.cfg.HardDelete && it.Delete {
			flowDocument = json.RawMessage(`"delete"`)
		}

		if converted, err := b.target.ConvertAll(it.Key, it.Values, flowDocument); err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		} else if err = t.storeFiles.WriteRow(ctx, it.Binding, converted); err != nil {
			return nil, fmt.Errorf("writing Store to scratch file: %w", err)
		} else {
			b.storeMergeBounds.NextKey(converted[:len(b.target.Keys)])
		}
	}
	if it.Err() != nil {
		return nil, it.Err()
	}

	for idx, b := range t.bindings {
		if !t.storeFiles.Started(idx) {
			continue
		}

		var query string
		if b.mustMerge {
			mergeQuery, err := renderQueryTemplate(b.target, t.templates.storeUpdate, b.storeMergeBounds.Build(), t.objAndArrayAsJson)
			if err != nil {
				return nil, fmt.Errorf("rendering merge query template: %w", err)
			}
			query = mergeQuery
		} else {
			query = b.storeInsertSQL
		}
		b.mustMerge = false

		uris, err := t.storeFiles.Flush(idx)
		if err != nil {
			return nil, fmt.Errorf("flushing store file for %s: %w", b.target.Path, err)
		}

		t.cp[b.target.StateKey] = &checkpointItem{
			Query:         query,
			SourceURIs:    uris,
			JobPrefix:     uuid.NewString(),
			TempTableName: b.tempTableName,
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var checkpointJSON, err = json.Marshal(t.cp)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	group, groupCtx := errgroup.WithContext(ctx)
	// You can run up to 1,000 concurrent multi-statement queries, so we use a
	// generous concurrency limit here, while not leaving it completely
	// unlimited just to maintain some sense of decorum.
	group.SetLimit(100)

	for sk, item := range t.cp {
		var b *binding
		for _, binding := range t.bindings {
			if binding.target.StateKey == sk {
				b = binding
				break
			}
		}

		if b == nil {
			// No binding is enabled for this state key.
			continue
		}

		group.Go(func() error {
			t.be.StartedResourceCommit(b.target.Path)
			if err := t.client.queryIdempotent(groupCtx, b.storeSchema, item.Query, item.JobPrefix, item.SourceURIs, item.TempTableName); err != nil {
				return fmt.Errorf("acknowledge query for %q: %w", b.target.Path, err)
			} else if err := t.storeFiles.CleanupCheckpoint(ctx, item.SourceURIs); err != nil {
				// Queries will fail if the source files have been deleted, but
				// if queryIdempotent has returned without error then a job for
				// that query will never be attempted again.
				return fmt.Errorf("cleaning up staged files for %q: %w", b.target.Path, err)
			}
			t.be.FinishedResourceCommit(b.target.Path)

			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, err
	}

	var checkpointClear = make(checkpoint)
	for _, b := range t.bindings {
		checkpointClear[b.target.StateKey] = nil
		delete(t.cp, b.target.StateKey)
	}
	var checkpointJSON, err = json.Marshal(checkpointClear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}

	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

func (t *transactor) Destroy() {
	_ = t.client.bigqueryClient.Close()
}
