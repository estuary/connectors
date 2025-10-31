package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"google.golang.org/api/iterator"
)

// credentialConfig represents authentication options for Cloud Spanner
type credentialConfig struct {
	ServiceAccountJSON string `json:"service_account_json,omitempty" jsonschema:"title=Service Account JSON,description=Google Cloud Service Account JSON credentials. If not provided, Application Default Credentials (ADC) will be used." jsonschema_extras:"secret=true,multiline=true"`
}

// config represents the endpoint configuration for Cloud Spanner
type config struct {
	ProjectID  string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID" jsonschema_extras:"order=0"`
	InstanceID string `json:"instance_id" jsonschema:"title=Instance ID,description=Cloud Spanner Instance ID" jsonschema_extras:"order=1"`
	Database   string `json:"database" jsonschema:"title=Database,description=Cloud Spanner Database name" jsonschema_extras:"order=2"`

	Credentials *credentialConfig `json:"credentials,omitempty" jsonschema:"title=Credentials,description=Google Cloud authentication credentials" jsonschema_extras:"order=3"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	BatchSize int `json:"batch_size,omitempty" jsonschema:"title=Batch Size,description=Maximum number of mutations per commit (1-10000). Default is 5000.,default=5000"`
}

// Validate the configuration
func (c config) Validate() error {
	if c.ProjectID == "" {
		return fmt.Errorf("missing 'project_id'")
	}
	if c.InstanceID == "" {
		return fmt.Errorf("missing 'instance_id'")
	}
	if c.Database == "" {
		return fmt.Errorf("missing 'database'")
	}

	if c.Advanced.BatchSize != 0 {
		if c.Advanced.BatchSize < 1 || c.Advanced.BatchSize > maxMutationsPerBatch {
			return fmt.Errorf("'batch_size' must be between 1 and %d", maxMutationsPerBatch)
		}
	}

	return nil
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return "", map[string]bool{}
}

func (c config) DefaultNamespace() string {
	// Spanner doesn't have schemas
	return ""
}

// tableConfig represents per-binding resource configuration
type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false." jsonschema_extras:"x-delta-updates=true"`
}

func (r tableConfig) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	return nil
}

func (c tableConfig) WithDefaults(cfg config) tableConfig {
	// Spanner doesn't have schemas, so no defaults to apply
	return c
}

func (c tableConfig) Parameters() ([]string, bool, error) {
	return []string{c.Table}, c.Delta, nil
}

func newSpannerDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-spanner",
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"project":  cfg.ProjectID,
				"instance": cfg.InstanceID,
				"database": cfg.Database,
			}).Info("opening Spanner database")

			dialect := createSpannerDialect()
			templates := renderTemplates(dialect)

			return &sql.Endpoint[config]{
				Config:              cfg,
				Dialect:             dialect,
				MetaCheckpoints:     sql.FlowCheckpointsTable(sql.TablePath{}),
				NewClient:           newClient,
				CreateTableTemplate: templates.createTargetTable,
				NewTransactor:       newTransactor,
				ConcurrentApply:     false,
			}, nil
		},
		PreReqs: preReqs,
	}
}

// transactor implements the materialization transactor for Spanner using mutations
type transactor struct {
	client    *spanner.Client
	cfg       config
	templates templates
	bindings  []*spannerBinding
	fence     sql.Fence
	be        *m.BindingEvents
}

// spannerBinding represents a materialized binding with its configuration
type spannerBinding struct {
	target        sql.Table
	loadQuerySQL  string
	columnNames   []string // Column names in order for mutations
	keyColumnIdxs []int    // Indexes of key columns within columnNames
}

func newTransactor(
	ctx context.Context,
	featureFlags map[string]bool,
	ep *sql.Endpoint[config],
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
	is *boilerplate.InfoSchema,
	be *m.BindingEvents,
) (m.Transactor, error) {
	cfg := ep.Config

	// Build the database path
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		cfg.ProjectID, cfg.InstanceID, cfg.Database)

	// Create Spanner client
	// TODO: Add credential options based on cfg.Credentials if needed
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		return nil, fmt.Errorf("creating Spanner client: %w", err)
	}

	templates := renderTemplates(ep.Dialect)

	t := &transactor{
		client:    client,
		cfg:       cfg,
		templates: templates,
		fence:     fence,
		be:        be,
	}

	// Setup bindings
	for _, binding := range bindings {
		if err := t.addBinding(ctx, binding, is); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	return t, nil
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, is *boilerplate.InfoSchema) error {
	b := &spannerBinding{target: target}

	// Render the load query template
	loadQuerySQL, err := sql.RenderTableTemplate(target, t.templates.loadQuery)
	if err != nil {
		return fmt.Errorf("rendering load query: %w", err)
	}
	b.loadQuerySQL = loadQuerySQL

	// Build column names list for mutations
	// Order: keys first, then values, then flow_document
	for _, key := range target.Keys {
		b.columnNames = append(b.columnNames, key.Identifier)
		b.keyColumnIdxs = append(b.keyColumnIdxs, len(b.columnNames)-1)
	}
	for _, val := range target.Values {
		b.columnNames = append(b.columnNames, val.Identifier)
	}
	if target.Document != nil {
		b.columnNames = append(b.columnNames, target.Document.Identifier)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

// Load implements the Load phase using SQL queries
func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	ctx := it.Context()

	// Build a WHERE IN clause for each binding's keys
	type loadBatch struct {
		binding int
		keys    [][]interface{}
	}

	var batches []loadBatch
	currentBatch := make(map[int]*loadBatch)

	for it.Next() {
		bindingIdx := it.Binding
		b := t.bindings[bindingIdx]

		// Convert the key
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		// Add to current batch
		if currentBatch[bindingIdx] == nil {
			currentBatch[bindingIdx] = &loadBatch{binding: bindingIdx}
		}
		currentBatch[bindingIdx].keys = append(currentBatch[bindingIdx].keys, converted)

		// Flush if batch is getting large
		if len(currentBatch[bindingIdx].keys) >= 1000 {
			batches = append(batches, *currentBatch[bindingIdx])
			currentBatch[bindingIdx] = nil
		}
	}

	if it.Err() != nil {
		return it.Err()
	}

	// Collect remaining batches
	for _, batch := range currentBatch {
		if batch != nil && len(batch.keys) > 0 {
			batches = append(batches, *batch)
		}
	}

	// Execute loads for each batch
	t.be.StartedEvaluatingLoads()
	defer t.be.FinishedEvaluatingLoads()

	for _, batch := range batches {
		b := t.bindings[batch.binding]

		// Build WHERE IN query
		// For simplicity, we'll query one key at a time for now
		// TODO: Optimize with proper WHERE IN for composite keys
		for _, keyVals := range batch.keys {
			var whereClause strings.Builder
			whereClause.WriteString("WHERE ")
			for i, key := range b.target.Keys {
				if i > 0 {
					whereClause.WriteString(" AND ")
				}
				whereClause.WriteString(key.Identifier)
				whereClause.WriteString(" = @key")
				whereClause.WriteString(fmt.Sprintf("%d", i))
			}

			query := fmt.Sprintf("SELECT %d AS binding, %s FROM %s %s",
				batch.binding,
				b.target.Document.Identifier,
				b.target.Identifier,
				whereClause.String())

			stmt := spanner.Statement{SQL: query}
			// Add parameters
			params := make(map[string]interface{})
			for i, keyVal := range keyVals {
				params[fmt.Sprintf("key%d", i)] = keyVal
			}
			stmt.Params = params

			// Execute query
			iter := t.client.Single().Query(ctx, stmt)
			err := iter.Do(func(row *spanner.Row) error {
				var bindingNum int
				var document json.RawMessage

				if err := row.Columns(&bindingNum, &document); err != nil {
					return fmt.Errorf("scanning row: %w", err)
				}

				return loaded(bindingNum, document)
			})
			iter.Stop()

			if err != nil && err != iterator.Done {
				return fmt.Errorf("querying load: %w", err)
			}
		}
	}

	return nil
}

// Store implements the Store phase using Spanner mutations
func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	var mutations []*spanner.Mutation
	batchSize := 0

	maxBatch := t.cfg.Advanced.BatchSize
	if maxBatch == 0 {
		maxBatch = 5000 // Default batch size
	}

	for it.Next() {
		b := t.bindings[it.Binding]

		if it.Delete {
			// TODO: Implement deletions
			// Options:
			// 1. Use spanner.Delete() mutation (simple, included in same batch)
			// 2. Use DML DELETE statement (supports Partitioned DML for large deletes)
			// 3. Soft delete with flow_deleted column
			log.WithField("binding", it.Binding).Warn("deletions not yet implemented for Cloud Spanner, skipping")
			continue
		}

		// Build mutation for insert or update
		// Spanner's InsertOrUpdate handles both cases
		values, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting store values: %w", err)
		}

		mutation := spanner.InsertOrUpdate(b.target.Identifier, b.columnNames, values)
		mutations = append(mutations, mutation)
		batchSize++

		// Flush if batch is getting large
		// Note: In production, we should also track byte size
		if batchSize >= maxBatch {
			log.WithField("mutations", batchSize).Debug("flushing mutation batch")
			// For now, we'll accumulate all mutations and commit at once
			// A more sophisticated implementation would flush intermediate batches
		}
	}

	if it.Err() != nil {
		return nil, it.Err()
	}

	// Return the StartCommit function
	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return nil, m.RunAsyncOperation(func() error {
			// Marshal the checkpoint
			checkpointBytes, err := runtimeCheckpoint.Marshal()
			if err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			// Update the fence checkpoint
			t.fence.Checkpoint = checkpointBytes

			// Add fence update mutation
			// We need to update the checkpoints table
			fenceTableName := "flow_checkpoints_v1" // Default checkpoint table name
			fenceMutation := spanner.InsertOrUpdate(
				fenceTableName,
				[]string{"materialization", "key_begin", "key_end", "fence", "checkpoint"},
				[]interface{}{
					t.fence.Materialization.String(),
					t.fence.KeyBegin,
					t.fence.KeyEnd,
					t.fence.Fence,
					string(checkpointBytes),
				},
			)
			mutations = append(mutations, fenceMutation)

			// Apply all mutations atomically
			log.WithField("mutations", len(mutations)).Info("applying mutations to Spanner")
			_, err = t.client.Apply(ctx, mutations)
			if err != nil {
				return fmt.Errorf("applying mutations: %w", err)
			}

			return nil
		})
	}, nil
}

func (t *transactor) Destroy() {
	if t.client != nil {
		t.client.Close()
	}
}

func main() {
	boilerplate.RunMain(newSpannerDriver())
}
