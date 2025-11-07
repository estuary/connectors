package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var featureFlagDefaults = map[string]bool{
	"datetimes_as_string": true,
}

// credentialConfig represents authentication options for Cloud Spanner
type credentialConfig struct {
	ServiceAccountJSON string `json:"service_account_json,omitempty" jsonschema:"title=Service Account JSON,description=Google Cloud Service Account JSON credentials. If not provided, Application Default Credentials (ADC) will be used." jsonschema_extras:"secret=true,multiline=true"`
}

// config represents the endpoint configuration for Cloud Spanner
type config struct {
	ProjectID  string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID" jsonschema_extras:"order=0"`
	InstanceID string `json:"instance_id" jsonschema:"title=Instance ID,description=Cloud Spanner Instance ID" jsonschema_extras:"order=1"`
	Database   string `json:"database" jsonschema:"title=Database,description=Cloud Spanner Database name" jsonschema_extras:"order=2"`

	HardDelete bool `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`

	Credentials *credentialConfig `json:"credentials,omitempty" jsonschema:"title=Credentials,description=Google Cloud authentication credentials" jsonschema_extras:"order=4"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	NoFlowDocument bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	FeatureFlags   string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
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

	return nil
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c config) DefaultNamespace() string {
	// Spanner doesn't have schemas
	return ""
}

// tableConfig represents per-binding resource configuration
type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the database table" jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema,omitempty" jsonschema:"title=Alternative Schema,description=Alternative schema for this table (optional). Overrides the default namespace."`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	// Note: Delta updates are not supported in Cloud Spanner because all tables require primary keys
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
	// Delta updates are always false for Spanner since it requires primary keys
	var path []string
	if c.Schema != "" {
		path = []string{c.Schema, c.Table}
	} else {
		path = []string{c.Table}
	}
	return path, false, nil
}

// initializeFlowInternalSchema builds DDL statements for flow_internal schema setup
// It returns DDL statements to drop existing temporary tables, create the schema, and create all Load tables
func initializeFlowInternalSchema(bindings []*spannerBinding) []string {
	log.Info("preparing flow_internal schema DDL for Load operations")

	// Build DDL statements: drop existing temp tables, create schema if not exists, and create all Load tables
	var ddlStatements []string

	// Step 1: Drop all temporary tables if they exist
	// Note: Spanner doesn't support dropping a schema with tables in it, so we drop tables individually
	for idx := range bindings {
		tempTableName := fmt.Sprintf("flow_internal.flow_temp_table_%d", idx)
		ddlStatements = append(ddlStatements, fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName))
	}

	// Step 2: Create the schema if it doesn't exist
	ddlStatements = append(ddlStatements, "CREATE SCHEMA IF NOT EXISTS flow_internal")

	// Step 3: Create all Load tables for bindings
	for _, binding := range bindings {
		ddlStatements = append(ddlStatements, binding.createLoadTableSQL)
	}

	log.WithField("statements", len(ddlStatements)).Info("prepared flow_internal schema DDL")
	return ddlStatements
}

func newSpannerDriver() *sql.Driver[config, tableConfig] {
	return &sql.Driver[config, tableConfig]{
		DocumentationURL: "https://go.estuary.dev/materialize-spanner",
		StartTunnel: func(ctx context.Context, cfg config) error {
			// Spanner doesn't require network tunneling
			return nil
		},
		NewEndpoint: func(ctx context.Context, cfg config, featureFlags map[string]bool) (*sql.Endpoint[config], error) {
			log.WithFields(log.Fields{
				"project":  cfg.ProjectID,
				"instance": cfg.InstanceID,
				"database": cfg.Database,
			}).Info("opening Spanner database")

			dialect := createSpannerDialect(featureFlags)
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
	client      *spanner.Client
	adminClient *database.DatabaseAdminClient
	dbPath      string
	cfg         config
	templates   templates
	bindings    []*spannerBinding
	fence       sql.Fence
	be          *m.BindingEvents
}

// spannerBinding represents a materialized binding with its configuration
type spannerBinding struct {
	target             sql.Table
	createLoadTableSQL string
	loadQuerySQL       string
	dropLoadTableSQL   string
	columnNames        []string // Column names in order for mutations
	columnTypes        []string // DDL types for each column (e.g., "TIMESTAMP", "DATE", "INT64")
	keyColumnIdxs      []int    // Indexes of key columns within columnNames
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

	var opts []option.ClientOption

	if cfg.Credentials != nil && cfg.Credentials.ServiceAccountJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON)))
	}

	// Create a client for this transactor
	// Note: The client will be closed in the Destroy() method
	client, err := spanner.NewClient(ctx, dbPath, opts...)
	if err != nil {
		if strings.Contains(err.Error(), "PermissionDenied") {
			return nil, fmt.Errorf("permission denied: check your credentials and IAM roles")
		} else if strings.Contains(err.Error(), "NotFound") {
			return nil, fmt.Errorf("database not found: %s (check project, instance, and database IDs)", dbPath)
		} else {
			return nil, fmt.Errorf("connecting to Spanner: %w", err)
		}
	}

	// Create an admin client for DDL operations
	adminClient, err := database.NewDatabaseAdminClient(ctx, opts...)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("creating Spanner admin client: %w", err)
	}

	templates := renderTemplates(ep.Dialect)

	t := &transactor{
		client:      client,
		adminClient: adminClient,
		dbPath:      dbPath,
		cfg:         cfg,
		templates:   templates,
		fence:       fence,
		be:          be,
	}

	// Setup bindings first to get the createLoadTableSQL for each
	for _, binding := range bindings {
		if err := t.addBinding(ctx, binding, is); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	// Build DDL statements for flow_internal schema and Load tables
	ddlStatements := initializeFlowInternalSchema(t.bindings)

	// Execute all DDL statements in a single operation for efficiency
	log.WithField("statements", len(ddlStatements)).Info("executing flow_internal schema initialization")
	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: ddlStatements,
	})
	if err != nil {
		adminClient.Close()
		client.Close()
		return nil, fmt.Errorf("submitting flow_internal schema DDL: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		adminClient.Close()
		client.Close()
		return nil, fmt.Errorf("executing flow_internal schema DDL: %w", err)
	}
	log.Info("created flow_internal schema for Load operations")

	return t, nil
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table, is *boilerplate.InfoSchema) error {
	b := &spannerBinding{target: target}

	// Render createLoadTable template
	createLoadTableSQL, err := sql.RenderTableTemplate(target, t.templates.createLoadTable)
	if err != nil {
		return fmt.Errorf("rendering createLoadTable template: %w", err)
	}
	b.createLoadTableSQL = createLoadTableSQL

	// Render the appropriate load query template based on configuration
	var loadTemplate *template.Template
	if t.cfg.Advanced.NoFlowDocument {
		loadTemplate = t.templates.loadQueryNoFlowDocument
	} else {
		loadTemplate = t.templates.loadQuery
	}

	loadQuerySQL, err := sql.RenderTableTemplate(target, loadTemplate)
	if err != nil {
		return fmt.Errorf("rendering load query: %w", err)
	}
	b.loadQuerySQL = loadQuerySQL

	// Render dropLoadTable template
	dropLoadTableSQL, err := sql.RenderTableTemplate(target, t.templates.dropLoadTable)
	if err != nil {
		return fmt.Errorf("rendering dropLoadTable template: %w", err)
	}
	b.dropLoadTableSQL = dropLoadTableSQL

	// Build column names and types list for mutations
	// Order: keys first, then values, then flow_document
	for _, key := range target.Keys {
		b.columnNames = append(b.columnNames, key.Identifier)
		b.columnTypes = append(b.columnTypes, key.DDL)
		b.keyColumnIdxs = append(b.keyColumnIdxs, len(b.columnNames)-1)
	}
	for _, val := range target.Values {
		b.columnNames = append(b.columnNames, val.Identifier)
		b.columnTypes = append(b.columnTypes, val.DDL)
	}
	if target.Document != nil {
		b.columnNames = append(b.columnNames, target.Document.Identifier)
		b.columnTypes = append(b.columnTypes, target.Document.DDL)
	}

	t.bindings = append(t.bindings, b)
	return nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error                  { return nil }
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

// timedSpannerApply wraps spanner.Client.Apply with timing instrumentation
func (t *transactor) timedSpannerApply(ctx context.Context, mutations []*spanner.Mutation, operation string) (time.Time, time.Duration, error) {
	start := time.Now()
	ts, err := t.client.Apply(ctx, mutations)
	duration := time.Since(start)

	log.WithFields(log.Fields{
		"operation": operation,
		"duration":  duration.Seconds(),
		"mutations": len(mutations),
	}).Debug("spanner: apply timing")

	return ts, duration, err
}

// Load implements the Load phase using temporary tables with optimal batching
func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {
	loadStart := time.Now()
	var keyInsertionDuration, queryDuration time.Duration
	var loadedCount int

	defer func() {
		log.WithFields(log.Fields{
			"totalDuration":        time.Since(loadStart).Seconds(),
			"keyInsertionDuration": keyInsertionDuration.Seconds(),
			"queryDuration":        queryDuration.Seconds(),
			"totalKeys":            it.Total,
			"loadedDocs":           loadedCount,
		}).Info("load: phase complete")
	}()

	ctx := it.Context()

	// Track which bindings have keys and prepare for mutation batching
	type bindingState struct {
		tempTableCreated bool
		tempTableName    string
		keyColumnNames   []string
	}
	bindingStates := make(map[int]*bindingState)

	var currentBatch mutationBatch
	currentBatch.reset()
	hadLoads := false
	batchCount := 0

	// Helper function to flush current batch
	flushBatch := func() error {
		if len(currentBatch.mutations) == 0 {
			return nil
		}

		batchCount++

		_, batchDuration, err := t.timedSpannerApply(ctx, currentBatch.mutations, fmt.Sprintf("load-insert-keys-batch-%d", batchCount))
		if err != nil {
			return fmt.Errorf("applying load batch %d (%d operations, %d mutations): %w", batchCount, len(currentBatch.mutations), currentBatch.mutationCount, err)
		}

		keyInsertionDuration += batchDuration

		log.WithFields(log.Fields{
			"operations":         len(currentBatch.mutations),
			"mutationCount":      currentBatch.mutationCount,
			"bytes":              currentBatch.byteSize,
			"batch":              batchCount,
			"batchDuration":      batchDuration.Seconds(),
			"cumulativeDuration": keyInsertionDuration.Seconds(),
		}).Info("load: inserting load keys")

		currentBatch.reset()
		return nil
	}

	// Step 1: Iterate through keys, create temp tables, and insert keys with optimal batching
	for it.Next() {
		hadLoads = true
		bindingIdx := it.Binding
		b := t.bindings[bindingIdx]

		// Initialize state for this binding on first key
		if bindingStates[bindingIdx] == nil {
			tempTableName := fmt.Sprintf("flow_internal.flow_temp_table_%d", bindingIdx)

			log.WithField("table", tempTableName).Info("load: clearing temporary table")
			// Clear any existing data from the table using a mutation
			// Load tables are persistent and pre-created, so we need to delete old data
			deleteMutation := spanner.Delete(tempTableName, spanner.AllKeys())
			_, clearDuration, err := t.timedSpannerApply(ctx, []*spanner.Mutation{deleteMutation}, "load-clear-temp-table")
			if err != nil {
				return fmt.Errorf("clearing temporary load table for binding %d: %w", bindingIdx, err)
			}
			keyInsertionDuration += clearDuration

			keyColumnNames := make([]string, len(b.target.Keys))
			for i, key := range b.target.Keys {
				keyColumnNames[i] = key.Identifier
			}

			bindingStates[bindingIdx] = &bindingState{
				tempTableCreated: true,
				tempTableName:    tempTableName,
				keyColumnNames:   keyColumnNames,
			}
		}

		state := bindingStates[bindingIdx]

		// Convert the key
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}

		// Convert key values to Spanner-compatible types
		spannerKeyVals := make([]interface{}, len(converted))
		for i, keyVal := range converted {
			spannerVal, err := toSpannerValue(keyVal, b.target.Keys[i].Identifier, b.target.Keys[i].DDL)
			if err != nil {
				return fmt.Errorf("converting key value for %s: %w", b.target.Keys[i].Identifier, err)
			}
			spannerKeyVals[i] = spannerVal
		}

		// Add INSERT mutation for this key
		// Note: Each column counts as one mutation in Spanner's mutation count
		mutation := spanner.Insert(state.tempTableName, state.keyColumnNames, spannerKeyVals)
		currentBatch.addMutation(mutation, len(converted))

		// Flush if batch limits reached
		if currentBatch.shouldFlush() {
			if err := flushBatch(); err != nil {
				return err
			}
		}
	}

	if it.Err() != nil {
		return it.Err()
	}

	if !hadLoads {
		return nil
	}

	// Apply any remaining mutations
	if err := flushBatch(); err != nil {
		return err
	}

	log.WithField("batches", batchCount).Info("load: completed inserting all load keys")

	// Step 2: Build and execute single UNION ALL query to fetch all documents
	t.be.StartedEvaluatingLoads()
	defer t.be.FinishedEvaluatingLoads()

	var unionQueries []string
	for bindingIdx := range bindingStates {
		b := t.bindings[bindingIdx]
		unionQueries = append(unionQueries, b.loadQuerySQL)
	}

	log.Info("load: querying documents")
	queryStart := time.Now()
	combinedQuery := strings.Join(unionQueries, "\nUNION ALL\n")
	iter := t.client.Single().Query(ctx, spanner.Statement{SQL: combinedQuery})
	err := iter.Do(func(row *spanner.Row) error {
		var bindingNum int64
		var document spanner.NullJSON

		if err := row.Columns(&bindingNum, &document); err != nil {
			return fmt.Errorf("scanning row: %w", err)
		}

		// Convert NullJSON to json.RawMessage
		var rawDoc json.RawMessage
		if document.Valid {
			switch v := document.Value.(type) {
			case []byte:
				rawDoc = json.RawMessage(v)
			case string:
				rawDoc = json.RawMessage(v)
			default:
				// If it's a structured value, marshal it to JSON
				marshaled, err := json.Marshal(v)
				if err != nil {
					return fmt.Errorf("marshaling document: %w", err)
				}
				rawDoc = json.RawMessage(marshaled)
			}
		}

		loadedCount++
		return loaded(int(bindingNum), rawDoc)
	})
	iter.Stop()
	queryDuration = time.Since(queryStart)

	if err != nil && err != iterator.Done {
		return fmt.Errorf("querying load: %w", err)
	}

	log.Info("load: queried documents")

	return nil
}

// Store implements the Store phase using Spanner mutations with optimal batching
func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {
	storeStart := time.Now()
	var storeMutationDuration time.Duration
	var storeCount int

	ctx := it.Context()

	// Track all applied batches for error handling
	var appliedBatches [][]*spanner.Mutation
	var currentBatch mutationBatch
	currentBatch.reset()

	// Helper function to flush current batch
	flushBatch := func() error {
		if len(currentBatch.mutations) == 0 {
			return nil
		}

		batchNum := len(appliedBatches) + 1
		_, batchDuration, err := t.timedSpannerApply(ctx, currentBatch.mutations, fmt.Sprintf("store-apply-batch-%d", batchNum))
		if err != nil {
			return fmt.Errorf("applying batch %d (%d operations, %d mutations): %w", batchNum, len(currentBatch.mutations), currentBatch.mutationCount, err)
		}

		storeMutationDuration += batchDuration

		log.WithFields(log.Fields{
			"operations":         len(currentBatch.mutations),
			"mutationCount":      currentBatch.mutationCount,
			"bytes":              currentBatch.byteSize,
			"batch":              batchNum,
			"batchDuration":      batchDuration.Seconds(),
			"cumulativeDuration": storeMutationDuration.Seconds(),
		}).Info("store: applying mutation batch")

		// Track applied batch for checkpoint handling
		appliedBatches = append(appliedBatches, currentBatch.mutations)
		currentBatch.reset()
		return nil
	}

	// Process all store operations
	for it.Next() {
		storeCount++
		b := t.bindings[it.Binding]

		var mutation *spanner.Mutation

		if it.Delete && t.cfg.HardDelete {
			if !it.Exists {
				// Ignore items which do not exist and are already deleted
				continue
			}
			// Convert only the key columns for deletion
			keyValues, err := b.target.ConvertKey(it.Key)
			if err != nil {
				return nil, fmt.Errorf("converting delete keys: %w", err)
			}

			// Convert key values to Spanner-compatible types
			spannerKeyValues := make([]interface{}, len(keyValues))
			for i, val := range keyValues {
				spannerVal, err := toSpannerValue(val, b.columnNames[b.keyColumnIdxs[i]], b.columnTypes[b.keyColumnIdxs[i]])
				if err != nil {
					return nil, fmt.Errorf("converting key value for column %s: %w", b.columnNames[b.keyColumnIdxs[i]], err)
				}
				spannerKeyValues[i] = spannerVal
			}

			// Create a Delete mutation
			key := spanner.Key(spannerKeyValues)
			mutation = spanner.Delete(b.target.Identifier, spanner.KeySetFromKeys(key))
			// Delete mutations affect only key columns
			currentBatch.addMutation(mutation, len(keyValues))

			// Flush if batch limits reached
			if currentBatch.shouldFlush() {
				if err := flushBatch(); err != nil {
					return nil, err
				}
			}
			continue
		} else {
			// Build mutation for insert or update
			// Spanner's InsertOrUpdate handles both cases
			values, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
			if err != nil {
				return nil, fmt.Errorf("converting store values: %w", err)
			}

			// Convert values to Spanner-compatible types
			spannerValues := make([]interface{}, len(values))
			for i, val := range values {
				spannerVal, err := toSpannerValue(val, b.columnNames[i], b.columnTypes[i])
				if err != nil {
					return nil, fmt.Errorf("converting value for column %s: %w", b.columnNames[i], err)
				}
				spannerValues[i] = spannerVal
			}

			mutation = spanner.InsertOrUpdate(b.target.Identifier, b.columnNames, spannerValues)
			// InsertOrUpdate mutations affect all columns
			currentBatch.addMutation(mutation, len(values))

			// Flush if batch limits reached
			if currentBatch.shouldFlush() {
				if err := flushBatch(); err != nil {
					return nil, err
				}
			}
		}
	}

	if it.Err() != nil {
		return nil, it.Err()
	}

	// Return the StartCommit function
	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		return nil, m.RunAsyncOperation(func() error {
			commitStart := time.Now()
			var commitDuration time.Duration

			defer func() {
				storeDuration := time.Since(storeStart)
				commitDuration = time.Since(commitStart)

				log.WithFields(log.Fields{
					"totalDuration":         storeDuration.Seconds(),
					"storePhaseDuration":    (storeDuration - commitDuration).Seconds(),
					"commitPhaseDuration":   commitDuration.Seconds(),
					"storeMutationDuration": storeMutationDuration.Seconds(),
					"storeCount":            storeCount,
					"numBatches":            len(appliedBatches),
				}).Info("store: phase complete")
			}()

			// Marshal the checkpoint
			checkpointBytes, err := runtimeCheckpoint.Marshal()
			if err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			// Update the fence checkpoint
			t.fence.Checkpoint = checkpointBytes

			// Add fence update mutation to the current batch
			fenceTableName := "flow_checkpoints_v1" // Default checkpoint table name
			fenceMutation := spanner.InsertOrUpdate(
				fenceTableName,
				[]string{"materialization", "key_begin", "key_end", "fence", "checkpoint"},
				[]interface{}{
					t.fence.Materialization.String(),
					int64(t.fence.KeyBegin), // Convert uint32 to int64
					int64(t.fence.KeyEnd),   // Convert uint32 to int64
					int64(t.fence.Fence),    // Convert uint64 to int64
					string(checkpointBytes),
				},
			)
			// Fence mutation affects 5 columns
			currentBatch.addMutation(fenceMutation, 5)

			// Flush the final batch (includes fence mutation)
			if err := flushBatch(); err != nil {
				return err
			}

			log.WithField("batches", len(appliedBatches)).Info("store: completed all mutation batches")

			return nil
		})
	}, nil
}

func (t *transactor) Destroy() {
	if t.client != nil {
		t.client.Close()
	}
	if t.adminClient != nil {
		t.adminClient.Close()
	}
}

func main() {
	boilerplate.RunMain(newSpannerDriver())
}
