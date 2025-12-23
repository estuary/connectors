package main

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"strings"
	"text/template"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	instancepb "cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
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
	ServiceAccountJSON string `json:"service_account_json" jsonschema:"title=Service Account JSON,description=Google Cloud Service Account JSON credentials." jsonschema_extras:"secret=true,multiline=true"`
}

// config represents the endpoint configuration for Cloud Spanner
type config struct {
	ProjectID  string `json:"project_id" jsonschema:"title=Project ID,description=Google Cloud Project ID" jsonschema_extras:"order=0"`
	InstanceID string `json:"instance_id" jsonschema:"title=Instance ID,description=Cloud Spanner Instance ID" jsonschema_extras:"order=1"`
	Database   string `json:"database" jsonschema:"title=Database,description=Cloud Spanner Database name" jsonschema_extras:"order=2"`

	HardDelete bool `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).,default=false" jsonschema_extras:"order=3"`

	Credentials credentialConfig `json:"credentials" jsonschema:"title=Credentials,description=Google Cloud authentication credentials" jsonschema_extras:"order=4"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	NoFlowDocument              bool   `json:"no_flow_document,omitempty" jsonschema:"title=Exclude Flow Document,description=When enabled the root document will not be required for standard updates.,default=false"`
	DisableKeyDistributionOptimization bool   `json:"disable_key_distribution_optimization,omitempty" jsonschema:"title=Disable Key Distribution Optimization,description=When enabled the hash prefix normally added to table keys will be omitted. The hash prefix distributes writes across Spanner splits and avoids hotspots.,default=false"`
	FeatureFlags                string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
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

// sqlDriver contains the sql.Driver configuration for Spanner
var sqlDriver = &sql.Driver[config, tableConfig]{
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
		templates := renderTemplates(dialect, !cfg.Advanced.DisableKeyDistributionOptimization)

		return &sql.Endpoint[config]{
			Config:              cfg,
			Dialect:             dialect,
			MetaCheckpoints:     sql.FlowCheckpointsTable(nil),
			NewClient:           newClient,
			CreateTableTemplate: templates.createTargetTable,
			NewTransactor:       newTransactor,
			ConcurrentApply:     false,
			NoFlowDocument:      cfg.Advanced.NoFlowDocument,
		}, nil
	},
	PreReqs: preReqs,
}

// driver implements boilerplate.Connector with custom validation
type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	return sqlDriver.Spec(ctx, req)
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	// First run standard validation
	resp, err := sqlDriver.Validate(ctx, req)
	if err != nil {
		return nil, err
	}

	// Check if key distribution optimization setting changed
	var lastConfigJson []byte
	if req.LastMaterialization != nil {
		lastConfigJson = req.LastMaterialization.ConfigJson
	}
	keyDistOptChanged, err := keyDistributionOptimizationChanged(lastConfigJson, req.ConfigJson)
	if err != nil {
		return nil, err
	}

	if keyDistOptChanged {
		// Add INCOMPATIBLE constraint to all key fields for all bindings.
		// This forces table re-creation (not just truncation) when the setting changes,
		// because the primary key structure changes (flow_key_hash is added or removed).
		for idx, binding := range resp.Bindings {
			reqBinding := req.Bindings[idx]
			for _, p := range reqBinding.Collection.Projections {
				if p.IsPrimaryKey {
					if binding.Constraints == nil {
						binding.Constraints = make(map[string]*pm.Response_Validated_Constraint)
					}
					log.WithFields(log.Fields{
						"binding": idx,
						"field":   p.Field,
					}).Info("adding INCOMPATIBLE constraint for key field due to key distribution optimization change")
					binding.Constraints[p.Field] = &pm.Response_Validated_Constraint{
						Type:   pm.Response_Validated_Constraint_INCOMPATIBLE,
						Reason: "The 'disable_key_distribution_optimization' setting has changed. This changes the table's primary key structure (flow_key_hash column is added or removed) and requires re-creating all tables. Backfill all bindings to proceed.",
					}
				}
			}
		}
	}

	return resp, nil
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return sqlDriver.Apply(ctx, req)
}

func (driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return sqlDriver.NewTransactor(ctx, req, be)
}

// keyDistributionOptimizationChanged checks if the disable_key_distribution_optimization
// setting changed between the old and new endpoint config.
func keyDistributionOptimizationChanged(oldConfig, newConfig []byte) (bool, error) {
	// Skip if no previous materialization
	if len(oldConfig) == 0 {
		return false, nil
	}

	var newCfg config
	var oldFullCfg struct{
		Config config `json:"config"`
	}
	if err := json.Unmarshal(oldConfig, &oldFullCfg); err != nil {
		return false, fmt.Errorf("parsing previous endpoint config: %w", err)
	}
	if err := json.Unmarshal(newConfig, &newCfg); err != nil {
		return false, fmt.Errorf("parsing new endpoint config: %w", err)
	}

	return oldFullCfg.Config.Advanced.DisableKeyDistributionOptimization != newCfg.Advanced.DisableKeyDistributionOptimization, nil
}

// transactor implements the materialization transactor for Spanner using mutations
type transactor struct {
	client        *spanner.Client
	adminClient   *database.DatabaseAdminClient
	dbPath        string
	cfg           config
	templates     templates
	bindings      []*spannerBinding
	fence         sql.Fence
	be            *m.BindingEvents
	numPartitions int // Number of partitions for key distribution (nodes × 10)
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

// hashSingleValue computes FNV-1a 64-bit hash of a single value.
// Used for partition routing when key distribution optimization is disabled.
func hashSingleValue(val interface{}) (int64, error) {
	h := fnv.New64a()

	switch v := val.(type) {
	case int64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
		h.Write(buf)
	case float64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
		h.Write(buf)
	case string:
		h.Write([]byte(v))
	case []byte:
		h.Write(v)
	case bool:
		if v {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case nil:
		h.Write([]byte{0})
	default:
		// For any other type, convert to JSON string for consistent serialization
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return 0, fmt.Errorf("marshaling value of type %T for hashing: %w", v, err)
		}
		h.Write(jsonBytes)
	}

	return int64(h.Sum64()), nil
}

// computeKeyHash computes a deterministic hash of the given key values using FNV-1a 64-bit algorithm.
// The hash provides uniform distribution to avoid write hotspots in Spanner.
func computeKeyHash(keyValues []interface{}) (int64, error) {
	h := fnv.New64a()

	for _, val := range keyValues {
		// Serialize each value deterministically to bytes
		switch v := val.(type) {
		case int64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, uint64(v))
			h.Write(buf)
		case float64:
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, math.Float64bits(v))
			h.Write(buf)
		case string:
			h.Write([]byte(v))
		case []byte:
			h.Write(v)
		case bool:
			if v {
				h.Write([]byte{1})
			} else {
				h.Write([]byte{0})
			}
		case nil:
			h.Write([]byte{0})
		default:
			// For any other type, convert to JSON string for consistent serialization
			jsonBytes, err := json.Marshal(v)
			if err != nil {
				return 0, fmt.Errorf("marshaling key value of type %T for hashing: %w", v, err)
			}
			h.Write(jsonBytes)
		}
	}

	// Return the hash as int64 (Spanner's INT64 type)
	return int64(h.Sum64()), nil
}

// queryNodeCount queries the Spanner instance to determine the number of nodes.
// This is used to calculate the number of partitions for key distribution.
func queryNodeCount(ctx context.Context, projectID, instanceID string, opts []option.ClientOption) (int, error) {
	// Create instance admin client
	instanceAdminClient, err := instance.NewInstanceAdminClient(ctx, opts...)
	if err != nil {
		return 0, fmt.Errorf("creating instance admin client: %w", err)
	}
	defer instanceAdminClient.Close()

	// Build instance path
	instancePath := fmt.Sprintf("projects/%s/instances/%s", projectID, instanceID)

	// Get instance information
	req := &instancepb.GetInstanceRequest{
		Name: instancePath,
	}
	inst, err := instanceAdminClient.GetInstance(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("getting instance information for %s: %w", instancePath, err)
	}

	// Calculate node count based on instance configuration
	// Spanner can have either NodeCount or ProcessingUnits configured
	nodeCount := int(inst.GetNodeCount())
	processingUnits := inst.GetProcessingUnits()

	if nodeCount == 0 && processingUnits > 0 {
		// Convert processing units to node count
		// 1 node = 1000 processing units
		nodeCount = int(processingUnits) / 1000
		if nodeCount == 0 {
			// If less than 1000 processing units, round up to 1 node
			nodeCount = 1
		}
	}

	if nodeCount == 0 {
		// Zero node count indicates configuration issue or insufficient permissions
		return 0, fmt.Errorf("unable to determine node count for instance %s: nodeCount=%d, processingUnits=%d (verify instance configuration and spanner.instances.get IAM permission)",
			instancePath, inst.GetNodeCount(), processingUnits)
	}

	log.WithFields(log.Fields{
		"instance":        instancePath,
		"nodeCount":       nodeCount,
		"processingUnits": processingUnits,
	}).Info("queried Spanner instance node count")

	return nodeCount, nil
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
) (_ m.Transactor, err error) {
	cfg := ep.Config

	// Create shared Spanner clients
	clients, err := newSpannerClients(ctx, cfg)
	if err != nil {
		return nil, err
	}

	// Ensure cleanup on error
	defer func() {
		if err != nil {
			clients.Close()
		}
	}()

	templates := renderTemplates(ep.Dialect, !cfg.Advanced.DisableKeyDistributionOptimization)

	// Always query node count for partition calculation
	// Partitions = nodes × 10 for parallel flushing
	opts := []option.ClientOption{option.WithCredentialsJSON([]byte(cfg.Credentials.ServiceAccountJSON))}
	nodeCount, err := queryNodeCount(ctx, cfg.ProjectID, cfg.InstanceID, opts)
	if err != nil {
		return nil, fmt.Errorf("querying node count: %w", err)
	}
	numPartitions := nodeCount * 10

	log.WithFields(log.Fields{
		"nodeCount":                   nodeCount,
		"numPartitions":               numPartitions,
		"keyDistributionOptimization": !cfg.Advanced.DisableKeyDistributionOptimization,
	}).Info("calculated partitions for parallel flushing")

	t := &transactor{
		client:        clients.dataClient,
		adminClient:   clients.adminClient,
		dbPath:        clients.dbPath,
		cfg:           cfg,
		templates:     templates,
		fence:         fence,
		be:            be,
		numPartitions: numPartitions,
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
	op, err := clients.adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   clients.dbPath,
		Statements: ddlStatements,
	})
	if err != nil {
		return nil, fmt.Errorf("submitting flow_internal schema DDL: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
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
	// Order: [flow_key_hash (optional)], keys, values, flow_document
	if !t.cfg.Advanced.DisableKeyDistributionOptimization {
		b.columnNames = append(b.columnNames, "flow_key_hash")
		b.columnTypes = append(b.columnTypes, "INT64 NOT NULL")
	}

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
	var clearDuration, queryDuration time.Duration
	var loadedCount int

	ctx := it.Context()

	// Create async batch flusher for parallel mutation flushes
	flusher := newAsyncBatchFlusher(ctx, t, "load-insert-keys")
	log.WithField("partitions", t.numPartitions).Info("load: using partition-based batching")

	defer func() {
		keyInsertionDuration := clearDuration + flusher.getTotalDuration()
		log.WithFields(log.Fields{
			"totalDuration":        time.Since(loadStart).Seconds(),
			"keyInsertionDuration": keyInsertionDuration.Seconds(),
			"queryDuration":        queryDuration.Seconds(),
			"totalKeys":            it.Total,
			"loadedDocs":           loadedCount,
		}).Info("load: phase complete")
	}()

	// Track which bindings have keys and prepare for mutation batching
	type bindingState struct {
		tempTableCreated bool
		tempTableName    string
		keyColumnNames   []string
	}
	bindingStates := make(map[int]*bindingState)

	hadLoads := false

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
			_, duration, err := t.timedSpannerApply(ctx, []*spanner.Mutation{deleteMutation}, "load-clear-temp-table")
			if err != nil {
				return fmt.Errorf("clearing temporary load table for binding %d: %w", bindingIdx, err)
			}
			clearDuration += duration

			keyColumnNames := make([]string, 0, len(b.target.Keys)+1)

			// If key distribution optimization is enabled, prepend flow_key_hash column
			if !t.cfg.Advanced.DisableKeyDistributionOptimization {
				keyColumnNames = append(keyColumnNames, "flow_key_hash")
			}

			for _, key := range b.target.Keys {
				keyColumnNames = append(keyColumnNames, key.Identifier)
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
		spannerKeyVals := make([]interface{}, 0, len(converted)+1)

		// Compute partition index for routing
		var partitionHash int64
		if !t.cfg.Advanced.DisableKeyDistributionOptimization {
			// When optimization enabled: compute full key hash for flow_key_hash column and partition routing
			keyHash, err := computeKeyHash(converted)
			if err != nil {
				return fmt.Errorf("computing key hash for load: %w", err)
			}
			spannerKeyVals = append(spannerKeyVals, keyHash)
			partitionHash = keyHash
		} else {
			// When optimization disabled: hash only first key value for partition routing
			// This allows customers with well-distributed keys to benefit from parallel flushing
			hash, err := hashSingleValue(converted[0])
			if err != nil {
				return fmt.Errorf("hashing first key value for partition routing: %w", err)
			}
			partitionHash = hash
		}

		for i, keyVal := range converted {
			spannerVal, err := toSpannerValue(keyVal, b.target.Keys[i].Identifier, b.target.Keys[i].DDL)
			if err != nil {
				return fmt.Errorf("converting key value for %s: %w", b.target.Keys[i].Identifier, err)
			}
			spannerKeyVals = append(spannerKeyVals, spannerVal)
		}

		// Add INSERT mutation for this key
		// Note: Each column counts as one mutation in Spanner's mutation count
		mutation := spanner.Insert(state.tempTableName, state.keyColumnNames, spannerKeyVals)

		// Calculate actual byte size of the mutation values
		mutationSize := calculateMutationByteSize(spannerKeyVals)

		// Route to partition based on hash
		partitionIdx := flusher.partitionedBatch.getPartitionIndex(partitionHash)
		if err := flusher.partitionedBatch.addMutation(partitionIdx, mutation, len(spannerKeyVals), mutationSize); err != nil {
			return fmt.Errorf("adding load mutation: %w", err)
		}

		// Check if any partitions are ready to flush and launch async
		readyPartitions := flusher.partitionedBatch.getReadyPartitions()
		if err := flusher.flushPartitionsAsync(readyPartitions); err != nil {
			return fmt.Errorf("flushing ready partitions during load: %w", err)
		}
	}

	if it.Err() != nil {
		return it.Err()
	}

	if !hadLoads {
		return nil
	}

	// Flush any remaining mutations asynchronously
	nonEmptyPartitions := flusher.partitionedBatch.getNonEmptyPartitions()
	if err := flusher.flushPartitionsAsync(nonEmptyPartitions); err != nil {
		return fmt.Errorf("flushing remaining partitions during load: %w", err)
	}

	// Wait for all flushes (both background and final) to complete
	if err := flusher.wait(); err != nil {
		return fmt.Errorf("waiting for all flushes: %w", err)
	}

	log.WithField("batches", flusher.getBatchCount()).Info("load: completed inserting all load keys")

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
			return fmt.Errorf("scanning load result row: %w", err)
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
	var storeCount int
	var storeBytes int

	ctx := it.Context()

	// Create async batch flusher for parallel mutation flushes
	flusher := newAsyncBatchFlusher(ctx, t, "store")
	log.WithField("partitions", t.numPartitions).Info("store: using partition-based batching")

	// Ensure flusher is cleaned up on error paths
	defer func() {
		if err != nil {
			// Wait for any pending flushes to complete before returning error
			if waitErr := flusher.wait(); waitErr != nil {
				log.WithError(waitErr).Error("error waiting for flusher cleanup on error path")
			}
		}
	}()

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
			spannerKeyValues := make([]interface{}, 0, len(keyValues)+1)

			// Compute partition index for routing
			var partitionHash int64
			if !t.cfg.Advanced.DisableKeyDistributionOptimization {
				// When optimization enabled: compute full key hash for flow_key_hash column and partition routing
				keyHash, err := computeKeyHash(keyValues)
				if err != nil {
					return nil, fmt.Errorf("computing key hash for delete: %w", err)
				}
				spannerKeyValues = append(spannerKeyValues, keyHash)
				partitionHash = keyHash
			} else {
				// When optimization disabled: hash only first key value for partition routing
				hash, err := hashSingleValue(keyValues[0])
				if err != nil {
					return nil, fmt.Errorf("hashing first key value for partition routing: %w", err)
				}
				partitionHash = hash
			}

			for i, val := range keyValues {
				spannerVal, err := toSpannerValue(val, b.columnNames[b.keyColumnIdxs[i]], b.columnTypes[b.keyColumnIdxs[i]])
				if err != nil {
					return nil, fmt.Errorf("converting key value for column %s: %w", b.columnNames[b.keyColumnIdxs[i]], err)
				}
				spannerKeyValues = append(spannerKeyValues, spannerVal)
			}

			// Create a Delete mutation
			key := spanner.Key(spannerKeyValues)
			mutation = spanner.Delete(b.target.Identifier, spanner.KeySetFromKeys(key))

			// Calculate actual byte size of the mutation values
			mutationSize := calculateMutationByteSize(spannerKeyValues)
			storeBytes += mutationSize

			// Route to partition based on hash
			partitionIdx := flusher.partitionedBatch.getPartitionIndex(partitionHash)
			if err := flusher.partitionedBatch.addMutation(partitionIdx, mutation, len(keyValues), mutationSize); err != nil {
				return nil, fmt.Errorf("adding delete mutation: %w", err)
			}

			// Check if any partitions are ready to flush and launch async
			readyPartitions := flusher.partitionedBatch.getReadyPartitions()
			if err := flusher.flushPartitionsAsync(readyPartitions); err != nil {
				return nil, fmt.Errorf("flushing ready partitions during store delete: %w", err)
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
			spannerValues := make([]interface{}, 0, len(values)+1)

			// Compute partition index for routing
			// The key values are the first len(b.target.Keys) values
			keyValues := values[:len(b.target.Keys)]
			var partitionHash int64
			if !t.cfg.Advanced.DisableKeyDistributionOptimization {
				// When optimization enabled: compute full key hash for flow_key_hash column and partition routing
				keyHash, err := computeKeyHash(keyValues)
				if err != nil {
					return nil, fmt.Errorf("computing key hash for store: %w", err)
				}
				spannerValues = append(spannerValues, keyHash)
				partitionHash = keyHash
			} else {
				// When optimization disabled: hash only first key value for partition routing
				hash, err := hashSingleValue(keyValues[0])
				if err != nil {
					return nil, fmt.Errorf("hashing first key value for partition routing: %w", err)
				}
				partitionHash = hash
			}

			for i, val := range values {
				// When key distribution optimization is enabled, column indices are offset by 1
				colIdx := i
				if !t.cfg.Advanced.DisableKeyDistributionOptimization {
					colIdx = i + 1
				}
				spannerVal, err := toSpannerValue(val, b.columnNames[colIdx], b.columnTypes[colIdx])
				if err != nil {
					return nil, fmt.Errorf("converting value for column %s: %w", b.columnNames[colIdx], err)
				}
				spannerValues = append(spannerValues, spannerVal)
			}

			mutation = spanner.InsertOrUpdate(b.target.Identifier, b.columnNames, spannerValues)

			// Calculate actual byte size of the mutation values
			mutationSize := calculateMutationByteSize(spannerValues)
			storeBytes += mutationSize

			// Route to partition based on hash
			partitionIdx := flusher.partitionedBatch.getPartitionIndex(partitionHash)
			if err := flusher.partitionedBatch.addMutation(partitionIdx, mutation, len(values), mutationSize); err != nil {
				return nil, fmt.Errorf("adding store mutation: %w", err)
			}

			// Check if any partitions are ready to flush and launch async
			readyPartitions := flusher.partitionedBatch.getReadyPartitions()
			if err := flusher.flushPartitionsAsync(readyPartitions); err != nil {
				return nil, fmt.Errorf("flushing ready partitions during store: %w", err)
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
					"totalDuration":       storeDuration.Seconds(),
					"storePhaseDuration":  (storeDuration - commitDuration).Seconds(),
					"commitPhaseDuration": commitDuration.Seconds(),
					"storeCount":          storeCount,
					"storeBytes":          storeBytes,
					"numBatches":          flusher.getBatchCount(),
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
			// Use the standard checkpoint table name
			fenceValues := []interface{}{
				t.fence.Materialization.String(),
				int64(t.fence.KeyBegin), // Convert uint32 to int64
				int64(t.fence.KeyEnd),   // Convert uint32 to int64
				int64(t.fence.Fence),    // Convert uint64 to int64
				base64.StdEncoding.EncodeToString(checkpointBytes),
			}
			fenceMutation := spanner.InsertOrUpdate(
				sql.DefaultFlowCheckpoints,
				[]string{"materialization", "key_begin", "key_end", "fence", "checkpoint"},
				fenceValues,
			)

			// Flush all data partitions first (before fence)
			nonEmptyPartitions := flusher.partitionedBatch.getNonEmptyPartitions()
			if err := flusher.flushPartitionsAsync(nonEmptyPartitions); err != nil {
				return fmt.Errorf("flushing remaining partitions during commit: %w", err)
			}

			// Wait for all data flushes to complete
			if err := flusher.wait(); err != nil {
				return fmt.Errorf("flushing data mutations: %w", err)
			}

			// Apply fence mutation separately after all data succeeds
			// This ensures data is never committed without the checkpoint update
			if _, _, err := t.timedSpannerApply(ctx, []*spanner.Mutation{fenceMutation}, "store-fence"); err != nil {
				return fmt.Errorf("applying fence checkpoint: %w", err)
			}

			log.WithField("batches", flusher.getBatchCount()).Info("store: completed all mutation batches")

			return nil
		})
	}, nil
}

func (t *transactor) Destroy() {
	t.client.Close()
	t.adminClient.Close()
}

func main() {
	boilerplate.RunMain(&driver{})
}
