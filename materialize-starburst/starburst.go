package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	_ "github.com/trinodb/trino-go-client/trino"
	"go.gazette.dev/core/consumer/protocol"
	"net/url"
	"time"
)

// config represents the endpoint configuration for starburst.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Host               string `json:"host" jsonschema:"title=Host and optional port" jsonschema_extras:"order=0"`
	Catalog            string `json:"catalog" jsonschema:"title=Catalog" jsonschema_extras:"order=1"`
	Schema             string `json:"schema" jsonschema:"title=Schema" jsonschema_extras:"order=2"`
	Account            string `json:"account" jsonschema:"title=Account" jsonschema_extras:"order=3"`
	Password           string `json:"password" jsonschema:"title=Password" jsonschema_extras:"secret=true,order=4"`
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID" jsonschema_extras:"order=5"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key" jsonschema_extras:"secret=true,order=6"`
	Region             string `json:"region" jsonschema:"title=Region" jsonschema_extras:"order=7"`
	Bucket             string `json:"bucket" jsonschema:"title=Bucket" jsonschema_extras:"order=8"`
	BucketPath         string `json:"bucketPath" jsonschema:"title=Bucket Path,description=A prefix that will be used to store objects in S3." jsonschema_extras:"order=9"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce active warehouse time by increasing the delay between updates. Defaults to 30 minutes if unset.,enum=0s,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {

	var params = make(url.Values)
	params.Add("catalog", c.Catalog)
	params.Add("schema", c.Schema)

	var uri = url.URL{
		Scheme:   "https",
		Host:     c.Host,
		User:     url.UserPassword(c.Account, c.Password),
		RawQuery: params.Encode(),
	}

	return uri.String()
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"account", c.Account},
		{"host", c.Host},
		{"catalog", c.Catalog},
		{"schema", c.Schema},
		{"password", c.Password},
		{"awsAccessKeyId", c.AWSAccessKeyID},
		{"awsSecretAccessKey", c.AWSSecretAccessKey},
		{"region", c.Region},
		{"bucket", c.Bucket},
		{"bucketPath", c.BucketPath},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type tableConfig struct {
	Table  string `json:"table" jsonschema:"title=Table,description=Name of the table" jsonschema_extras:"x-collection-name=true"`
	Schema string `json:"schema,omitempty" jsonschema:"title=Schema,description=Schema where the table resides"`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{Schema: ep.Config.(*config).Schema}
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Schema, c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return false // Starburst currently doesn't support delta updates.
}

// newStarburstDriver creates a new Driver for Starburst.
func newStarburstDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-starburst",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse Starburst configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"host":    cfg.Host,
				"account": cfg.Account,
				"catalog": cfg.Catalog,
				"schema":  cfg.Schema,
			}).Info("opening Starburst")

			var metaBase sql.TablePath
			var metaSpecs, _ = sql.MetaTables(metaBase)
			var templates = renderTemplates(starburstDialect)

			return &sql.Endpoint{
				Config:               cfg,
				Dialect:              starburstDialect,
				MetaSpecs:            &metaSpecs,
				NewClient:            newClient,
				CreateTableTemplate:  templates.createTargetTable,
				ReplaceTableTemplate: templates.createOrReplaceTargetTable,
				NewResource:          newTableConfig,
				NewTransactor:        newTransactor,
				Tenant:               tenant,
			}, nil
		},
	}
}

type transactor struct {
	cfg  *config
	cp   checkpoint
	load struct {
		conn *stdsql.Conn
	}
	store struct {
		conn *stdsql.Conn
	}
	bindings    []*binding
	s3Operator  *S3Operator
	updateDelay time.Duration
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	_ sql.Fence,
	tables []sql.Table,
	open pm.Request_Open,
) (_ m.Transactor, err error) {
	var cfg = ep.Config.(*config)
	var templates = renderTemplates(starburstDialect)

	var transactor = &transactor{
		cfg: cfg,
	}
	if transactor.updateDelay, err = m.ParseDelay(cfg.Advanced.UpdateDelay); err != nil {
		return nil, err
	}

	// Establish connections.
	transactor.load.conn, err = connectToDb(ctx, cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("connection for load failed: %w", err)
	}
	transactor.store.conn, err = connectToDb(ctx, cfg.ToURI())
	if err != nil {
		return nil, fmt.Errorf("connection for store failed: %w", err)
	}

	for _, table := range tables {
		materializationSpec := open.Materialization.Bindings[table.Binding]
		if err = transactor.addBinding(ctx, materializationSpec, table, templates); err != nil {
			return nil, fmt.Errorf("adding binding %v failed: %w", table, err)
		}
	}

	s3Config := s3config{AWSAccessKeyID: cfg.AWSAccessKeyID, AWSSecretAccessKey: cfg.AWSSecretAccessKey, Region: cfg.Region, Bucket: cfg.Bucket}
	transactor.s3Operator, err = NewS3Operator(s3Config)
	if err != nil {
		return nil, fmt.Errorf("creating s3 operator: %w", err)
	}

	return transactor, nil
}

type binding struct {
	target              sql.Table
	materializationSpec *pf.MaterializationSpec_Binding
	load                struct {
		createTempTable string
		dropTempTable   string
		loadQuery       string
	}
	store struct {
		createTempTable string
		dropTempTable   string
		mergeIntoTarget string
	}
}

func (t *transactor) addBinding(ctx context.Context, materializationSpec *pf.MaterializationSpec_Binding, target sql.Table, templates templates) error {
	var d = new(binding)
	var err error
	d.target = target

	if d.load.createTempTable, err = sql.RenderTableTemplate(target, templates.createLoadTempTable); err != nil {
		return fmt.Errorf("createLoadTempTable template: %w", err)
	}
	if d.load.dropTempTable, err = sql.RenderTableTemplate(target, templates.dropLoadTempTable); err != nil {
		return fmt.Errorf("dropLoadTempTable template: %w", err)
	}
	if d.load.loadQuery, err = sql.RenderTableTemplate(target, templates.loadQuery); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}

	if d.store.createTempTable, err = sql.RenderTableTemplate(target, templates.createStoreTempTable); err != nil {
		return fmt.Errorf("createStoreTempTable template: %w", err)
	}
	if d.store.dropTempTable, err = sql.RenderTableTemplate(target, templates.dropStoreTempTable); err != nil {
		return fmt.Errorf("dropStoreTempTable template: %w", err)
	}
	if d.store.mergeIntoTarget, err = sql.RenderTableTemplate(target, templates.mergeIntoTarget); err != nil {
		return fmt.Errorf("mergeIntoTarget template: %w", err)
	}

	d.materializationSpec = materializationSpec
	t.bindings = append(t.bindings, d)

	// Drop existing temp tables
	if _, err := t.load.conn.ExecContext(ctx, d.store.dropTempTable); err != nil {
		return fmt.Errorf("execurting(%s) failed: %w", d.store.dropTempTable, err)
	}
	if _, err := t.store.conn.ExecContext(ctx, d.load.dropTempTable); err != nil {
		return fmt.Errorf("execurting(%s) failed: %w", d.load.dropTempTable, err)
	}

	return nil
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {

	if err := json.Unmarshal(state, &t.cp); err != nil {
		return err
	}

	if t.cp == nil {
		t.cp = make(checkpoint)
	}
	return nil
}

func (t *transactor) AckDelay() time.Duration {
	return t.updateDelay
}

func (t *transactor) Load(it *m.LoadIterator, loaded func(int, json.RawMessage) error) error {

	loadFileProcessor, err := NewParquetFileProcessor(it.Context(), t.s3Operator, nil, t.bindings, t.cfg.BucketPath)
	if err != nil {
		return fmt.Errorf("creating parquet file processor failed: %w", err)
	}

	for it.Next() {
		// Store data to local file
		if err := loadFileProcessor.Store(it.Binding, it.Key); err != nil {
			return fmt.Errorf("storing file locally failed %w", err)
		}
	}

	//Upload file to cloud
	if _, err := loadFileProcessor.Commit(); err != nil {
		return fmt.Errorf("storing file on cloud failed %w", err)
	}

	for _, b := range t.bindings {
		// Create temp load table from stored files
		dataDirPrefix := loadFileProcessor.GetCloudPrefix(b.target.Binding)
		dataDir := fmt.Sprintf("s3://%s/%s", t.cfg.Bucket, dataDirPrefix)
		if _, err := t.store.conn.ExecContext(it.Context(), b.load.createTempTable, dataDir); err != nil {
			return fmt.Errorf("creating load temp table failed: %w", err)
		}
		// Fetch data from load temp table
		rows, err := t.load.conn.QueryContext(it.Context(), b.load.loadQuery)
		if err != nil {
			return fmt.Errorf("querying Load documents: %w", err)
		}
		for rows.Next() {
			var binding int
			var document string

			err = rows.Scan(&binding, &document)
			if err != nil {
				return fmt.Errorf("scanning Load document: %w", err)
			}
			err = loaded(binding, json.RawMessage(document))
			if err != nil {
				return err
			}
		}

		if err := rows.Close(); err != nil {
			return fmt.Errorf("closing rows: %w", err)
		}

		// Drop temp table
		if _, err := t.load.conn.ExecContext(it.Context(), b.load.dropTempTable); err != nil {
			return fmt.Errorf("dropping load table: %w", err)
		}
		// Cleanup files on cloud
		err = loadFileProcessor.Delete(dataDirPrefix)
		if err != nil {
			return fmt.Errorf("deleting data filed: %w", err)
		}
	}
	return nil
}

type checkpointItem struct {
	CreateSql     string
	MergeSql      string
	DropSql       string
	DataDirPrefix string
}

type checkpoint map[string]*checkpointItem

func (t *transactor) Store(it *m.StoreIterator) (_ m.StartCommitFunc, err error) {

	storeFileProcessor, err := NewParquetFileProcessor(it.Context(), t.s3Operator, nil, t.bindings, t.cfg.BucketPath)
	if err != nil {
		return nil, fmt.Errorf("initialization of file processor failed: %w", err)
	}
	defer storeFileProcessor.Destroy()

	for it.Next() {
		var b = t.bindings[it.Binding]
		doc, err := b.target.Document.MappedType.Converter(it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting document %s: %w", b.target.Document.Field, err)
		}
		row := append(append(it.Key, it.Values...), doc)
		//Store data to local file
		if err := storeFileProcessor.Store(it.Binding, row); err != nil {
			return nil, fmt.Errorf("storing file locally failed %w", err)
		}
	}

	// Upload local files to cloud
	_, err = storeFileProcessor.Commit()
	if err != nil {
		return nil, fmt.Errorf("storing file on cloud failed %w", err)
	}

	for _, b := range t.bindings {

		t.cp[b.target.StateKey] = &checkpointItem{
			DataDirPrefix: storeFileProcessor.GetCloudPrefix(b.target.Binding),
			CreateSql:     b.store.createTempTable,
			MergeSql:      b.store.mergeIntoTarget,
			DropSql:       b.store.dropTempTable}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		var checkpointJSON, err = json.Marshal(t.cp)
		if err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}
		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {

	fileProcessor, err := NewParquetFileProcessor(ctx, t.s3Operator, nil, t.bindings, t.cfg.BucketPath)
	if err != nil {
		return nil, fmt.Errorf("initialization of file processor failed: %w", err)
	}
	defer fileProcessor.Destroy()

	log.Info("store: starting committing changes")
	for stateKey, item := range t.cp {
		// we skip queries that belong to tables which do not have a binding anymore
		// since these tables might be deleted already
		if !t.hasStateKey(stateKey) {
			continue
		}

		dataDir := fmt.Sprintf("s3://%s/%s", t.cfg.Bucket, item.DataDirPrefix)
		// Create a binding-scoped temporary table for store documents to be merged into target table
		if _, err := t.store.conn.ExecContext(ctx, item.CreateSql, dataDir); err != nil {
			return nil, fmt.Errorf("creating temp table failed: %w", err)
		}
		// Merging temp table with target
		if _, err := t.store.conn.ExecContext(ctx, item.MergeSql); err != nil {
			return nil, fmt.Errorf("merging failed: %w", err)
		}
		// Drop temp table
		if _, err := t.store.conn.ExecContext(ctx, item.DropSql); err != nil {
			return nil, fmt.Errorf("droping temp table failed: %w", err)
		}
		//Clean cloud files
		if err := fileProcessor.Delete(item.DataDirPrefix); err != nil {
			return nil, fmt.Errorf("deleting files on cloud failed: %w", err)
		}
	}
	log.Info("store: finished committing changes")

	// After having applied the checkpoint, we try to clean up the checkpoint in the ack response
	// so that a restart of the connector does not need to run the same queries again
	// Note that this is an best-effort "attempt" and there is no guarantee that this checkpoint update
	// can actually be committed
	// Important to note that in this case we do not reset the checkpoint for all bindings, but only the ones
	// that have been committed in this transaction. The reason is that it may be the case that a binding
	// which has been disabled right after a failed attempt to run its queries, must be able to recover by enabling
	// the binding and running the queries that are pending for its last transaction.
	var checkpointClear = make(checkpoint)
	for _, b := range t.bindings {
		checkpointClear[b.target.StateKey] = nil
		delete(t.cp, b.target.StateKey)
	}
	checkpointJSON, err := json.Marshal(checkpointClear)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint clearing json: %w", err)
	}
	return &pf.ConnectorState{UpdatedJson: json.RawMessage(checkpointJSON), MergePatch: true}, nil
}

func (t *transactor) hasStateKey(stateKey string) bool {
	for _, b := range t.bindings {
		if b.target.StateKey == stateKey {
			return true
		}
	}
	return false
}

func (t *transactor) Destroy() {
	t.load.conn.Close()
	t.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newStarburstDriver())
}
