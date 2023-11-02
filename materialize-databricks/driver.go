package main

import (
	"os"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"text/template"
	"time"

	_ "github.com/databricks/databricks-sql-go"
	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	"github.com/databricks/databricks-sdk-go"
	dbConfig "github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/files"
	driverctx "github.com/databricks/databricks-sql-go/driverctx"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/google/uuid"
	"go.gazette.dev/core/consumer/protocol"
)

// TODO: Error logging is really bad from the driver, it is almost always followed by a "Command Not Found" error

const defaultPort = "443"
const volumeName = "flow_staging"

type tableConfig struct {
	Table         string `json:"table" jsonschema:"title=Table,description=Name of the table" jsonschema_extras:"x-collection-name=true"`
	Schema        string `json:"schema" jsonschema:"title=Schema,description=Schema where the table resides,default=default"`
	AdditionalSql string `json:"additional_table_create_sql,omitempty" jsonschema:"title=Additional Table Create SQL,description=Additional SQL statement(s) to be run in the same transaction that creates the table." jsonschema_extras:"multiline=true"`
	Delta         bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{Schema: "default"}
}

// Validate the resource configuration.
func (r tableConfig) Validate() error {
	var forbiddenChars = ". /"
	if r.Table == "" {
		return fmt.Errorf("missing table")
	}
	if strings.ContainsAny(r.Table, forbiddenChars) {
		return fmt.Errorf("table name %q contains one of the forbidden characters %q", r.Table, forbiddenChars)
	}

	if r.Schema == "" {
		return fmt.Errorf("missing schema")
	}
	if strings.ContainsAny(r.Schema, forbiddenChars) {
		return fmt.Errorf("schema name %q contains one of the forbidden characters %q", r.Schema, forbiddenChars)
	}

	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Schema, c.Table}
}

func (c tableConfig) GetAdditionalSql() string {
	return c.AdditionalSql
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func newSqlServerDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-databricks",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("parsing endpoint configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"address":  cfg.Address,
				"path": cfg.HTTPPath,
			}).Info("connecting to databricks")

			var metaBase sql.TablePath
			var metaSpecs, _ = sql.MetaTables(metaBase)

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             databricksDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     nil,
				Client:              client{uri: cfg.ToURI()},
				CreateTableTemplate: tplCreateTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				CheckPrerequisites:  prereqs,
				Tenant:              tenant,
			}, nil
		},
	}
}

func prereqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	log.WithFields(log.Fields{
		"url":  cfg.ToURI(),
	}).Warn("connecting to databricks")

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then. Note that this should be long enough to allow
	// for an automatically shut down instance to be started up again
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if db, err := stdsql.Open("databricks", cfg.ToURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var execErr dbsqlerr.DBExecutionError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &execErr) {
			// See https://pkg.go.dev/github.com/databricks/databricks-sql-go/errors#pkg-constants
			// and https://docs.databricks.com/en/error-messages/index.html
			switch execErr.SqlState() {
			}
		} else if errors.As(err, &netConnErr) {
			if netConnErr.IsNotFound {
				err = fmt.Errorf("host at address %q cannot be found", cfg.Address)
			}
		} else if errors.As(err, &netOpErr) {
			if netOpErr.Timeout() {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", cfg.Address)
			}
		}

		errs.Err(err)
	} else {
		db.Close()
	}

	return errs
}

// client implements the sql.Client interface.
type client struct {
	uri string
}


func (c client) AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string, columnDDL string) (string, error) {
	var query string

	err := c.withDB(func(db *stdsql.DB) error {
		query = fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s %s;",
			tableIdentifier,
			columnIdentifier,
			columnDDL,
		)

		if !dryRun {
			if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
				var execErr dbsqlerr.DBExecutionError
				if errors.As(err, &execErr) {
					// If the column already exists, we can just skip
					if strings.Contains(execErr.Error(), "FIELDS_ALREADY_EXISTS") {
						return nil
					}
				}

				return err
			}
		}

		return nil
	});

	if err != nil {
		return "", err
	}

	return query, nil
}

func (c client) DropNotNullForColumn(ctx context.Context, dryRun bool, table sql.Table, column sql.Column) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
		table.Identifier,
		column.Identifier,
	)

	if !dryRun {
		if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		// Fail-fast: surface a connection issue.
		if err = db.PingContext(ctx); err != nil {
			return fmt.Errorf("connecting to DB: %w", err)
		}
		err = db.QueryRowContext(
			ctx,
			fmt.Sprintf(
				"SELECT version, spec FROM %s WHERE materialization = %s;",
				specs.Identifier,
				databricksDialect.Literal(materialization.String()),
			),
		).Scan(&version, &specB64)

		return err
	})
	return
}

// ExecStatements is used for the DDL statements of ApplyUpsert and ApplyDelete.
func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("databricks", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	cfg *config

	wsClient *databricks.WorkspaceClient

	localStagingPath string

	// Variables exclusively used by Load.
	load struct {
		conn     *stdsql.Conn
		unionSQL string
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
	}
	bindings []*binding
}

func (t transactor) Context(ctx context.Context) context.Context {
	return driverctx.NewContextWithStagingInfo(ctx, []string{t.localStagingPath})
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
	open pm.Request_Open,
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:       cfg.Credentials.PersonalAccessToken,
		Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
	})
	if err != nil {
		return nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	var d = &transactor{cfg: cfg, wsClient: wsClient}

	// Establish connections.
	if db, err := stdsql.Open("databricks", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("load sql.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if db, err := stdsql.Open("databricks", cfg.ToURI()); err != nil {
		return nil, fmt.Errorf("store sql.Open: %w", err)
	} else if d.store.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding); err != nil {
			return nil, fmt.Errorf("addBinding of %s: %w", binding.Path, err)
		}
	}

	var cp checkpoint
	if open.StateJson != nil {
		if err := json.Unmarshal(open.StateJson, &cp); err != nil {
			return nil, fmt.Errorf("parsing driver config: %w", err)
		}
	}

	if err = d.applyCheckpoint(ctx, cp); err != nil {
		return nil, fmt.Errorf("applying recovered checkpoint: %w", err)
	}

	// Build a query which unions the results of each load subquery.
	var subqueries []string
	for _, b := range d.bindings {
		subqueries = append(subqueries, b.loadQuerySQL)
	}
	d.load.unionSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	if tempDir, err := os.MkdirTemp("", "staging"); err != nil {
		return nil, err
	} else {
		d.localStagingPath = tempDir
	}

	// Create volume for storing staged files
	if _, err := d.store.conn.ExecContext(ctx, fmt.Sprintf("CREATE VOLUME IF NOT EXISTS %s;", volumeName)); err != nil {
		return nil, fmt.Errorf("Exec(CREATE VOLUME IF NOT EXISTS %s;): %w", volumeName, err)
	}

	return d, nil
}

type binding struct {
	target               sql.Table

	// path to where we store staging files
	rootStagingPath      string

	// a binding needs to be merged if there are updates to existing documents
	// otherwise we just do a direct copy by moving all data from temporary table
	// into the target table. Note that in case of delta updates, "needsMerge"
	// will always be false
	needsMerge           bool

	createLoadTableSQL   string
	truncateLoadSQL      string
	dropLoadSQL          string
	loadQuerySQL         string

	createStoreTableSQL  string
	truncateStoreSQL     string
	dropStoreSQL         string

	mergeInto            string

	copyIntoDirect       string
	copyIntoLoad         string
	copyIntoStore        string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var b = &binding{target: target}

	b.rootStagingPath = fmt.Sprintf("/Volumes/%s/%s/%s", t.cfg.CatalogName, target.Path[0], volumeName)

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.createLoadTableSQL, tplCreateLoadTable},
		{&b.createStoreTableSQL, tplCreateStoreTable},
		{&b.loadQuerySQL, tplLoadQuery},
		{&b.truncateLoadSQL, tplTruncateLoad},
		{&b.truncateStoreSQL, tplTruncateStore},
		{&b.dropLoadSQL, tplDropLoad},
		{&b.dropStoreSQL, tplDropStore},
		{&b.mergeInto, tplMergeInto},
	} {
		var err error
		if *m.sql, err = sql.RenderTableTemplate(target, m.tpl); err != nil {
			return err
		}
	}

	for _, m := range []struct {
		sql *string
		tpl *template.Template
	}{
		{&b.copyIntoDirect, tplCopyIntoDirect},
		{&b.copyIntoStore, tplCopyIntoStore},
		{&b.copyIntoLoad, tplCopyIntoLoad},
	} {
		var err error
		if *m.sql, err = RenderTableWithStagingPath(target, b.rootStagingPath, m.tpl); err != nil {
			return err
		}
	}

	t.bindings = append(t.bindings, b)

	// Drop existing temp tables
	if _, err := t.load.conn.ExecContext(ctx, b.dropLoadSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.dropLoadSQL, err)
	}
	if _, err := t.store.conn.ExecContext(ctx, b.dropStoreSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.dropStoreSQL, err)
	}

	// Create a binding-scoped temporary table for staged keys to load.
	if _, err := t.load.conn.ExecContext(ctx, b.createLoadTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createLoadTableSQL, err)
	}

	// Create a binding-scoped temporary table for store documents to be merged
	// into target table
	if _, err := t.store.conn.ExecContext(ctx, b.createStoreTableSQL); err != nil {
		return fmt.Errorf("Exec(%s): %w", b.createStoreTableSQL, err)
	}

	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = d.Context(it.Context())

	var localFiles = make(map[int]*os.File)
	for it.Next() {
		var b = d.bindings[it.Binding]

		if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else {
			if _, ok := localFiles[it.Binding]; !ok {
				var err error
				if localFiles[it.Binding], err = os.Create(fmt.Sprintf("%s/%d_load.json", d.localStagingPath, it.Binding)); err != nil {
					return fmt.Errorf("load: creating staged file: %w", err)
				}
			}

			var jsonMap = make(map[string]interface{}, len(b.target.Keys))
			for i, col := range b.target.Keys {
				// FIXME: what happens here in case of nested keys?
				jsonMap[col.Field] = converted[i]
			}

			if bs, err := json.Marshal(jsonMap); err != nil {
				return fmt.Errorf("marshalling load document component: %w", err)
			} else {
				bs = append(bs, byte('\n'))
				localFiles[it.Binding].Write(bs)
			}
		}
	}

	for _, f := range localFiles {
		f.Close()
	}

	// Upload the staged file
	for binding, _ := range localFiles {
		var b = d.bindings[binding]

		var randomKey, err = uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("generating random key for file: %w", err)
		}

		var source = fmt.Sprintf("%s/%d_load.json", d.localStagingPath, binding)
		var destination = fmt.Sprintf("%s/%d_%s_load.json", b.rootStagingPath, binding, randomKey)

		if bs, err := os.ReadFile(source); err != nil {
			return err
		} else {
			log.WithFields(log.Fields{
				"content": string(bs),
				"destination": destination,
			}).Warn("load file")
		}

		if sourceFile, err := os.Open(source); err != nil {
			return fmt.Errorf("opening local staged file: %w", err)
		} else if err := d.wsClient.Files.Upload(ctx, files.UploadRequest{ Contents: sourceFile, FilePath: destination, Overwrite: true}); err != nil {
			return fmt.Errorf("opening remote staged file: %w", err)
		}

		// COPY INTO temporary load table from staged files
		if _, err := d.load.conn.ExecContext(ctx, b.copyIntoLoad); err != nil {
			return fmt.Errorf("load: writing keys: %w", err)
		}
	}


	if it.Err() != nil {
		return it.Err()
	}

	// Issue a union join of the target tables and their (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.conn.QueryContext(ctx, d.load.unionSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document string

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage([]byte(document))); err != nil {
			return err
		}
		log.WithFields(log.Fields{
			"content": document,
			"binding": binding,
		}).Warn("loaded")
	}

	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	for _, b := range d.bindings {
		if _, err = d.load.conn.ExecContext(ctx, b.truncateLoadSQL); err != nil {
			return fmt.Errorf("truncating load table: %w", err)
		}
	}

	return nil
}

type checkpoint struct {
	Queries []string
}

func (d *transactor) Store(it *pm.StoreIterator) (_ pm.StartCommitFunc, err error) {
	ctx := it.Context()

	var localFiles = make(map[int]*os.File)
	for it.Next() {
		var b = d.bindings[it.Binding]

		if _, ok := localFiles[it.Binding]; !ok {
			var err error
			if localFiles[it.Binding], err = os.Create(fmt.Sprintf("%s/%d_store.json", d.localStagingPath, it.Binding)); err != nil {
				return nil, fmt.Errorf("load: creating staged file: %w", err)
			}
		}

		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON);
		if err != nil {
			return nil, fmt.Errorf("converting store parameters: %w", err)
		}

		var jsonMap = make(map[string]interface{}, len(b.target.Columns()))
		for i, col := range b.target.Columns() {
			// We store JSON fields as strings
			if v, ok := converted[i].(json.RawMessage); ok {
				jsonMap[col.Field] = string(v)
			} else {
				jsonMap[col.Field] = converted[i]
			}
		}

		if bs, err := json.Marshal(jsonMap); err != nil {
			return nil, fmt.Errorf("marshalling load document component: %w", err)
		} else {
			bs = append(bs, byte('\n'))
			localFiles[it.Binding].Write(bs)
		}

		log.WithFields(log.Fields{
			"binding": it.Binding,
			"exists": it.Exists,
			"doc": jsonMap,
		}).Warn("store iterator")
		if it.Exists {
			b.needsMerge = true
		}
	}

	for _, f := range localFiles {
		f.Close()
	}

	// Upload the staged files and build a list of merge and truncate queries that need to be run
	// to effectively commit the files into destination tables. These queries are stored
	// in the checkpoint so that if the connector is restarted in middle of a commit
	// it can run the same queries on the next startup. This is the pattern for 
	// recovery log being authoritative and the connector idempotently applies a commit
	var queries []string
	for binding, _ := range localFiles {
		var b = d.bindings[binding]

		// In case of delta updates, we directly copy from staged files into the target table
		if b.target.DeltaUpdates {
			continue
		}

		var randomKey, err = uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("generating random key for file: %w", err)
		}

		var source = fmt.Sprintf("%s/%d_store.json", d.localStagingPath, binding)
		var destination = fmt.Sprintf("%s/%d_%s_store.json", b.rootStagingPath, binding, randomKey)

		if bs, err := os.ReadFile(source); err != nil {
			return nil, err
		} else {
			log.WithFields(log.Fields{
				"binding": binding,
				"content": string(bs),
				"destination": destination,
			}).Warn("store file")
		}

		if sourceFile, err := os.Open(source); err != nil {
			return nil, fmt.Errorf("opening local staged file: %w", err)
		} else if err := d.wsClient.Files.Upload(ctx, files.UploadRequest{ Contents: sourceFile, FilePath: destination, Overwrite: true}); err != nil {
			return nil, fmt.Errorf("opening remote staged file: %w", err)
		}

		// COPY INTO temporary load table from staged files
		if _, err := d.store.conn.ExecContext(ctx, renderWithFiles(b.copyIntoStore, destination)); err != nil {
			return nil, fmt.Errorf("store: writing to temporary table: %w", err)
		}

		queries = append(queries, renderWithFiles(b.mergeInto, destination), b.truncateStoreSQL)
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var cp = checkpoint{Queries: queries}

		var checkpointJSON, err = json.Marshal(cp)
		if err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		var commitOp = pf.RunAsyncOperation(func() error {
			select {
			case <-runtimeAckCh:
				return d.applyCheckpoint(ctx, cp)
			case <-ctx.Done():
				return ctx.Err()
			}
		})

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, commitOp
	}, nil
}

func renderWithFiles(tpl string, files ...string) string {
	var s = strings.Join(files, "','")
	s = "'" + s + "'"

	return fmt.Sprintf(tpl, s)
}

// applyCheckpoint merges data from temporary table to main table
func (d *transactor) applyCheckpoint(ctx context.Context, cp checkpoint) error {
	for _, q := range cp.Queries {
		if _, err := d.store.conn.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("query %q failed: %w", q, err)
		}
	}

	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newSqlServerDriver())
}
