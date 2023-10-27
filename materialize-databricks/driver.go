package main

import (
	"os"
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
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
	"go.gazette.dev/core/consumer/protocol"
)

const defaultPort = "443"
const volumeName = "flow_staging"

// config represents the endpoint configuration for sql server.
type config struct {
	Address  string         `json:"address" jsonschema:"title=Address,description=Host and port of the SQL warehouse (in the form of host[:port]). Port 443 is used as the default if no specific port is provided." jsonschema_extras:"order=0"`
	HTTPPath string         `json:"httpPath" jsonschema:"title=HTTP path,description=HTTP path of your SQL warehouse"`
	AccessToken string      `json:"accessToken" jsonschema:"title=Personal Access Token,description=Your personal access token for accessing the SQL warehouse"`
	CatalogName string      `json:"catalogName" jsonschema:"title=Catalog Name,description=Name of your Unity Catalog."`

	// TODO: support Azure and GCP authentication
}

// Validate the configuration.
func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"httpPath", c.HTTPPath},
		{"accessToken", c.AccessToken},
		{"catalogName", c.CatalogName},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

// ToURI puts together address and httpPath to form the full workspace URL
func (c *config) ToURI() string {
	var address = c.Address
	if !strings.Contains(address, ":") {
		address = address + ":" + defaultPort
	}

	var params = make(url.Values)
	params.Add("catalog", c.CatalogName)
	params.Add("userAgentEntry", "Estuary Technologies+Flow")

	var uri = url.URL{
		Host: address,
		Path: c.HTTPPath,
		User: url.UserPassword("token", c.AccessToken),
		RawQuery: params.Encode(),
	}

	return strings.TrimLeft(uri.String(), "/")
}

// TODO: validate table names must conform to these limitations:
// https://docs.databricks.com/en/sql/language-manual/sql-ref-names.html
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
	if r.Table == "" {
		return fmt.Errorf("missing table")
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
			var metaSpecs, metaCheckpoints = sql.MetaTables(metaBase)

			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             databricksDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
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


// TODO: test
func (c client) AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string, columnDDL string) (string, error) {
	var query string

	err := c.withDB(func(db *stdsql.DB) error {
		query = fmt.Sprintf(
			"ALTER TABLE %s ADD %s %s;",
			tableIdentifier,
			columnIdentifier,
			columnDDL,
		)

		if !dryRun {
			if err := c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, []string{query}) }); err != nil {
				// TODO: handle error case
				//var sqlServerError *mssqldb.Error

				//if errors.As(err, &sqlServerError) {
				//// See SQLServer error reference:
				//// https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors-1000-to-1999?view=sql-server-2017
				//switch sqlServerError.Number {
				//case 1909:
				//// 1909: Duplicate column name, means the column already exists, we
				//// just skip
				//return nil
				//}
				//}

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

// TODO: test
func (c client) DropNotNullForColumn(ctx context.Context, dryRun bool, table sql.Table, column sql.Column) (string, error) {
	var projection = column.Projection
	projection.Inference.Exists = pf.Inference_MAY
	var mapped, err = databricksDialect.TypeMapper.MapType(&projection)
	if err != nil {
		return "", fmt.Errorf("drop not null: mapping type of %s failed: %w", column.Identifier, err)
	}

	query := fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s %s;",
		table.Identifier,
		column.Identifier,
		mapped.DDL,
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
	var err = c.withDB(func(db *stdsql.DB) error {
		var err error
		fence, err = installFence(ctx, databricksDialect, db, checkpoints, fence, base64.StdEncoding.DecodeString)
		return err
	})
	return fence, err
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
		fence sql.Fence
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
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)

	wsClient, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("%s/%s", cfg.Address, cfg.HTTPPath),
		Token:       cfg.AccessToken,
		Credentials: dbConfig.PatCredentials{}, // enforce PAT auth
	})
	if err != nil {
		return nil, fmt.Errorf("initialising workspace client: %w", err)
	}

	var d = &transactor{cfg: cfg, wsClient: wsClient}
	d.store.fence = fence

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
	remoteStagingPath    string

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

	log.WithFields(log.Fields{
		"path": target.Path,
	}).Warn("debug")
	b.remoteStagingPath = fmt.Sprintf("/Volumes/%s/%s/%s", t.cfg.CatalogName, target.Path[0], volumeName)

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
		{&b.copyIntoLoad, tplCopyIntoLoad},
		{&b.copyIntoStore, tplCopyIntoStore},
	} {
		var err error
		if *m.sql, err = RenderTableWithStagingPath(target, b.remoteStagingPath, m.tpl); err != nil {
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
				// TODO: what happens here in case of nested keys?
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

		var source = fmt.Sprintf("%s/%d_load.json", d.localStagingPath, binding)
		var destination = fmt.Sprintf("%s/%d_load.json", b.remoteStagingPath, binding)
		if bs, err := os.ReadFile(source); err != nil {
			return err
		} else {
			log.WithFields(log.Fields{
				"content": string(bs),
				"destination": destination,
			}).Warn("debug")
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

		if it.Exists {
			b.needsMerge = true
		}
	}

	for _, f := range localFiles {
		f.Close()
	}

	// Upload the staged files
	for binding, _ := range localFiles {
		var b = d.bindings[binding]

		// In case of delta updates, we directly copy from staged files into the target table
		if b.target.DeltaUpdates {
			continue
		}

		var source = fmt.Sprintf("%s/%d_store.json", d.localStagingPath, binding)
		var destination = fmt.Sprintf("%s/%d_store.json", b.remoteStagingPath, binding)
		if bs, err := os.ReadFile(source); err != nil {
			return nil, err
		} else {
			log.WithFields(log.Fields{
				"content": string(bs),
				"destination": destination,
			}).Warn("debug")
		}

		if sourceFile, err := os.Open(source); err != nil {
			return nil, fmt.Errorf("opening local staged file: %w", err)
		} else if err := d.wsClient.Files.Upload(ctx, files.UploadRequest{ Contents: sourceFile, FilePath: destination, Overwrite: true}); err != nil {
			return nil, fmt.Errorf("opening remote staged file: %w", err)
		}

		// COPY INTO temporary load table from staged files
		if _, err := d.store.conn.ExecContext(ctx, b.copyIntoStore); err != nil {
			return nil, fmt.Errorf("store: writing to temporary table: %w", err)
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, runtimeAckCh <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		return nil, pf.RunAsyncOperation(func() error {
			for _, b := range d.bindings {
				if b.needsMerge {
					if _, err := d.store.conn.ExecContext(ctx, b.mergeInto); err != nil {
						return fmt.Errorf("store merge on %q: %w", b.target.Identifier, err)
					}
				} else {
					if _, err := d.store.conn.ExecContext(ctx, b.copyIntoDirect); err != nil {
						return  fmt.Errorf("store direct copy on %q: %w", b.target.Identifier, err)
					}
				}

				if _, err = d.store.conn.ExecContext(ctx, b.truncateStoreSQL); err != nil {
					return fmt.Errorf("truncating store table: %w", err)
				}
			}

			/*
			TODO: handle fencing
			var err error
			if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
				return fmt.Errorf("marshalling checkpoint: %w", err)
			}

			var fenceUpdate strings.Builder
			if err := d.templates["updateFence"].Execute(&fenceUpdate, d.store.fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}

			if results, err := txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
				return fmt.Errorf("updating flow checkpoint: %w", err)
			} else if rowsAffected, err := results.RowsAffected(); err != nil {
				return fmt.Errorf("updating flow checkpoint (rows affected): %w", err)
			} else if rowsAffected < 1 {
				return fmt.Errorf("This instance was fenced off by another")
			}

			if err := txn.Commit(); err != nil {
				return fmt.Errorf("committing Store transaction: %w", err)
			}*/

			return nil
		})
	}, nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newSqlServerDriver())
}
