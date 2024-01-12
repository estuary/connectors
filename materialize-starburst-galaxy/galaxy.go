package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	fmt "fmt"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	_ "github.com/trinodb/trino-go-client/trino"
	"go.gazette.dev/core/consumer/protocol"
	"net/url"
	"strings"
)

// config represents the endpoint configuration for galaxy.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Host     string `json:"host" jsonschema:"title=Host and optional port" jsonschema_extras:"order=0"`
	Catalog  string `json:"catalog" jsonschema:"title=Catalog" jsonschema_extras:"order=1"`
	Schema   string `json:"schema" jsonschema:"title=Schema" jsonschema_extras:"order=2"`
	Account  string `json:"account" jsonschema:"title=Account" jsonschema_extras:"order=3"`
	Password string `json:"password" jsonschema:"title=Password" jsonschema_extras:"secret=true,order=4"`
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
	Delta  bool   `json:"delta_updates,omitempty" jsonschema:"default=false,title=Delta Update,description=Should updates to this table be done via delta updates. Default is false."`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{Schema: ep.Config.(*config).Schema}
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	if c.Schema == "" {
		return fmt.Errorf("expected schema")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Schema, c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

// newGalaxyDriver creates a new Driver for Starburst Galaxy.
func newGalaxyDriver() *sql.Driver {
	return &sql.Driver{

		DocumentationURL: "https://go.estuary.dev/materialize-starburst-galaxy",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var cfg = new(config)
			if err := pf.UnmarshalStrict(raw, cfg); err != nil {
				return nil, fmt.Errorf("failed to parse Galaxy configuration: %w", err)
			}

			log.WithFields(log.Fields{
				"host":    cfg.Host,
				"account": cfg.Account,
				"catalog": cfg.Catalog,
				"schema":  cfg.Schema,
			}).Info("opening Galaxy")

			var metaBase sql.TablePath
			var metaSpecs, _ = sql.MetaTables(metaBase)
			var dialect = galaxyDialect(cfg.Schema)
			var templates = renderTemplates(dialect)

			// TODO: Add replace table
			return &sql.Endpoint{
				Config:              cfg,
				Dialect:             dialect,
				MetaSpecs:           &metaSpecs,
				Client:              client{uri: cfg.ToURI(), templates: templates},
				CreateTableTemplate: templates.createTargetTable,
				NewResource:         newTableConfig,
				NewTransactor:       newTransactor,
				Tenant:              tenant,
			}, nil
		},
	}
}

type client struct {
	uri       string
	templates templates
}

func (c client) PreReqs(ctx context.Context, _ *sql.Endpoint) *sql.PrereqErr {
	errs := &sql.PrereqErr{}
	err := c.withDB(func(db *stdsql.DB) error {
		return db.PingContext(ctx)
	})
	if err != nil {
		errs.Err(err)
	}
	return errs
}

func (c client) Apply(ctx context.Context, _ *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {

	db, err := stdsql.Open("trino", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	var actionList []string
	for _, tc := range actions.CreateTables {
		actionList = append(actionList, tc.TableCreateSql)
	}

	for _, at := range actions.AlterTables {
		//TODO: Check if column not exist
		if len(at.AddColumns) > 0 {
			var addColumnsStmt strings.Builder
			if err := c.templates.alterTableColumns.Execute(&addColumnsStmt, at); err != nil {
				return "", fmt.Errorf("rendering alter table columns statement failed: %w", err)
			}
			actionList = append(actionList, addColumnsStmt.String())
		}
		if len(at.DropNotNulls) > 0 {
			return "", fmt.Errorf("dropping not nulls not supported")
		}
	}

	if len(actions.ReplaceTables) > 0 {
		return "", fmt.Errorf("replacing tables not supported")
	}

	// Normalize query by removing trailing ';' as Trino does not accept it.
	updateSpec.ParameterizedQuery = strings.TrimRight(updateSpec.ParameterizedQuery, ";")

	actionsLog := strings.Join(append(actionList, updateSpec.ParameterizedQuery), "\n")
	if req.DryRun {
		return actionsLog, nil
	}

	for _, action := range actionList {
		if _, err := db.ExecContext(ctx, action); err != nil {
			return "", fmt.Errorf("executing statement: %w", err)
		}
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return actionsLog, nil
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (spec, version string, err error) {
	db, err := c.openDB()
	defer db.Close()

	fetchVersionAndSpecQuery, err := sql.RenderTableTemplate(specs, c.templates.fetchVersionAndSpec)
	if err != nil {
		return "", "", err
	}

	// QueryRowContext cannot be used until Trino driver issue is fixed https://github.com/trinodb/trino-go-client/issues/102
	rows, err := db.QueryContext(ctx,
		fetchVersionAndSpecQuery,
		materialization.String())
	if err != nil {
		return "", "", fmt.Errorf("quering spec and version faield: %w", err)
	}
	var numberOfResults int
	for rows.Next() {
		numberOfResults++
		if err := rows.Scan(&version, &spec); err != nil {
			return "", "", fmt.Errorf("quering spec and version faield: %w", err)
		}
	}
	if numberOfResults != 1 {
		return "", "", fmt.Errorf("quering spec and version should return exactly one result number of results: %d", numberOfResults)
	}

	return
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(_ context.Context, _ sql.Table, _ sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c client) openDB() (*stdsql.DB, error) {
	return stdsql.Open("trino", c.uri)
}

func (c client) connectToDb(ctx context.Context) (*stdsql.Conn, error) {
	db, err := c.openDB()
	if err != nil {
		return nil, fmt.Errorf("stdsql.Open failed: %w", err)
	}
	return db.Conn(ctx)
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	db, err := c.openDB()
	if err != nil {
		return err
	}
	err = fn(db)
	if err != nil {
		db.Close()
		return err
	}
	return db.Close()
}

type transactor struct {
	cfg  *config
	load struct {
		conn *stdsql.Conn
	}
	store struct {
		conn *stdsql.Conn
	}
	bindings []*binding
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	_ sql.Fence,
	bindings []sql.Table,
	_ pm.Request_Open,
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)
	var c = ep.Client.(client)
	var templates = c.templates

	var result = &transactor{
		cfg: cfg,
	}

	// Establish connections.
	result.load.conn, err = c.connectToDb(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection for load failed: %w", err)
	}
	result.store.conn, err = c.connectToDb(ctx)
	if err != nil {
		return nil, fmt.Errorf("connection for store failed: %w", err)
	}

	for _, binding := range bindings {
		if err = result.addBinding(binding, templates); err != nil {
			return nil, fmt.Errorf("adding binding %v failed: %w", binding, err)
		}
	}
	return result, nil
}

type binding struct {
	target sql.Table
	load   struct {
		loadQuery string
	}
	store struct {
		insert string
		update string
	}
}

func (t *transactor) addBinding(target sql.Table, templates templates) error {
	var d = new(binding)
	var err error
	d.target = target

	if d.store.insert, err = sql.RenderTableTemplate(target, templates.storeInsert); err != nil {
		return fmt.Errorf("storeInsert template: %w", err)
	}
	if d.store.update, err = sql.RenderTableTemplate(target, templates.storeUpdate); err != nil {
		return fmt.Errorf("storeUpdate template: %w", err)
	}
	if d.load.loadQuery, err = sql.RenderTableTemplate(target, templates.loadQuery); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}

	t.bindings = append(t.bindings, d)
	return nil
}

func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	it.WaitForAcknowledged()
	for it.Next() {
		var b = t.bindings[it.Binding]
		converted, err := b.target.ConvertKey(it.Key)
		if err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		}
		rows, err := t.load.conn.QueryContext(it.Context(), b.load.loadQuery, converted...)
		if err != nil {
			return fmt.Errorf("querying Load documents: %w", err)
		}
		for rows.Next() {
			var binding int
			var document string

			if err = rows.Scan(&binding, &document); err != nil {
				return fmt.Errorf("scanning Load document: %w", err)
			} else if err = loaded(binding, json.RawMessage(document)); err != nil {
				return err
			}
		}

		if err := rows.Close(); err != nil {
			return fmt.Errorf("closing rows: %w", err)
		}

	}

	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {

	for it.Next() {
		var b = t.bindings[it.Binding]
		converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON)
		if err != nil {
			return nil, fmt.Errorf("converting Store: %w", err)
		}
		if it.Exists {
			// First elements in converted are keys which are last in UPDATE statement
			parameters := append(converted[len(it.Key):], converted[0:len(it.Key)]...)
			_, err := t.store.conn.ExecContext(it.Context(), b.store.update, parameters...)
			if err != nil {
				return nil, err
			}
		} else {
			_, err := t.store.conn.ExecContext(it.Context(), b.store.insert, converted...)
			if err != nil {
				return nil, err
			}
		}

	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, _ <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		return nil, nil
	}, nil
}

func (t *transactor) Destroy() {
	t.load.conn.Close()
	t.store.conn.Close()
}

func main() {
	boilerplate.RunMain(newGalaxyDriver())
}
