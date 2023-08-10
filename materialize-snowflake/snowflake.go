package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	sf "github.com/snowflakedb/gosnowflake"
	"go.gazette.dev/core/consumer/protocol"
)

// config represents the endpoint configuration for snowflake.
// It must match the one defined for the source specs (flow.yaml) in Rust.
type config struct {
	Host      string `json:"host" jsonschema:"title=Host URL,description=The Snowflake Host used for the connection. Must include the account identifier and end in .snowflakecomputing.com. Example: orgname-accountname.snowflakecomputing.com (do not include the protocol)." jsonschema_extras:"order=0"`
	Account   string `json:"account" jsonschema:"title=Account,description=The Snowflake account identifier." jsonschema_extras:"order=1"`
	User      string `json:"user" jsonschema:"title=User,description=The Snowflake user login name." jsonschema_extras:"order=2"`
	Password  string `json:"password" jsonschema:"title=Password,description=The password for the provided user." jsonschema_extras:"secret=true,order=3"`
	Database  string `json:"database" jsonschema:"title=Database,description=The SQL database to connect to." jsonschema_extras:"order=4"`
	Schema    string `json:"schema" jsonschema:"title=Schema,description=The SQL schema to use." jsonschema_extras:"order=5"`
	Warehouse string `json:"warehouse,omitempty" jsonschema:"title=Warehouse,description=The Snowflake virtual warehouse used to execute queries." jsonschema_extras:"order=6"`
	Role      string `json:"role,omitempty" jsonschema:"title=Role,description=The user role used to perform actions." jsonschema_extras:"order=7"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	UpdateDelay string `json:"updateDelay,omitempty" jsonschema:"title=Update Delay,description=Potentially reduce active warehouse time by increasing the delay between updates.,enum=15m,enum=30m,enum=1h,enum=2h,enum=4h"`
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI(tenant string) string {
	// Build a DSN connection string.
	var configCopy = c.asSnowflakeConfig(tenant)
	// client_session_keep_alive causes the driver to issue a periodic keepalive request.
	// Without this, the authentication token will expire after 4 hours of inactivity.
	// The Params map will not have been initialized if the endpoint config didn't specify
	// it, so we check and initialize here if needed.
	if configCopy.Params == nil {
		configCopy.Params = make(map[string]*string)
	}
	configCopy.Params["client_session_keep_alive"] = &trueString
	dsn, err := sf.DSN(&configCopy)
	if err != nil {
		panic(fmt.Errorf("building snowflake dsn: %w", err))
	}

	return dsn
}

func (c *config) asSnowflakeConfig(tenant string) sf.Config {
	var maxStatementCount string = "0"
	var json string = "json"
	return sf.Config{
		Account:     c.Account,
		Host:        c.Host,
		User:        c.User,
		Password:    c.Password,
		Database:    c.Database,
		Schema:      c.Schema,
		Warehouse:   c.Warehouse,
		Role:        c.Role,
		Application: fmt.Sprintf("%s_EstuaryFlow", tenant),
		Params: map[string]*string{
			// By default Snowflake expects the number of statements to be provided
			// with every request. By setting this parameter to zero we are allowing a
			// variable number of statements to be executed in a single request
			"MULTI_STATEMENT_COUNT":  &maxStatementCount,
			"GO_QUERY_RESULT_FORMAT": &json,
		},
	}
}

var hostRe = regexp.MustCompile(`(?i)^.+.snowflakecomputing\.com$`)

func validHost(h string) error {
	hasProtocol := strings.Contains(h, "://")
	missingDomain := !hostRe.MatchString(h)

	if hasProtocol && missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com and not include a protocol)", h)
	} else if hasProtocol {
		return fmt.Errorf("invalid host %q (must not include a protocol)", h)
	} else if missingDomain {
		return fmt.Errorf("invalid host %q (must end in snowflakecomputing.com)", h)
	}

	return nil
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"account", c.Account},
		{"host", c.Host},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
		{"schema", c.Schema},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.Advanced.UpdateDelay != "" {
		parsed, err := time.ParseDuration(c.Advanced.UpdateDelay)
		if err != nil {
			return fmt.Errorf("could not parse Update Delay '%s': must be a valid Go duration string", c.Advanced.UpdateDelay)
		}

		if parsed < 0 {
			return fmt.Errorf("update delay '%s' must not be negative", c.Advanced.UpdateDelay)
		}
	}

	return validHost(c.Host)
}

type tableConfig struct {
	Table string `json:"table" jsonschema_extras:"x-collection-name=true"`
	Delta bool   `json:"delta_updates,omitempty"`
}

func newTableConfig(ep *sql.Endpoint) sql.Resource {
	return &tableConfig{}
}

func (c tableConfig) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("expected table")
	}
	return nil
}

func (c tableConfig) Path() sql.TablePath {
	return []string{c.Table}
}

func (c tableConfig) DeltaUpdates() bool {
	return c.Delta
}

func (c tableConfig) GetAdditionalSql() string {
	return ""
}

// The Snowflake driver Params map uses string pointers as values, which is what this is used for.
var trueString = "true"

// newSnowflakeDriver creates a new Driver for Snowflake.
func newSnowflakeDriver() *sql.Driver {
	return &sql.Driver{
		DocumentationURL: "https://go.estuary.dev/materialize-snowflake",
		EndpointSpecType: new(config),
		ResourceSpecType: new(tableConfig),
		NewEndpoint: func(ctx context.Context, raw json.RawMessage, tenant string) (*sql.Endpoint, error) {
			var parsed = new(config)
			if err := pf.UnmarshalStrict(raw, parsed); err != nil {
				return nil, fmt.Errorf("parsing Snowflake configuration: %w", err)
			}

			var dsn = parsed.ToURI(tenant)

			log.WithFields(log.Fields{
				"host":     parsed.Host,
				"user":     parsed.User,
				"database": parsed.Database,
				"schema":   parsed.Schema,
				"tenant":   tenant,
			}).Info("opening Snowflake")

			var metaBase sql.TablePath
			var metaSpecs, metaCheckpoints = sql.MetaTables(metaBase)

			return &sql.Endpoint{
				Config:              parsed,
				Dialect:             snowflakeDialect,
				MetaSpecs:           &metaSpecs,
				MetaCheckpoints:     &metaCheckpoints,
				Client:              client{uri: dsn},
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

	if db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant)); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		var sfError *sf.SnowflakeError
		if errors.As(err, &sfError) {
			switch sfError.Number {
			case 260008:
				// This is the error if the host URL has an incorrect account identifier. The error
				// message from the Snowflake driver will accurately report that the account name is
				// incorrect, but would be confusing for a user because we have a separate "Account"
				// input field. We want to be specific here and report that it is the account
				// identifier in the host URL.
				err = fmt.Errorf("incorrect account identifier %q in host URL", strings.TrimSuffix(cfg.Host, ".snowflakecomputing.com"))
			case 390100:
				err = fmt.Errorf("incorrect username or password")
			case 390201:
				// This means "doesn't exist or not authorized", and we don't have a way to
				// distinguish between that for the database, schema, or warehouse. The snowflake
				// error message in these cases is fairly decent fortunately.
			case 390189:
				err = fmt.Errorf("role %q does not exist", cfg.Role)
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

func (c client) DropNotNullForColumn(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
		tableIdentifier,
		columnIdentifier,
	)

	if !dryRun {
		if err := c.withDB(func(db *stdsql.DB) error {
			// Snowflake columns that are already NOT NULL will return successfully.
			_, err := db.ExecContext(ctx, query)
			return err
		}); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier string, columnIdentifier string, columnDDL string) (string, error) {
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN %s %s",
		tableIdentifier,
		columnIdentifier,
		columnDDL,
	)

	if !dryRun {
		if err := c.withDB(func(db *stdsql.DB) error {
			if _, err := db.ExecContext(ctx, query); err != nil {
				var sfError *sf.SnowflakeError
				// There is no documentation for error number 1430 anywhere that I can find so I am not
				// 100% sure if this error number is exclusively used for "column already exists" types
				// of errors. So to be safe this error condition will match on the reported SQLState and
				// the error text itself that has been observed for a table with a column that already
				// exists.
				if errors.As(err, &sfError) &&
					sfError.Number == 1430 &&
					sfError.SQLState == "42601" &&
					strings.HasSuffix(err.Error(), "already exists") {
					log.WithFields(log.Fields{
						"table":  tableIdentifier,
						"column": columnIdentifier,
						"ddl":    columnDDL,
						"err":    err.Error(),
					}).Debug("column already existed in table")
					err = nil
				}
				return err
			}

			return nil
		}); err != nil {
			return "", err
		}
	}

	return query, nil
}

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		specB64, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
		return err
	})
	return
}

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	var err = c.withDB(func(db *stdsql.DB) error {
		var err error
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, base64.StdEncoding.DecodeString)
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("snowflake", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

type transactor struct {
	cfg *config
	// Variables exclusively used by Load.
	load struct {
		conn *stdsql.Conn
	}
	// Variables exclusively used by Store.
	store struct {
		conn  *stdsql.Conn
		fence *sql.Fence
		round int
	}
	bindings    []*binding
	updateDelay time.Duration
}

func newTransactor(
	ctx context.Context,
	ep *sql.Endpoint,
	fence sql.Fence,
	bindings []sql.Table,
) (_ pm.Transactor, err error) {
	var cfg = ep.Config.(*config)

	var d = &transactor{
		cfg: cfg,
	}

	if cfg.Advanced.UpdateDelay != "" {
		// UpdateDelay has already been validated in (*config).Validate. This parsing is not
		// expected to fail.
		d.updateDelay, err = time.ParseDuration(cfg.Advanced.UpdateDelay)
		if err != nil {
			return nil, fmt.Errorf("could not parse UpdateDelay '%s'", cfg.Advanced.UpdateDelay)
		}
	}

	d.store.fence = &fence

	// Establish connections.
	if db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant)); err != nil {
		return nil, fmt.Errorf("load stdsql.Open: %w", err)
	} else if d.load.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("load db.Conn: %w", err)
	}
	if db, err := stdsql.Open("snowflake", cfg.ToURI(ep.Tenant)); err != nil {
		return nil, fmt.Errorf("store stdsql.Open: %w", err)
	} else if d.store.conn, err = db.Conn(ctx); err != nil {
		return nil, fmt.Errorf("store db.Conn: %w", err)
	}

	// Create stage for file-based transfers.
	if _, err = d.load.conn.ExecContext(ctx, createStageSQL); err != nil {
		return nil, fmt.Errorf("creating transfer stage : %w", err)
	}

	for _, binding := range bindings {
		if err = d.addBinding(ctx, binding); err != nil {
			return nil, fmt.Errorf("%v: %w", binding, err)
		}
	}

	return d, nil
}

type binding struct {
	target sql.Table
	// Variables exclusively used by Load.
	load struct {
		loadQuery string
		stage     *stagedFile
	}
	// Variables accessed by Prepare, Store, and Commit.
	store struct {
		stage     *stagedFile
		mergeInto string
		copyInto  string
		mustMerge bool
	}
}

type TableWithUUID struct {
	Table      *sql.Table
	RandomUUID string
}

func (t *transactor) addBinding(ctx context.Context, target sql.Table) error {
	var d = new(binding)
	var err error
	d.target = target

	d.load.stage = newStagedFile(os.TempDir())
	d.store.stage = newStagedFile(os.TempDir())

	if d.load.loadQuery, err = RenderTableWithRandomUUIDTemplate(target, d.load.stage.uuid, tplLoadQuery); err != nil {
		return fmt.Errorf("loadQuery template: %w", err)
	}
	if d.store.copyInto, err = RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, tplCopyInto); err != nil {
		return fmt.Errorf("copyInto template: %w", err)
	}
	if d.store.mergeInto, err = RenderTableWithRandomUUIDTemplate(target, d.store.stage.uuid, tplMergeInto); err != nil {
		return fmt.Errorf("mergeInto template: %w", err)
	}

	t.bindings = append(t.bindings, d)
	return nil
}

func (d *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	var ctx = it.Context()

	for it.Next() {
		var b = d.bindings[it.Binding]

		if err := b.load.stage.start(ctx, d.load.conn); err != nil {
			return err
		} else if converted, err := b.target.ConvertKey(it.Key); err != nil {
			return fmt.Errorf("converting Load key: %w", err)
		} else if err = b.load.stage.encodeRow(converted); err != nil {
			return fmt.Errorf("encoding Load key to scratch file: %w", err)
		}
	}
	if it.Err() != nil {
		return it.Err()
	}

	var subqueries []string
	for _, b := range d.bindings {
		if !b.load.stage.started {
			// Pass.
		} else if err := b.load.stage.flush(); err != nil {
			return fmt.Errorf("load.stage(): %w", err)
		} else {
			subqueries = append(subqueries, b.load.loadQuery)
		}
	}

	if len(subqueries) == 0 {
		return nil // Nothing to load.
	}
	var loadAllSQL = strings.Join(subqueries, "\nUNION ALL\n") + ";"

	// Issue a join of the target table and (now staged) load keys,
	// and send results to the |loaded| callback.
	rows, err := d.load.conn.QueryContext(sf.WithStreamDownloader(ctx), loadAllSQL)
	if err != nil {
		return fmt.Errorf("querying Load documents: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var binding int
		var document stdsql.RawBytes

		if err = rows.Scan(&binding, &document); err != nil {
			return fmt.Errorf("scanning Load document: %w", err)
		} else if err = loaded(binding, json.RawMessage(document)); err != nil {
			return fmt.Errorf("sending loaded document for table %q: %w", d.bindings[binding].target.Identifier, err)
		}
	}
	if err = rows.Err(); err != nil {
		return fmt.Errorf("querying Loads: %w", err)
	}

	return nil
}

func (d *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	d.store.round++

	for it.Next() {
		var b = d.bindings[it.Binding]

		if err := b.store.stage.start(it.Context(), d.store.conn); err != nil {
			return nil, err
		} else if converted, err := b.target.ConvertAll(it.Key, it.Values, it.RawJSON); err != nil {
			return nil, fmt.Errorf("converting Store: %w", err)
		} else if err = b.store.stage.encodeRow(converted); err != nil {
			return nil, fmt.Errorf("encoding Store to scratch file: %w", err)
		}

		if it.Exists {
			b.store.mustMerge = true
		}
	}

	return func(ctx context.Context, runtimeCheckpoint *protocol.Checkpoint, _ <-chan struct{}) (*pf.ConnectorState, pf.OpFuture) {
		var err error
		if d.store.fence.Checkpoint, err = runtimeCheckpoint.Marshal(); err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("marshalling checkpoint: %w", err))
		}

		return nil, sql.CommitWithDelay(ctx, d.store.round == 1, d.updateDelay, it.Total, d.commit)
	}, nil
}

func (d *transactor) commit(ctx context.Context) error {
	var txn, err = d.store.conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store.conn.BeginTx: %w", err)
	}
	defer txn.Rollback()

	// First we must validate the fence has not been modified.
	var fenceUpdate strings.Builder
	if err := tplUpdateFence.Execute(&fenceUpdate, d.store.fence); err != nil {
		return fmt.Errorf("evaluating fence template: %w", err)
	} else if _, err = txn.ExecContext(ctx, fenceUpdate.String()); err != nil {
		err = fmt.Errorf("txn.Exec: %w", err)

		// Give recommendation to user for resolving timeout issues
		if strings.Contains(err.Error(), "timeout") {
			return fmt.Errorf("fence.Update: %w  (ensure LOCK_TIMEOUT and STATEMENT_TIMEOUT_IN_SECONDS are at least ten minutes)", err)
		}

		return err
	}

	for _, b := range d.bindings {
		if !b.store.stage.started {
			// No table update required
		} else if err := b.store.stage.flush(); err != nil {
			return err
		} else if !b.store.mustMerge {
			// We can issue a faster COPY INTO the target table.
			if _, err = d.store.conn.ExecContext(ctx, b.store.copyInto); err != nil {
				return fmt.Errorf("copying Store documents into table %q: %w", b.target.Identifier, err)
			}
		} else {
			// We must MERGE into the target table.
			if _, err = d.store.conn.ExecContext(ctx, b.store.mergeInto); err != nil {
				return fmt.Errorf("merging Store documents into table %q: %w", b.target.Identifier, err)
			}
		}

		// Reset for next transaction.
		b.store.mustMerge = false
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("txn.Commit: %w", err)
	}

	return nil
}

func (d *transactor) Destroy() {
	d.load.conn.Close()
	d.store.conn.Close()
}

func main() {
	// gosnowflake also uses logrus for logging and the logs it produces may be confusing when
	// intermixed with our connector logs. We disable the gosnowflake logger here and log as needed
	// when handling errors from the sql driver.
	sf.GetLogger().SetOutput(io.Discard)
	boilerplate.RunMain(newSnowflakeDriver())
}
