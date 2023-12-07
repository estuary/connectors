package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	mysql "github.com/go-sql-driver/mysql"
)

type client struct {
	uri string
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {
	db, err := stdsql.Open("mysql", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	resolved, err := sql.ResolveActions(ctx, db, actions, ep.Dialect, "def")
	if err != nil {
		return "", fmt.Errorf("resolving apply actions: %w", err)
	}

	statements := []string{}
	for _, tc := range resolved.CreateTables {
		statements = append(statements, tc.TableCreateSql)
	}

	for _, ta := range resolved.AlterTables {
		var alterColumnStmt strings.Builder
		if err := renderTemplates(ep.Dialect)["alterTableColumns"].Execute(&alterColumnStmt, ta); err != nil {
			return "", fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		statements = append(statements, alterColumnStmt.String())
	}

	for _, tr := range resolved.ReplaceTables {
		// `TableReplaceSql` is only the "create table" part, and we need to also include the
		// statement to drop the table first. Also see additional comments in sqlgen.go.
		statements = append(statements, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tr.Identifier))
		statements = append(statements, tr.TableReplaceSql)
	}

	action := strings.Join(append(statements, updateSpec.QueryString), "\n")
	if req.DryRun {
		return action, nil
	}

	if len(resolved.ReplaceTables) > 0 {
		if err := sql.StdIncrementFence(ctx, db, ep, req.Materialization.Name.String()); err != nil {
			return "", err
		}
	}

	for _, s := range statements {
		if _, err := db.ExecContext(ctx, s); err != nil {
			// Handling for errors resulting from identifiers being too long.
			// TODO(whb): At some point we could consider moving this into `Validate` to check
			// identifier lengths, rather than passing validate but failing in apply. I'm holding
			// off on that for now since I don't know if the 64 character limit is universal, or
			// specific to certain versions of MySQL/MariaDB.
			var mysqlErr *mysql.MySQLError
			if errors.As(err, &mysqlErr) {
				// See MySQL error reference: https://dev.mysql.com/doc/mysql-errors/5.7/en/error-reference-introduction.html
				switch mysqlErr.Number {
				case 1059:
					err = fmt.Errorf("%w.\nPossible resolutions include:\n%s\n%s\n%s",
						err,
						"1. Adding a projection to rename this field, see https://go.estuary.dev/docs-projections",
						"2. Exclude the field, see https://go.estuary.dev/docs-field-selection",
						"3. Disable the corresponding binding")
				}
			}

			return "", fmt.Errorf("executing statement: %w", err)
		}
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return action, nil
}

func (c client) PreReqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if db, err := stdsql.Open("mysql", cfg.ToURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var mysqlErr *mysql.MySQLError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &mysqlErr) {
			// See MySQL error reference: https://dev.mysql.com/doc/mysql-errors/5.7/en/error-reference-introduction.html
			switch mysqlErr.Number {
			case 1045:
				err = fmt.Errorf("incorrect username or password (%d): %s", mysqlErr.Number, mysqlErr.Message)
			case 1049:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", cfg.Database, mysqlErr.Number, mysqlErr.Message)
			case 1044:
				err = fmt.Errorf("database %q cannot be accessed, it might not exist or you do not have permission to access it (%d): %s", cfg.Database, mysqlErr.Number, mysqlErr.Message)
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
	} else if conn, err := db.Conn(ctx); err != nil {
		errs.Err(fmt.Errorf("could not create a connection to database %q at %q: %w", cfg.Database, cfg.Address, err))
	} else {
		var row = conn.QueryRowContext(ctx, "SELECT @@GLOBAL.local_infile;")
		var localInFileEnabled bool

		if err := row.Scan(&localInFileEnabled); err != nil {
			errs.Err(fmt.Errorf("could not read `local_infile` global variable: %w", err))
		} else if !localInFileEnabled {
			errs.Err(fmt.Errorf("`local_infile` global variable must be enabled on your mysql server. You can enable this using `SET GLOBAL local_infile = true`"))
		} else {
			db.Close()
		}
	}

	return errs
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
	var db, err = stdsql.Open("mysql", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}
