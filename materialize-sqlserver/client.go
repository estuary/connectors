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

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	mssqldb "github.com/microsoft/go-mssqldb"
)

type client struct {
	uri     string
	dialect sql.Dialect
}

func (c client) InfoSchema(ctx context.Context, ep *sql.Endpoint, resourcePaths [][]string) (is *boilerplate.InfoSchema, err error) {
	cfg := ep.Config.(*config)

	if err := c.withDB(func(db *stdsql.DB) error {
		var baseSchema string
		if err := db.QueryRowContext(ctx, "select schema_name()").Scan(&baseSchema); err != nil {
			return fmt.Errorf("querying schema for current user: %w", err)
		}

		is, err = sql.StdFetchInfoSchema(ctx, db, ep.Dialect, cfg.Database, baseSchema, resourcePaths)
		return err
	}); err != nil {
		return nil, err
	}
	return
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("sqlserver", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	resolved, err := sql.ResolveActions(ctx, db, actions, ep.Dialect, cfg.Database)
	if err != nil {
		return "", fmt.Errorf("resolving apply actions: %w", err)
	}

	statements := []string{}
	for _, tc := range resolved.CreateTables {
		statements = append(statements, tc.TableCreateSql)
	}

	for _, ta := range resolved.AlterTables {
		// SQL Server supports adding multiple columns in a single statement, but only a single
		// modification per statement.
		if len(ta.AddColumns) > 0 {
			var addColumnsStmt strings.Builder
			if err := renderTemplates(c.dialect)["alterTableColumns"].Execute(&addColumnsStmt, ta); err != nil {
				return "", fmt.Errorf("rendering alter table columns statement: %w", err)
			}
			statements = append(statements, addColumnsStmt.String())
		}

		for _, dn := range ta.DropNotNulls {
			statements = append(statements, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s %s;",
				ta.Identifier,
				dn.Identifier,
				dn.NullableDDL,
			))
		}
	}

	for _, tr := range resolved.ReplaceTables {
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

	if db, err := stdsql.Open("sqlserver", cfg.ToURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var sqlServerErr *mssqldb.Error
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &sqlServerErr) {
			// See SQLServer error reference: https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-2017
			switch sqlServerErr.Number {
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
		fence, err = installFence(ctx, c.dialect, db, checkpoints, fence, base64.StdEncoding.DecodeString)
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("sqlserver", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}
