package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

type client struct {
	uri string
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {
	db, err := stdsql.Open("snowflake", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Currently the "catalog" is always the database value from the endpoint configuration in all
	// capital letters. It is possible to connect to Snowflake databases that aren't in all caps by
	// quoting the database name. We don't do that currently and it's hard to say if we ever will
	// need to, although that means we can't connect to databases that aren't in the Snowflake
	// default ALL CAPS format. The practical implications are that if somebody puts in a database
	// like "database", we'll actually connect to the database "DATABASE", and so we can't rely on
	// the endpoint configuration value entirely and will query it here to be future-proof.
	var catalog string
	if err := db.QueryRowContext(ctx, "SELECT CURRENT_DATABASE()").Scan(&catalog); err != nil {
		return "", fmt.Errorf("querying for connected database: %w", err)
	}

	resolved, err := sql.ResolveActions(ctx, db, actions, ep.Dialect, catalog)
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
		statements = append(statements, tr.TableReplaceSql)
	}

	// The spec will get updated last, after all the other actions are complete, but include it in
	// the description of actions.
	action := strings.Join(append(statements, updateSpec.QueryString), "\n")
	if req.DryRun {
		return action, nil
	}

	if len(resolved.ReplaceTables) > 0 {
		if err := sql.StdIncrementFence(ctx, db, ep, req.Materialization.Name.String()); err != nil {
			return "", err
		}
	}

	// Execute statements in parallel for efficiency. Each statement acts on a single table, and
	// everything that needs to be done for a given table is contained in that statement.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, s := range statements {
		s := s
		group.Go(func() error {
			if _, err := db.ExecContext(groupCtx, s); err != nil {
				return fmt.Errorf("executing apply statement: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
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
		// Check for an active warehouse for the connection. If there is no default warehouse for
		// the user and the configuration did not set a warehouse, this may be `null`, and the user
		// needs to configure a specific warehouse to use.
		var currentWarehouse *string
		if err := db.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE();").Scan(&currentWarehouse); err != nil {
			errs.Err(fmt.Errorf("checking for active warehouse: %w", err))
		} else {
			if currentWarehouse == nil {
				errs.Err(fmt.Errorf("no warehouse configured and default warehouse not set for user '%s': must set a value for 'Warehouse' in the endpoint configuration", cfg.User))
			}
		}

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
