package main

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	dbsqlerr "github.com/databricks/databricks-sql-go/errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"golang.org/x/sync/errgroup"
)

type client struct {
	uri string
}

func (c client) InfoSchema(ctx context.Context, ep *sql.Endpoint, resourcePaths [][]string) (is *boilerplate.InfoSchema, err error) {
	cfg := ep.Config.(*config)

	schemaLiterals := []string{ep.Dialect.Literal(cfg.SchemaName)}
	for _, p := range resourcePaths {
		loc := ep.Dialect.TableLocator(p)
		schemaLiterals = append(schemaLiterals, ep.Dialect.Literal(loc.TableSchema))
	}

	slices.Sort(schemaLiterals)
	schemaLiterals = slices.Compact(schemaLiterals)

	is = boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(ep.Dialect.TableLocator),
		ep.Dialect.ColumnLocator,
	)

	if err := c.withDB(func(db *stdsql.DB) error {
		rows, err := db.QueryContext(ctx, fmt.Sprintf(`
			select table_schema, table_name, column_name, is_nullable, data_type, character_maximum_length
			from system.information_schema.columns
			where table_catalog = %s
			and table_schema in (%s);
			`,
			ep.Dialect.Literal(cfg.CatalogName),
			strings.Join(schemaLiterals, ","),
		))
		if err != nil {
			return err
		}
		defer rows.Close()

		type columnRow struct {
			TableSchema            string
			TableName              string
			ColumnName             string
			IsNullable             string
			DataType               string
			CharacterMaximumLength stdsql.NullInt64
		}

		for rows.Next() {
			var c columnRow
			if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.IsNullable, &c.DataType, &c.CharacterMaximumLength); err != nil {
				return err
			}

			is.PushField(boilerplate.EndpointField{
				Name:               c.ColumnName,
				Nullable:           strings.EqualFold(c.IsNullable, "yes"),
				Type:               c.DataType,
				CharacterMaxLength: int(c.CharacterMaximumLength.Int64),
			}, c.TableSchema, c.TableName)
		}

		return rows.Err()
	}); err != nil {
		return nil, err
	}

	return
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("databricks", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Build the list of schemas included in this update.
	var schemas []string
	for _, t := range actions.CreateTables {
		if !slices.Contains(schemas, t.InfoLocation.TableSchema) {
			schemas = append(schemas, t.InfoLocation.TableSchema)
		}
	}
	for _, t := range actions.AlterTables {
		if !slices.Contains(schemas, t.InfoLocation.TableSchema) {
			schemas = append(schemas, t.InfoLocation.TableSchema)
		}
	}

	existing, err := fetchExistingColumns(ctx, db, databricksDialect, cfg.CatalogName, schemas)
	if err != nil {
		return "", fmt.Errorf("fetching columns: %w", err)
	}

	resolved, err := sql.FilterActions(actions, databricksDialect, existing)
	if err != nil {
		return "", err
	}

	// Build up the list of actions for logging. These won't be executed directly, since Databricks
	// apparently doesn't support multi-statement queries or dropping nullability for multiple
	// columns in a single statement, and throws errors on concurrent table updates.
	actionList := []string{}
	for _, tc := range resolved.CreateTables {
		actionList = append(actionList, tc.TableCreateSql)
	}
	for _, ta := range resolved.AlterTables {
		if len(ta.AddColumns) > 0 {
			var addColumnsStmt strings.Builder
			if err := tplAlterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
				return "", fmt.Errorf("rendering alter table columns statement: %w", err)
			}
			actionList = append(actionList, addColumnsStmt.String())
		}
		for _, dn := range ta.DropNotNulls {
			actionList = append(actionList, fmt.Sprintf(
				"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
				ta.Identifier,
				dn.Identifier,
			))
		}
	}
	for _, tr := range resolved.ReplaceTables {
		actionList = append(actionList, tr.TableReplaceSql)
	}
	action := strings.Join(append(actionList, updateSpec.QueryString), "\n")
	if req.DryRun {
		return action, nil
	}

	// Execute actions for each table involved separately. Concurrent actions can't be done on the
	// same table without throwing errors, so each goroutine handles the actions only for that
	// table, looping over them if needed until they are done. This is going to look pretty similar
	// to building the actions list, except this time we are actually running the queries.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, tc := range resolved.CreateTables {
		tc := tc
		group.Go(func() error {
			if _, err := db.ExecContext(groupCtx, tc.TableCreateSql); err != nil {
				return fmt.Errorf("executing table create statement: %w", err)
			}
			return nil
		})
	}

	for _, ta := range resolved.AlterTables {
		ta := ta

		group.Go(func() error {
			if len(ta.AddColumns) > 0 {
				var addColumnsStmt strings.Builder
				if err := tplAlterTableColumns.Execute(&addColumnsStmt, ta); err != nil {
					return fmt.Errorf("rendering alter table columns statement: %w", err)
				}
				if _, err := db.ExecContext(groupCtx, addColumnsStmt.String()); err != nil {
					return fmt.Errorf("executing table add columns statement: %w", err)
				}
			}
			for _, dn := range ta.DropNotNulls {
				q := fmt.Sprintf(
					"ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL;",
					ta.Identifier,
					dn.Identifier,
				)
				if _, err := db.ExecContext(groupCtx, q); err != nil {
					return fmt.Errorf("executing table drop not null statement: %w", err)
				}
			}
			return nil
		})
	}

	// TODO(whb): There's a chance that if a previous driver checkpoint was persisted before the
	// actual data load completed, these table replacements will not be compatible with the data
	// referenced by the persisted driver checkpoint. This is pretty unlikely to happen, and will be
	// resolved when we incorporate the use of state keys, which incorporate the backfill counter.
	for _, tr := range resolved.ReplaceTables {
		tr := tr
		group.Go(func() error {
			if _, err := db.ExecContext(groupCtx, tr.TableReplaceSql); err != nil {
				return fmt.Errorf("executing table create statement: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := db.ExecContext(ctx, updateSpec.QueryString); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return action, nil
}

func (c client) PreReqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then. Note that this should be long enough to allow
	// for an automatically shut down instance to be started up again
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if db, err := stdsql.Open("databricks", c.uri); err != nil {
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
