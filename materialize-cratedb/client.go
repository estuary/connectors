package main

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type client struct {
	db  *stdsql.DB
	cfg config
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	db, err := stdsql.Open("pgx", ep.Config.ToURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: ep.Config,
	}, nil
}

func preReqs(ctx context.Context, conf config, tenant string) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	cfg := conf

	db, err := stdsql.Open("pgx", cfg.ToURI())
	if err != nil {
		errs.Err(err)
		return errs
	}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// Fixme (CrateDB - Ivan): We early return here on purpose to avoid the
	// db.PingContext below, for some reason it quickly returns
	// bad connection. We should probably look it up.
	return errs

	if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var pgErr *pgconn.ConnectError
		if errors.As(err, &pgErr) {
			err = pgErr.Unwrap()
			if errStr := err.Error(); strings.Contains(errStr, "(SQLSTATE 28P01)") {
				err = fmt.Errorf("incorrect username or password")
			} else if strings.Contains(errStr, "(SQLSTATE 3D000") {
				err = fmt.Errorf("database %q does not exist", cfg.Database)
			} else if strings.Contains(errStr, "context deadline exceeded") {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", cfg.Address)
			}
		}

		errs.Err(err)
	}

	return errs
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	catalog := c.cfg.Database
	if catalog == "" {
		// An endpoint-level database configuration is not required, so query for the active
		// database if that's the case.
		if err := c.db.QueryRowContext(ctx, "select current_database();").Scan(&catalog); err != nil {
			return fmt.Errorf("querying for connected database: %w", err)
		}
	}

	return sql.StdPopulateInfoSchema(ctx, is, c.db, crateDialect, catalog, resourcePaths)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	var res tableConfig
	if tc.Resource != nil {
		res = tc.Resource.(tableConfig)
	}

	txn, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("db.BeginTx: %w", err)
	}
	defer txn.Rollback()

	if _, err := txn.ExecContext(ctx, tc.TableCreateSql); err != nil {
		return fmt.Errorf("executing CREATE TABLE statement: %w", err)
	}

	if res.AdditionalSql != "" {
		if _, err := txn.ExecContext(ctx, res.AdditionalSql); err != nil {
			return fmt.Errorf("executing additional SQL statement '%s': %w", res.AdditionalSql, err)
		}

		log.WithFields(log.Fields{
			"table": tc.Identifier,
			"query": res.AdditionalSql,
		}).Info("executed AdditionalSql")
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	return nil
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", crateDialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string

	if len(ta.AddColumns) > 0 {
		var alterColumnStmtBuilder strings.Builder
		if err := tplAlterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		alterColumnStmt := alterColumnStmtBuilder.String()

		stmts = append(stmts, alterColumnStmt)
	}

	if len(ta.ColumnTypeChanges) > 0 {
		if steps, err := sql.StdColumnTypeMigrations(ctx, crateDialect, ta.Table, ta.ColumnTypeChanges); err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		} else {
			stmts = append(stmts, steps...)
		}
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (c *client) ListSchemas(ctx context.Context) ([]string, error) {
	return sql.StdListSchemas(ctx, c.db)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.StdInstallFence(ctx, c.db, checkpoints, fence)
}

func (c *client) Close() {
	c.db.Close()
}
