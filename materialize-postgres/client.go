package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/jackc/pgconn"

	_ "github.com/jackc/pgx/v4/stdlib"
)

type client struct {
	db  *stdsql.DB
	cfg *config
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("pgx", cfg.ToURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: cfg,
	}, nil
}

func (c *client) PreReqs(ctx context.Context) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := c.db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var pgErr *pgconn.PgError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "28P01":
				err = fmt.Errorf("incorrect username or password")
			case "3D000":
				err = fmt.Errorf("database %q does not exist", c.cfg.Database)
			}
		} else if errors.As(err, &netConnErr) {
			if netConnErr.IsNotFound {
				err = fmt.Errorf("host at address %q cannot be found", c.cfg.Address)
			}
		} else if errors.As(err, &netOpErr) {
			if netOpErr.Timeout() {
				err = fmt.Errorf("connection to host at address %q timed out (incorrect host or port?)", c.cfg.Address)
			}
		}

		errs.Err(err)
	}

	return errs
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	catalog := c.cfg.Database
	if catalog == "" {
		// An endpoint-level database configuration is not required, so query for the active
		// database if that's the case.
		if err := c.db.QueryRowContext(ctx, "select current_database();").Scan(&catalog); err != nil {
			return nil, fmt.Errorf("querying for connected database: %w", err)
		}
	}

	return sql.StdFetchInfoSchema(ctx, c.db, pgDialect, catalog, resourcePaths)
}

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	_, err := c.db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...)
	return err
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	var res tableConfig
	if tc.ResourceConfigJson != nil {
		if err := pf.UnmarshalStrict(tc.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("unmarshalling resource binding for bound collection %q: %w", tc.Source.String(), err)
		}
	}

	statements := []string{}
	if res.AdditionalSql != "" {
		statements = append(statements, txnStatements(tc.TableCreateSql, res.AdditionalSql))
	} else {
		statements = append(statements, tc.TableCreateSql)
	}

	_, err := c.db.ExecContext(ctx, strings.Join(statements, "\n"))
	return err
}

func (c *client) ReplaceTable(ctx context.Context, tr sql.TableReplace) (string, boilerplate.ActionApplyFn, error) {
	var res tableConfig
	if tr.ResourceConfigJson != nil {
		if err := pf.UnmarshalStrict(tr.ResourceConfigJson, &res); err != nil {
			return "", nil, fmt.Errorf("unmarshalling resource binding for bound collection %q: %w", tr.Source.String(), err)
		}
	}

	statements := []string{}
	if res.AdditionalSql != "" {
		statements = append(statements, txnStatements(tr.TableReplaceSql, res.AdditionalSql))
	} else {
		statements = append(statements, tr.TableReplaceSql)
	}

	query := strings.Join(statements, "\n")

	return query, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, query)
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var alterColumnStmtBuilder strings.Builder
	if err := tplAlterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
		return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
	}
	alterColumnStmt := alterColumnStmtBuilder.String()

	return alterColumnStmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, alterColumnStmt)
		return err
	}, nil
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (string, string, error) {
	return sql.StdFetchSpecAndVersion(ctx, c.db, specs, materialization)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.StdInstallFence(ctx, c.db, checkpoints, fence, base64.StdEncoding.DecodeString)
}

func (c *client) Close() {
	c.db.Close()
}

func txnStatements(stmts ...string) string {
	return strings.Join(slices.Insert([]string{"BEGIN;", "COMMIT;"}, 1, stmts...), "\n")
}
