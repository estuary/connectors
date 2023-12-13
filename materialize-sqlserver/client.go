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
	"github.com/estuary/flow/go/protocols/flow"
	mssqldb "github.com/microsoft/go-mssqldb"
)

type client struct {
	db  *stdsql.DB
	cfg *config
	ep  *sql.Endpoint
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("sqlserver", cfg.ToURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: cfg,
		ep:  ep,
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
		var sqlServerErr *mssqldb.Error
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &sqlServerErr) {
			// See SQLServer error reference: https://learn.microsoft.com/en-us/sql/relational-databases/errors-events/database-engine-events-and-errors?view=sql-server-2017
			switch sqlServerErr.Number {
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

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (is *boilerplate.InfoSchema, err error) {
	return sql.StdFetchInfoSchema(ctx, c.db, c.ep.Dialect, c.cfg.Database, resourcePaths)
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var statements []string

	// SQL Server supports adding multiple columns in a single statement, but only a single
	// modification per statement.
	if len(ta.AddColumns) > 0 {
		var addColumnsStmt strings.Builder
		if err := renderTemplates(c.ep.Dialect)["alterTableColumns"].Execute(&addColumnsStmt, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		statements = append(statements, addColumnsStmt.String())
	}

	// Dropping a NOT NULL constraint requires re-stating the full field definition without the NOT
	// NULL part. The `data_type` from the information_schema.columns can mostly be used as the
	// column's definition, except for some column types that have a length component. These must be
	// augmented with the `character_maximum_length`.
	typesWithLengths := []string{"VARCHAR", "NVARCHAR", "VARBINARY"}
	for _, dn := range ta.DropNotNulls {
		ddl := strings.ToUpper(dn.Type)
		if slices.Contains(typesWithLengths, ddl) {
			if dn.CharacterMaxLength == -1 {
				ddl = fmt.Sprintf("%s(MAX)", ddl)
			} else {
				ddl = fmt.Sprintf("%s(%d)", ddl, dn.CharacterMaxLength)
			}
		}

		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ALTER COLUMN %s %s;",
			ta.Identifier,
			c.ep.Dialect.Identifier(dn.Name),
			ddl,
		))
	}

	return strings.Join(statements, "\n"), func(ctx context.Context) error {
		for _, stmt := range statements {
			if _, err := c.db.ExecContext(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) ReplaceTable(ctx context.Context, tr sql.TableReplace) (string, boilerplate.ActionApplyFn, error) {
	return tr.TableReplaceSql, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, tr.TableReplaceSql)
		return err
	}, nil
}

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	_, err := c.db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...)
	return err
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	return installFence(ctx, c.ep.Dialect, c.db, checkpoints, fence, base64.StdEncoding.DecodeString)
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization flow.Materialization) (string, string, error) {
	return sql.StdFetchSpecAndVersion(ctx, c.db, specs, materialization)
}

func (c *client) Close() {
	c.db.Close()
}
