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

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/jackc/pgconn"
)

type client struct {
	uri string
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {
	cfg := ep.Config.(*config)

	db, err := stdsql.Open("pgx", c.uri)
	if err != nil {
		return "", err
	}
	defer db.Close()

	catalog := cfg.Database
	if catalog == "" {
		// An endpoint-level database configuration is not required, so query for the active
		// database if that's the case.
		if err := db.QueryRowContext(ctx, "select current_database();").Scan(&catalog); err != nil {
			return "", fmt.Errorf("querying for connected database: %w", err)
		}
	}

	resolved, err := sql.ResolveActions(ctx, db, actions, pgDialect, catalog)
	if err != nil {
		return "", fmt.Errorf("resolving apply actions: %w", err)
	}

	// Convenience for wrapping some number of statements in a transaction block.
	txnStatements := func(stmts ...string) string {
		return strings.Join(slices.Insert([]string{"BEGIN;", "COMMIT;"}, 1, stmts...), "\n")
	}

	statements := []string{}
	for _, tc := range resolved.CreateTables {
		var res tableConfig
		if tc.ResourceConfigJson != nil {
			if err = pf.UnmarshalStrict(tc.ResourceConfigJson, &res); err != nil {
				return "", fmt.Errorf("unmarshalling resource binding for bound collection %q: %w", tc.Source.String(), err)
			}
		}

		if res.AdditionalSql != "" {
			statements = append(statements, txnStatements(tc.TableCreateSql, res.AdditionalSql))
		} else {
			statements = append(statements, tc.TableCreateSql)
		}
	}

	for _, ta := range resolved.AlterTables {
		var alterColumnStmt strings.Builder
		if err := tplAlterTableColumns.Execute(&alterColumnStmt, ta); err != nil {
			return "", fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		statements = append(statements, alterColumnStmt.String())
	}

	for _, tr := range resolved.ReplaceTables {
		var res tableConfig
		if tr.ResourceConfigJson != nil {
			if err = pf.UnmarshalStrict(tr.ResourceConfigJson, &res); err != nil {
				return "", fmt.Errorf("unmarshalling resource binding for bound collection %q: %w", tr.Source.String(), err)
			}
		}

		if res.AdditionalSql != "" {
			statements = append(statements, txnStatements(tr.TableReplaceSql, res.AdditionalSql))
		} else {
			statements = append(statements, txnStatements(tr.TableReplaceSql))
		}
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

	if db, err := stdsql.Open("pgx", cfg.ToURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(ctx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var pgErr *pgconn.PgError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "28P01":
				err = fmt.Errorf("incorrect username or password")
			case "3D000":
				err = fmt.Errorf("database %q does not exist", cfg.Database)
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
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, base64.StdEncoding.DecodeString)
		return err
	})
	return fence, err
}

func (c client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("pgx", c.uri)
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}
