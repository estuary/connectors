package main

import (
	"context"
	stdsql "database/sql"
	"fmt"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"slices"
	"strings"
)

type client struct {
	db        *stdsql.DB
	cfg       *config
	ep        *sql.Endpoint
	templates templates
}

func openDB(uri string) (*stdsql.DB, error) {
	return stdsql.Open("trino", uri)
}

func connectToDb(ctx context.Context, uri string) (*stdsql.Conn, error) {
	db, err := openDB(uri)
	if err != nil {
		return nil, fmt.Errorf("stdsql.Open failed: %w", err)
	}
	return db.Conn(ctx)
}

func newClient(_ context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

	db, err := openDB(cfg.ToURI())
	if err != nil {
		return nil, err
	}

	var dialect = starburstDialect(cfg.Schema)
	var templates = renderTemplates(dialect)

	return &client{
		db:        db,
		cfg:       cfg,
		ep:        ep,
		templates: templates,
	}, nil
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(c.ep.Dialect.TableLocator),
		c.ep.Dialect.ColumnLocator,
	)

	// Map the resource paths to an appropriate identifier for inclusion in the coming query.
	schemas := []string{c.ep.Dialect.Literal(c.cfg.Schema)}
	for _, p := range resourcePaths {
		loc := c.ep.Dialect.TableLocator(p)
		schemas = append(schemas, c.ep.Dialect.Literal(loc.TableSchema))
	}

	slices.Sort(schemas)
	schemas = slices.Compact(schemas)

	rows, err := c.db.QueryContext(ctx, fmt.Sprintf(`
		select table_schema, table_name, column_name, is_nullable, data_type, column_default
		from information_schema.columns
		where table_catalog = %s
		and table_schema in (%s)
		`,
		c.ep.Dialect.Literal(c.cfg.Catalog),
		strings.Join(schemas, ","),
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type columnRow struct {
		TableSchema   string
		TableName     string
		ColumnName    string
		IsNullable    string
		DataType      string
		ColumnDefault stdsql.NullString
	}

	for rows.Next() {
		var c columnRow
		if err := rows.Scan(&c.TableSchema, &c.TableName, &c.ColumnName, &c.IsNullable, &c.DataType, &c.ColumnDefault); err != nil {
			return nil, err
		}

		is.PushField(boilerplate.EndpointField{
			Name:               c.ColumnName,
			Nullable:           strings.EqualFold(c.IsNullable, "yes"),
			Type:               c.DataType,
			CharacterMaxLength: 0, // Trino does not have max length in information schema
			HasDefault:         c.ColumnDefault.Valid,
		}, c.TableSchema, c.TableName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return is, nil
}

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	// Normalize query by removing trailing ';' as Trino does not accept it.
	updateSpec.QueryString = strings.TrimRight(updateSpec.QueryString, ";")
	_, err := c.db.ExecContext(ctx, updateSpec.QueryString)
	return err
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

func (c *client) AlterTable(_ context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {

	var stmts []string

	if len(ta.AddColumns) > 0 {
		for _, col := range ta.AddColumns {
			var addColumnsStmt strings.Builder
			type AlterTableTemplateParams struct {
				TableIdentifier  string
				ColumnIdentifier string
				NullableDDL      string
			}
			alterColumnParams := AlterTableTemplateParams{ta.Identifier, col.Identifier, col.NullableDDL}
			if err := c.templates.alterTableColumns.Execute(&addColumnsStmt, alterColumnParams); err != nil {
				return "", nil, fmt.Errorf("rendering alter table columns statement failed: %w", err)
			}
			stmts = append(stmts, addColumnsStmt.String())
		}
	}
	if len(ta.DropNotNulls) > 0 {
		return "", nil, fmt.Errorf("dropping not nulls not supported")
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

func (c *client) PreReqs(ctx context.Context) *sql.PrereqErr {
	errs := &sql.PrereqErr{}
	err := c.db.PingContext(ctx)
	if err != nil {
		errs.Err(err)
	}
	return errs
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (spec, version string, err error) {

	fetchVersionAndSpecQuery, err := sql.RenderTableTemplate(specs, c.templates.fetchVersionAndSpec)
	if err != nil {
		return "", "", err
	}

	// QueryRowContext cannot be used until Trino driver issue is fixed https://github.com/trinodb/trino-go-client/issues/102
	rows, err := c.db.QueryContext(ctx,
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

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return sql.StdSQLExecStatements(ctx, c.db, statements)
}

// InstallFence is a no-op since materialize-starburst doesn't use fencing.
func (c *client) InstallFence(_ context.Context, _ sql.Table, _ sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, nil
}

func (c *client) Close() {
	c.db.Close()
}
