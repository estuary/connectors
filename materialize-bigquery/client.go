package connector

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/blob"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql-v2"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	bigqueryClient *bigquery.Client
	cfg            config
	ep             *sql.Endpoint[config]
}

func newClient(ctx context.Context, ep *sql.Endpoint[config]) (sql.Client, error) {
	return ep.Config.client(ctx, ep)
}

func (c *client) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	rpDatasets := make(map[string]struct{})
	for _, p := range resourcePaths {
		rpDatasets[c.ep.TableLocator(p).TableSchema] = struct{}{}
	}

	// Fetch table and column metadata using the metadata API. This API is free to use, and has very
	// high rate limits. Running a job to query the INFORMATION_SCHEMA view costs a minimum of 10MB
	// per query, so this is a little more cost effective and efficient.
	var mu sync.Mutex
	group, groupCtx := errgroup.WithContext(ctx)

	// The table listing will fail if the dataset doesn't already exist, so only attempt to list
	// tables in datasets that do exist.
	existingDatasets, err := c.ListSchemas(ctx)
	if err != nil {
		return fmt.Errorf("listing schemas: %w", err)
	}

	for ds := range rpDatasets {
		if !slices.Contains(existingDatasets, ds) {
			log.WithField("dataset", ds).Debug("not listing tables for dataset since it doesn't exist")
			continue
		}

		tableIter := c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, ds).Tables(ctx)

		for {
			table, err := tableIter.Next()
			if err != nil {
				if err == iterator.Done {
					break
				}
				return fmt.Errorf("table iterator next: %w", err)
			}

			group.Go(func() error {
				md, err := table.Metadata(groupCtx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
				if err != nil {
					return fmt.Errorf("getting metadata for %s.%s: %w", table.DatasetID, table.TableID, err)
				}

				mu.Lock()
				defer mu.Unlock()

				res := is.PushResource(table.DatasetID, table.TableID)
				res.Meta = md.Schema
				for _, f := range md.Schema {
					res.PushField(boilerplate.ExistingField{
						Name:               f.Name,
						Nullable:           !f.Required,
						Type:               string(f.Type),
						CharacterMaxLength: int(f.MaxLength),
						HasDefault:         len(f.DefaultValueExpression) > 0,
					})
				}

				return nil
			})
		}
	}

	if err := group.Wait(); err != nil {
		return err
	}

	return nil
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.query(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", c.ep.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.query(ctx, stmt)
		return err
	}, nil
}

var columnMigrationSteps = []sql.ColumnMigrationStep{
	sql.StdMigrationSteps[0],
	sql.StdMigrationSteps[1],
	sql.StdMigrationSteps[2],
	sql.StdMigrationSteps[3],
	func(dialect sql.Dialect, table sql.Table, instructions []sql.MigrationInstruction) ([]string, error) {
		// BigQuery does not support making a column REQUIRED when it is NULLABLE
		// TODO: do we prefer to backfill in these instances for BigQuery, or just continue
		// with this no-op as-is?

		return []string{}, nil
	},
}

// TODO(whb): In display of needless cruelty, BigQuery will throw an error if you try to use an
// ALTER TABLE sql statement to add columns to a table with no pre-existing columns, claiming that
// it does not have a schema. I believe the client API would allow us to set the schema, but this
// would have to be done in a totally different way than we are currently doing things and I'm not
// up for tackling that right now. With recent changes to at least recognizing that tables with no
// columns exist, we'll at least get a coherent error message when this happens and can know that
// the workaround is to re-backfill the table so it gets created fresh with the needed columns.
func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var stmts []string
	if len(ta.DropNotNulls) > 0 || len(ta.AddColumns) > 0 {
		var alterColumnStmtBuilder strings.Builder
		if err := renderTemplates(c.ep.Dialect).alterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
			return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		alterColumnStmt := alterColumnStmtBuilder.String()
		stmts = append(stmts, alterColumnStmt)
	}

	if len(ta.ColumnTypeChanges) > 0 {
		if steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges, columnMigrationSteps...); err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		} else {
			stmts = append(stmts, steps...)
		}
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			if _, err := c.query(ctx, stmt); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (c *client) ListSchemas(ctx context.Context) ([]string, error) {
	// BigQuery represents the concept of a "schema" with datasets.
	iter := c.bigqueryClient.Datasets(ctx)
	iter.ProjectID = c.cfg.ProjectID

	dsNames := []string{}
	for {
		ds, err := iter.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, fmt.Errorf("dataset iterator next: %w", err)
		}

		dsNames = append(dsNames, ds.DatasetID)
	}

	return dsNames, nil
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	if err := c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, schemaName).Create(ctx, &bigquery.DatasetMetadata{
		Location: c.cfg.Region,
	}); err != nil {
		return "", err
	}

	return fmt.Sprintf("CREATE DATASET %q.%q", c.cfg.ProjectID, schemaName), nil
}

func preReqs(ctx context.Context, cfg config, tenant string) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	c, err := cfg.client(ctx, nil)
	if err != nil {
		errs.Err(fmt.Errorf("creating client: %w", err))
		return errs
	}

	var googleErr *googleapi.Error

	if meta, err := c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, c.cfg.Dataset).Metadata(ctx); err != nil {
		if errors.As(err, &googleErr) {
			// The raw error message returned if the dataset or project can't be found can be pretty
			// vague, but a 404 code always means that one of those two things couldn't be found.
			if googleErr.Code == http.StatusNotFound {
				err = fmt.Errorf("the ProjectID %q or BigQuery Dataset %q could not be found: %s (code %v)", c.cfg.ProjectID, c.cfg.Dataset, googleErr.Message, googleErr.Code)
			}
		}
		errs.Err(err)
	} else if meta.Location != c.cfg.Region {
		errs.Err(fmt.Errorf("dataset %q is actually in region %q, which is different than the configured region %q", c.cfg.Dataset, meta.Location, c.cfg.Region))
	}

	if bucket, err := blob.NewGCSBucket(ctx, cfg.Bucket, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON))); err != nil {
		errs.Err(fmt.Errorf("creating GCS bucket: %w", err))
	} else if err := bucket.CheckPermissions(ctx, blob.CheckPermissionsConfig{Prefix: cfg.effectiveBucketPath()}); err != nil {
		errs.Err(err)
	}

	return errs
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return errors.New("internal error: ExecStatements not implemented")
}

func (c *client) InstallFence(ctx context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	return sql.Fence{}, errors.New("internal error: InstallFence not implemented")

}

func (c *client) Close() {
	c.bigqueryClient.Close()
}
