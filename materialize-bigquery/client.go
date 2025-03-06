package connector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	bigqueryClient     *bigquery.Client
	cloudStorageClient *storage.Client
	cfg                config
	ep                 *sql.Endpoint
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)
	return cfg.client(ctx, ep)
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(c.ep.TableLocator),
		c.ep.ColumnLocator,
	)

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
		return nil, fmt.Errorf("listing schemas: %w", err)
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
				return nil, fmt.Errorf("table iterator next: %w", err)
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
		return nil, err
	}

	return is, nil
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
	func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, tempColumnIdentifier string) (string, error) {
		return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;",
			table.Identifier,
			tempColumnIdentifier,
			// Always create these new columns as nullable
			migration.NullableDDL,
		), nil
	},
	func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, tempColumnIdentifier string) (string, error) {
		return fmt.Sprintf(
			// The WHERE filter is required by some warehouses (bigquery)
			"UPDATE %s SET %s = %s WHERE true;",
			table.Identifier,
			tempColumnIdentifier,
			migration.CastSQL(migration),
		), nil
	},
	func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, _ string) (string, error) {
		return fmt.Sprintf(
			"ALTER TABLE %s DROP COLUMN %s;",
			table.Identifier,
			migration.Identifier,
		), nil
	},
	func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, tempColumnIdentifier string) (string, error) {
		return fmt.Sprintf(
			"ALTER TABLE %s RENAME COLUMN %s TO %s;",
			table.Identifier,
			tempColumnIdentifier,
			migration.Identifier,
		), nil
	},
	func(dialect sql.Dialect, table sql.Table, migration sql.ColumnTypeMigration, _ string) (string, error) {
		// BigQuery does not support making a column REQUIRED when it is NULLABLE
		// TODO: do we prefer to backfill in these instances for BigQuery, or just continue
		// with this no-op as-is?

		return "", nil
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
		for _, m := range ta.ColumnTypeChanges {
			if steps, err := sql.StdColumnTypeMigration(ctx, c.ep.Dialect, ta.Table, m, columnMigrationSteps...); err != nil {
				return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
			} else {
				stmts = append(stmts, steps...)
			}
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

func (c *client) CreateSchema(ctx context.Context, schemaName string) error {
	return c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, schemaName).Create(ctx, &bigquery.DatasetMetadata{
		Location: c.cfg.Region,
	})
}

func preReqs(ctx context.Context, conf any, tenant string) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	cfg := conf.(*config)
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

	// Verify cloud storage abilities.
	data := []byte("test")

	objectDir := path.Join(c.cfg.Bucket, c.cfg.BucketPath)
	objectKey := path.Join(objectDir, uuid.NewString())
	objectHandle := c.cloudStorageClient.Bucket(c.cfg.Bucket).Object(objectKey)

	writer := objectHandle.NewWriter(ctx)
	if _, err := writer.Write(data); err != nil {
		// This won't err until the writer is closed in the case of having the wrong bucket or
		// authorization configured, and would most likely be caused by a logic error in the
		// connector code.
		errs.Err(err)
	} else if err := writer.Close(); err != nil {
		// Handling for the two most common cases: The bucket doesn't exist, or the bucket does
		// exist but the configured credentials aren't authorized to write to it.
		if errors.As(err, &googleErr) {
			if googleErr.Code == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", c.cfg.Bucket)
			} else if googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to bucket %q", objectDir)
			}
		}
		errs.Err(err)
	} else {
		// Verify that the created object can be read and deleted. The unauthorized case is handled
		// & formatted specifically in these checks since the existence of the bucket has already
		// been verified by creating the temporary test object.
		if reader, err := objectHandle.NewReader(ctx); err != nil {
			errs.Err(err)
		} else if _, err := reader.Read(make([]byte, len(data))); err != nil {
			if errors.As(err, &googleErr) && googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf("not authorized to read from bucket %q", objectDir)
			}
			errs.Err(err)
		} else {
			reader.Close()
		}

		// It's technically possible to be able to delete but not read, so delete is checked even if
		// read failed.
		if err := objectHandle.Delete(ctx); err != nil {
			if errors.As(err, &googleErr) && googleErr.Code == http.StatusForbidden {
				err = fmt.Errorf("not authorized to delete from bucket %q", objectDir)
			}
			errs.Err(err)
		}
	}

	return errs
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	_, err := c.query(ctx, strings.Join(statements, "\n"))
	return err
}

func (c *client) InstallFence(ctx context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	var query strings.Builder
	if err := renderTemplates(c.ep.Dialect).installFence.Execute(&query, fence); err != nil {
		return fence, fmt.Errorf("evaluating fence template: %w", err)
	}

	log.Info("installing fence")
	job, err := c.query(ctx, query.String())
	if err != nil {
		return fence, err
	}
	var bqFence struct {
		Fence      int64  `bigquery:"fence"`
		Checkpoint string `bigquery:"checkpoint"`
	}
	log.Info("reading installed fence")
	if err = c.fetchOne(ctx, job, &bqFence); err != nil {
		return fence, fmt.Errorf("read fence: %w", err)
	}

	checkpoint, err := base64.StdEncoding.DecodeString(bqFence.Checkpoint)
	if err != nil {
		return fence, fmt.Errorf("base64.Decode(checkpoint): %w", err)
	}

	fence.Fence = bqFence.Fence
	fence.Checkpoint = checkpoint

	log.WithFields(log.Fields{
		"fence":            fence.Fence,
		"keyBegin":         fence.KeyBegin,
		"keyEnd":           fence.KeyEnd,
		"materialization":  fence.Materialization.String(),
		"checkpointsTable": fence.TablePath,
	}).Info("fence installed successfully")

	return fence, nil
}

func (c *client) Close() {
	c.bigqueryClient.Close()
	c.cloudStorageClient.Close()
}
