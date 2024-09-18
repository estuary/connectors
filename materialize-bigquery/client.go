package connector

import (
	"context"
	dbSql "database/sql"
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
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
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
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)
	return cfg.client(ctx)
}

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	// First check if there are any interrupted column migrations which must be resumed, before we
	// construct the InfoSchema
	var migrationsTable = bqDialect.Identifier(c.cfg.ProjectID, c.cfg.Dataset, "flow_migrations")
	// We check for existence of the table by requesting metadata, this is the recommended approach by BigQuery docs
	// see https://pkg.go.dev/cloud.google.com/go/bigquery#Dataset.Table
	_, err := c.bigqueryClient.DatasetInProject(c.cfg.ProjectID, c.cfg.Dataset).Table("flow_migrations").Metadata(ctx)
	if err == nil {
		job, err := c.query(ctx, fmt.Sprintf("SELECT table, step, col_identifier, col_field, col_ddl FROM %s", migrationsTable))
		if err != nil {
			return nil, fmt.Errorf("finding flow_migrations job: %w", err)
		}
		it, err := job.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("finding flow_migrations: %w", err)
		}
		type migration struct {
			TableIdentifier string `bigquery:"table"`
			Step            int    `bigquery:"step"`
			ColIdentifier   string `bigquery:"col_identifier"`
			ColField        string `bigquery:"col_field"`
			DDL             string `bigquery:"col_ddl"`
		}
		for {
			var stmts [][]string
			var m migration
			if err := it.Next(&m); err == iterator.Done {
				break
			} else if err != nil {
				return nil, fmt.Errorf("reading flow_migrations row: %w", err)
			}

			var steps, acceptableErrors = c.columnChangeSteps(m.TableIdentifier, m.ColField, m.ColIdentifier, m.DDL)

			for s := m.Step; s < len(steps); s++ {
				stmts = append(stmts, []string{steps[s], acceptableErrors[s]})
				stmts = append(stmts, []string{fmt.Sprintf("UPDATE %s SET step = %d WHERE table='%s';", migrationsTable, s+1, m.TableIdentifier)})
			}

			stmts = append(stmts, []string{fmt.Sprintf("DELETE FROM %s WHERE table='%s';", migrationsTable, m.TableIdentifier)})

			for _, stmt := range stmts {
				query := stmt[0]
				var acceptableError string
				if len(stmt) > 1 {
					acceptableError = stmt[1]
				}

				log.WithField("q", query).WithField("acceptableError", acceptableError).Info("migration")
				if _, err := c.query(ctx, query); err != nil {
					if acceptableError != "" && strings.Contains(err.Error(), acceptableError) {
						// This is an acceptable error for this step, so we do not throw an error
					} else {
						return nil, err
					}
				}
			}

			if _, err := c.query(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", migrationsTable)); err != nil {
				return nil, fmt.Errorf("dropping flow_migrations: %w", err)
			}
		}
	}

	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(bqDialect.TableLocator),
		bqDialect.ColumnLocator,
	)

	rpDatasets := make(map[string]struct{})
	for _, p := range resourcePaths {
		rpDatasets[bqDialect.TableLocator(p).TableSchema] = struct{}{}
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

			mu.Lock()
			is.PushResource(table.DatasetID, table.TableID)
			mu.Unlock()

			group.Go(func() error {
				md, err := table.Metadata(groupCtx, bigquery.WithMetadataView(bigquery.BasicMetadataView))
				if err != nil {
					return fmt.Errorf("getting metadata for %s.%s: %w", table.DatasetID, table.TableID, err)
				}

				mu.Lock()
				defer mu.Unlock()

				for _, f := range md.Schema {
					is.PushField(boilerplate.EndpointField{
						Name:               f.Name,
						Nullable:           !f.Required,
						Type:               string(f.Type),
						CharacterMaxLength: int(f.MaxLength),
						HasDefault:         len(f.DefaultValueExpression) > 0,
					}, table.DatasetID, table.TableID)
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

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	_, err := c.query(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...)
	return err
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.query(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", bqDialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.query(ctx, stmt)
		return err
	}, nil
}

func (c *client) columnChangeSteps(tableIdentifier, colField, colIdentifier, DDL string) ([]string, []string) {
	var tempColumnName = fmt.Sprintf("%s_flowtmp1", colField)
	var tempColumnIdentifier = bqDialect.Identifier(tempColumnName)
	var tempOriginalRename = fmt.Sprintf("%s_flowtmp2", colField)
	var tempOriginalRenameIdentifier = bqDialect.Identifier(tempOriginalRename)
	return []string{
			fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN %s %s;",
				tableIdentifier,
				tempColumnIdentifier,
				DDL,
			),
			fmt.Sprintf(
				"UPDATE %s SET %s = CAST(%s AS %s) WHERE true;",
				tableIdentifier,
				tempColumnIdentifier,
				colIdentifier,
				DDL,
			),
			fmt.Sprintf(
				"ALTER TABLE %s RENAME COLUMN %s TO %s;",
				tableIdentifier,
				colIdentifier,
				tempOriginalRenameIdentifier,
			),
			fmt.Sprintf(
				"ALTER TABLE %s RENAME COLUMN %s TO %s;",
				tableIdentifier,
				tempColumnIdentifier,
				colIdentifier,
			),
			fmt.Sprintf(
				"ALTER TABLE %s DROP COLUMN %s;",
				tableIdentifier,
				tempOriginalRenameIdentifier,
			),
		}, []string{
			fmt.Sprintf("Column already exists: %s", tempColumnName),
			"",
			fmt.Sprintf("Column already exists: %s", tempOriginalRename),
			fmt.Sprintf("Column already exists: %s", colField),
			fmt.Sprintf("Column not found: %s", tempOriginalRename),
		}
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
	var alterColumnStmtBuilder strings.Builder
	if err := tplAlterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
		return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
	}
	alterColumnStmt := alterColumnStmtBuilder.String()
	if len(strings.Trim(alterColumnStmt, "\n")) > 0 {
		stmts = append(stmts, alterColumnStmt)
	}

	if len(ta.ColumnTypeChanges) > 0 {
		var migrationsTable = bqDialect.Identifier(c.cfg.ProjectID, c.cfg.Dataset, "flow_migrations")
		stmts = append(stmts, fmt.Sprintf("CREATE OR REPLACE TABLE %s(table STRING, step INTEGER, col_identifier STRING, col_field STRING, col_ddl STRING);", migrationsTable))

		for _, ch := range ta.ColumnTypeChanges {
			stmts = append(stmts, fmt.Sprintf(
				"INSERT INTO %s(table, step, col_identifier, col_field, col_ddl) VALUES ('%s', 0, '%s', '%s', '%s');",
				migrationsTable,
				ta.Identifier,
				ch.Identifier,
				ch.Field,
				ch.DDL,
			))
		}

		for _, ch := range ta.ColumnTypeChanges {
			var steps, _ = c.columnChangeSteps(ta.Identifier, ch.Field, ch.Identifier, ch.DDL)

			for s := 0; s < len(steps); s++ {
				stmts = append(stmts, steps[s])
				stmts = append(stmts, fmt.Sprintf("UPDATE %s SET STEP=%d WHERE table='%s';", migrationsTable, s+1, ta.Identifier))
			}
		}

		stmts = append(stmts, fmt.Sprintf("DROP TABLE %s;", migrationsTable))
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

func preReqs(ctx context.Context, conf any, tenant string) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	cfg := conf.(*config)
	c, err := cfg.client(ctx)
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

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	job, err := c.query(ctx, fmt.Sprintf(
		"SELECT version, spec FROM %s WHERE materialization=%s;",
		specs.Identifier,
		specs.Keys[0].Placeholder,
	), materialization.String())
	if err != nil {
		return "", "", err
	}

	var data struct {
		Version string `bigquery:"version"`
		SpecB64 string `bigquery:"spec"`
	}

	if err := c.fetchOne(ctx, job, &data); err == errNotFound {
		return "", "", dbSql.ErrNoRows
	} else if err != nil {
		return "", "", err
	}

	log.WithFields(log.Fields{
		"table":           specs.Identifier,
		"materialization": materialization.String(),
		"version":         data.Version,
	}).Info("existing materialization spec loaded")

	return data.SpecB64, data.Version, nil
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	_, err := c.query(ctx, strings.Join(statements, "\n"))
	return err
}

func (c *client) InstallFence(ctx context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	var query strings.Builder
	if err := tplInstallFence.Execute(&query, fence); err != nil {
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
