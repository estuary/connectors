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

	"cloud.google.com/go/bigquery"
	storage "cloud.google.com/go/storage"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

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
	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(bqDialect.TableLocator),
		bqDialect.ColumnLocator,
	)

	datasets := []string{c.cfg.Dataset}
	for _, p := range resourcePaths {
		datasets = append(datasets, p[1]) // Dataset is always the second element of the path.
	}

	slices.Sort(datasets)
	datasets = slices.Compact(datasets)

	for _, ds := range datasets {
		job, err := c.query(ctx, fmt.Sprintf(
			"select table_schema, table_name, column_name, is_nullable, data_type, column_default from %s.%s.INFORMATION_SCHEMA.COLUMNS;",
			bqDialect.Identifier(c.cfg.ProjectID), // Use the project containing the dataset rather than the billing project if a billing project is configured.
			bqDialect.Identifier(ds),
		))
		if err != nil {
			return nil, fmt.Errorf("querying INFORMATION_SCHEMA.COLUMNS for dataset %q: %w", ds, err)
		}

		it, err := job.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("reading job: %w", err)
		}

		type columnRow struct {
			TableSchema   string `bigquery:"table_schema"`
			TableName     string `bigquery:"table_name"`
			ColumnName    string `bigquery:"column_name"`
			IsNullable    string `bigquery:"is_nullable"` // string YES or NO
			DataType      string `bigquery:"data_type"`
			ColumnDefault string `bigquery:"column_default"` // "NULL" if no default
		}

		for {
			var c columnRow
			if err = it.Next(&c); err == iterator.Done {
				break
			} else if err != nil {
				return nil, fmt.Errorf("columnRow read: %w", err)
			}

			if strings.HasPrefix(c.DataType, "BIGNUMERIC") {
				// BigQuery includes the precision of BIGNUMERIC columns in the DataType string. We
				// want to treat all pre-existing BIGNUMERIC columns the same, so that is stripped
				// out here.
				c.DataType = "BIGNUMERIC"
			}

			is.PushField(boilerplate.EndpointField{
				Name:               c.ColumnName,
				Nullable:           strings.EqualFold(c.IsNullable, "yes"),
				Type:               c.DataType,
				CharacterMaxLength: 0, // BigQuery does not have a character_maximum_length in its INFORMATION_SCHEMA.COLUMNS view.
				HasDefault:         !strings.EqualFold(c.ColumnDefault, "null"),
			}, c.TableSchema, c.TableName)
		}
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

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	var alterColumnStmtBuilder strings.Builder
	if err := tplAlterTableColumns.Execute(&alterColumnStmtBuilder, ta); err != nil {
		return "", nil, fmt.Errorf("rendering alter table columns statement: %w", err)
	}
	alterColumnStmt := alterColumnStmtBuilder.String()

	return alterColumnStmt, func(ctx context.Context) error {
		_, err := c.query(ctx, alterColumnStmt)
		return err
	}, nil
}

func (c *client) PreReqs(ctx context.Context) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

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
