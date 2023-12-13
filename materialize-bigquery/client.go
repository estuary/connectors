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
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type client struct {
	bigqueryClient     *bigquery.Client
	cloudStorageClient *storage.Client
	config             config
}

func (c client) InfoSchema(ctx context.Context, ep *sql.Endpoint, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	cfg := ep.Config.(*config)

	// Query the information schema for all the datasets we care about to build up a list of
	// existing tables and columns. The datasets we care about are the dataset configured for the
	// overall endpoint, as well as any distinct datasets configured for any of the bindings.
	datasets := []string{cfg.Dataset}
	for _, p := range resourcePaths {
		datasets = append(datasets, p[1]) // Dataset is always the second element of the path.
	}

	slices.Sort(datasets)
	datasets = slices.Compact(datasets)

	is := boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(ep.Dialect.TableLocator),
		ep.Dialect.ColumnLocator,
	)

	for _, ds := range datasets {
		job, err := c.query(ctx, fmt.Sprintf(
			"select table_schema, table_name, column_name, is_nullable, data_type from %s.INFORMATION_SCHEMA.COLUMNS;",
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
			TableSchema string `bigquery:"table_schema"`
			TableName   string `bigquery:"table_name"`
			ColumnName  string `bigquery:"column_name"`
			IsNullable  string `bigquery:"is_nullable"` // string YES or NO
			DataType    string `bigquery:"data_Type"`
		}

		for {
			var c columnRow
			if err = it.Next(&c); err == iterator.Done {
				break
			} else if err != nil {
				return nil, fmt.Errorf("columnRow read: %w", err)
			}

			is.PushField(boilerplate.EndpointField{
				Name:               c.ColumnName,
				Nullable:           strings.EqualFold(c.IsNullable, "yes"),
				Type:               c.DataType,
				CharacterMaxLength: 0, // BigQuery does not have a character_maximum_length in its INFORMATION_SCHEMA.COLUMNS view.
			}, c.TableSchema, c.TableName)
		}
	}

	return is, nil
}

func (c client) Apply(ctx context.Context, ep *sql.Endpoint, req *pm.Request_Apply, actions sql.ApplyActions, updateSpec sql.MetaSpecsUpdate) (string, error) {
	client := ep.Client.(*client)
	cfg := ep.Config.(*config)

	// Query the information schema for all the datasets we care about to build up a list of
	// existing tables and columns. The datasets we care about are the dataset configured for the
	// overall endpoint, as well as any distinct datasets configured for any of the bindings.
	datasets := []string{cfg.Dataset}
	for _, t := range actions.CreateTables {
		ds := t.Path[1]
		if !slices.Contains(datasets, ds) {
			datasets = append(datasets, ds)
		}
	}
	for _, t := range actions.AlterTables {
		ds := t.Path[1]
		if !slices.Contains(datasets, ds) {
			datasets = append(datasets, ds)
		}
	}
	for idx := range datasets {
		datasets[idx] = bqDialect.Identifier(datasets[idx])
	}

	existing := &sql.ExistingColumns{}
	for _, ds := range datasets {
		job, err := client.query(ctx, fmt.Sprintf(
			"select table_name, column_name, is_nullable, data_type from %s.INFORMATION_SCHEMA.COLUMNS;",
			bqDialect.Identifier(ds),
		))
		if err != nil {
			return "", fmt.Errorf("querying INFORMATION_SCHEMA.COLUMNS for dataset %q: %w", ds, err)
		}

		it, err := job.Read(ctx)
		if err != nil {
			return "", fmt.Errorf("reading job: %w", err)
		}

		type columnRow struct {
			TableName  string `bigquery:"table_name"`
			ColumnName string `bigquery:"column_name"`
			IsNullable string `bigquery:"is_nullable"` // string YES or NO
			DataType   string `bigquery:"data_Type"`
		}

		for {
			var c columnRow
			if err = it.Next(&c); err == iterator.Done {
				break
			} else if err != nil {
				return "", fmt.Errorf("columnRow read: %w", err)
			}

			existing.PushColumn(
				ds,
				c.TableName,
				c.ColumnName,
				strings.EqualFold(c.IsNullable, "yes"),
				c.DataType,
				0, // BigQuery does not have a character_maximum_length in its INFORMATION_SCHEMA.COLUMNS view.
			)
		}
	}

	filtered, err := sql.FilterActions(actions, bqDialect, existing)
	if err != nil {
		return "", err
	}

	statements := []string{}
	for _, tc := range filtered.CreateTables {
		statements = append(statements, tc.TableCreateSql)
	}

	for _, ta := range filtered.AlterTables {
		var alterColumnStmt strings.Builder
		if err := tplAlterTableColumns.Execute(&alterColumnStmt, ta); err != nil {
			return "", fmt.Errorf("rendering alter table columns statement: %w", err)
		}
		statements = append(statements, alterColumnStmt.String())
	}

	for _, tr := range filtered.ReplaceTables {
		statements = append(statements, tr.TableReplaceSql)
	}

	// The spec will get updated last, after all the other actions are complete, but include it in
	// the description of actions.
	action := strings.Join(append(statements, updateSpec.QueryString), "\n")
	if req.DryRun {
		return action, nil
	}

	if len(filtered.ReplaceTables) > 0 {
		if _, err := c.query(ctx, fmt.Sprintf(
			`UPDATE %s SET fence=fence+1 WHERE materialization = %s`,
			bqDialect.Identifier(ep.MetaCheckpoints.Path...),
			bqDialect.Literal(req.Materialization.Name.String()),
		)); err != nil {
			return "", fmt.Errorf("incrementing fence: %w", err)
		}
	}

	// Execute statements in parallel for efficiency. Each statement acts on a single table, and
	// everything that needs to be done for a given table is contained in that statement.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, s := range statements {
		s := s
		group.Go(func() error {
			if _, err := c.query(groupCtx, s); err != nil {
				return fmt.Errorf("executing apply statement: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
	}

	// Once all the table actions are done, we can update the stored spec.
	if _, err := c.query(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...); err != nil {
		return "", fmt.Errorf("executing spec update statement: %w", err)
	}

	return action, nil
}

func (c client) PreReqs(ctx context.Context, ep *sql.Endpoint) *sql.PrereqErr {
	cfg := ep.Config.(*config)
	client := ep.Client.(*client)
	errs := &sql.PrereqErr{}

	var googleErr *googleapi.Error

	if meta, err := client.bigqueryClient.DatasetInProject(cfg.ProjectID, cfg.Dataset).Metadata(ctx); err != nil {
		if errors.As(err, &googleErr) {
			// The raw error message returned if the dataset or project can't be found can be pretty
			// vague, but a 404 code always means that one of those two things couldn't be found.
			if googleErr.Code == http.StatusNotFound {
				err = fmt.Errorf("the ProjectID %q or BigQuery Dataset %q could not be found: %s (code %v)", cfg.ProjectID, cfg.Dataset, googleErr.Message, googleErr.Code)
			}
		}
		errs.Err(err)
	} else if meta.Location != cfg.Region {
		errs.Err(fmt.Errorf("dataset %q is actually in region %q, which is different than the configured region %q", cfg.Dataset, meta.Location, cfg.Region))
	}

	// Verify cloud storage abilities.
	data := []byte("test")

	objectDir := path.Join(cfg.Bucket, cfg.BucketPath)
	objectKey := path.Join(objectDir, uuid.NewString())
	objectHandle := client.cloudStorageClient.Bucket(cfg.Bucket).Object(objectKey)

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
				err = fmt.Errorf("bucket %q does not exist", cfg.Bucket)
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

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
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

func (c client) ExecStatements(ctx context.Context, statements []string) error {
	_, err := c.query(ctx, strings.Join(statements, "\n"))
	return err
}

func (c client) InstallFence(ctx context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
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
