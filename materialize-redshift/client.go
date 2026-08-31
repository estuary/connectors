package connector

import (
	"bytes"
	"compress/gzip"
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db  *stdsql.DB
	cfg config
	ep  *sql.Endpoint[config]
}

func newClient(ctx context.Context, materializationName string, ep *sql.Endpoint[config]) (sql.Client, error) {
	cfg := ep.Config

	db, err := stdsql.Open("pgx", cfg.toURI())
	if err != nil {
		return nil, err
	}

	return &client{
		db:  db,
		cfg: cfg,
		ep:  ep,
	}, nil
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

	return sql.StdPopulateInfoSchema(ctx, is, c.db, c.ep.Dialect, catalog, resourcePaths)
}

func (c *client) CreateTable(ctx context.Context, tc sql.TableCreate) error {
	_, err := c.db.ExecContext(ctx, tc.TableCreateSql)
	return err
}

func (c *client) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("DROP TABLE %s;", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	stmt := fmt.Sprintf("TRUNCATE TABLE %s;", c.ep.Dialect.Identifier(path...))

	return stmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, stmt)
		return err
	}, nil
}

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	// Redshift only allows a single column to be added per ALTER TABLE statement. Also, we will
	// never need to drop nullability constraints, since Redshift does not allow dropping
	// nullability and we don't ever create columns as NOT NULL as a result.
	if len(ta.DropNotNulls) != 0 { // sanity check
		return "", nil, fmt.Errorf("redshift cannot drop nullability constraints but got %d DropNotNulls for table %s", len(ta.DropNotNulls), ta.Identifier)
	}

	stmts := []string{}
	for _, c := range ta.AddColumns {
		stmts = append(stmts, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s %s;",
			ta.Identifier,
			c.Identifier,
			c.NullableDDL,
		))
	}

	if len(ta.ColumnTypeChanges) > 0 {
		if steps, err := sql.StdColumnTypeMigrations(ctx, c.ep.Dialect, ta.Table, ta.ColumnTypeChanges); err != nil {
			return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
		} else {
			stmts = append(stmts, steps...)
		}
	}

	return strings.Join(stmts, "\n"), func(ctx context.Context) error {
		for _, stmt := range stmts {
			_, err := c.db.ExecContext(ctx, stmt)
			if err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (c *client) ListSchemas(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx, "select schema_name from svv_all_schemas")
	if err != nil {
		return nil, fmt.Errorf("querying svv_all_schemas: %w", err)
	}
	defer rows.Close()

	out := []string{}

	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}
		out = append(out, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return out, nil
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) (string, error) {
	return sql.StdCreateSchema(ctx, c.db, c.ep.Dialect, schemaName)
}

func preReqs(ctx context.Context, cfg config) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	db, err := stdsql.Open("pgx", cfg.toURI())
	if err != nil {
		errs.Err(err)
		return errs
	}

	// Use a reasonable timeout for this connection test. It is not uncommon for a misconfigured
	// connection (wrong host, wrong port, etc.) to hang for several minutes on Ping and we want to
	// bail out well before then.
	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := db.PingContext(pingCtx); err != nil {
		// Provide a more user-friendly representation of some common error causes.
		var pgErr *pgconn.ConnectError

		if errors.As(err, &pgErr) {
			err = pgErr.Unwrap()
			if errStr := err.Error(); strings.Contains(errStr, "(SQLSTATE 28000)") {
				err = fmt.Errorf("incorrect username or password")
			} else if strings.Contains(errStr, "(SQLSTATE 3D000") {
				err = fmt.Errorf("database %q does not exist", cfg.Database)
			} else if strings.Contains(errStr, "context deadline exceeded") {
				errStr := `connection to host at address %q timed out, possible causes:
					* Redshift endpoint is not set to be publicly accessible
					* there is no inbound rule allowing Estuary's IP address to connect through the Redshift VPC security group
					* the configured address is incorrect, possibly with an incorrect host or port
					* if connecting through an SSH tunnel, the SSH bastion server may not be operational, or the connection details are incorrect`
				err = fmt.Errorf(errStr, cfg.Address)
			}
		}

		errs.Err(err)
	}

	parsedFlags := common.ParseFeatureFlags(cfg.Advanced.FeatureFlags, featureFlagDefaults)

	s3client, err := cfg.toS3Client(ctx, parsedFlags)
	if err != nil {
		// This is not caused by invalid S3 credentials, and would most likely be a logic error in
		// the connector code.
		errs.Err(err)
		return errs
	}

	// Test creating, reading, and deleting an object from the configured bucket and bucket path.
	testKey := path.Join(cfg.effectiveBucketPath(), uuid.NewString())

	var awsErr *awsHttp.ResponseError
	if _, err := s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("testing"),
	}); err != nil {
		if errors.As(err, &awsErr) {
			// Handling for the two most common cases: The bucket doesn't exist, or the bucket does
			// exist but the configured credentials aren't authorized to write to it.
			if awsErr.Response.Response.StatusCode == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", cfg.Bucket)
			} else if awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to %q", path.Join(cfg.Bucket, cfg.effectiveBucketPath()))
			}
		}
		errs.Err(err)
	} else if _, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to read from %q", path.Join(cfg.Bucket, cfg.effectiveBucketPath()))
		}
		errs.Err(err)
	} else if _, err := s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to delete from %q", path.Join(cfg.Bucket, cfg.effectiveBucketPath()))
		}
		errs.Err(err)
	}

	return errs
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

// InstallFence is a no-op: the checkpoints table holds applied-transaction
// tokens rather than a fence, and nothing reads the fence column.
func (c *client) InstallFence(_ context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	return fence, nil
}

func (c *client) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (c *client) checkpointsTableIdentifier() string {
	var path []string
	if c.cfg.Schema != "" {
		path = append(path, c.cfg.Schema)
	}
	path = append(path, sql.DefaultFlowCheckpoints)
	return c.ep.Dialect.Identifier(path...)
}

func (c *client) ListCheckpointsEntries(ctx context.Context) ([]string, error) {
	var out []string
	if err := c.withDB(func(db *stdsql.DB) error {
		var err error
		out, err = sql.ListCheckpointsEntries(ctx, db, c.checkpointsTableIdentifier())
		return err
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *client) DeleteCheckpointsEntry(ctx context.Context, taskName string) error {
	return c.withDB(func(db *stdsql.DB) error {
		return sql.DeleteCheckpointsEntry(ctx, db, c.checkpointsTableIdentifier(), taskName)
	})
}

func (c *client) SnapshotTestTable(ctx context.Context, path []string) (columnNames []string, rows [][]any, _ error) {
	if err := c.withDB(func(db *stdsql.DB) error {
		var err error
		columnNames, rows, err = snapshotTestTable(ctx, db, c.ep.Dialect, path)
		return err
	}); err != nil {
		return nil, nil, err
	}

	return columnNames, rows, nil
}

// snapshotTestTable returns all rows of the given table for snapshot testing.
// VARBYTE columns are read as base64 via FROM_VARBYTE so that binary values
// have the same textual representation as they do in Flow and other
// materializations, and so that hex-encoded strings composed entirely of
// digits aren't inadvertently emitted as JSON numbers by the snapshot encoder.
func snapshotTestTable(ctx context.Context, db *stdsql.DB, dialect sql.Dialect, path []string) ([]string, [][]any, error) {
	var tableSchema, tableName string
	switch len(path) {
	case 1:
		tableSchema = "public"
		tableName = path[0]
	case 2:
		tableSchema = path[0]
		tableName = path[1]
	default:
		return nil, nil, fmt.Errorf("unexpected resource path length %d", len(path))
	}

	colRows, err := db.QueryContext(
		ctx,
		`SELECT column_name, data_type FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2
			ORDER BY ordinal_position;`,
		strings.ToLower(tableSchema),
		strings.ToLower(tableName),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("listing columns for %s.%s: %w", tableSchema, tableName, err)
	}
	defer colRows.Close()

	var selectExprs []string
	for colRows.Next() {
		var name, dataType string
		if err := colRows.Scan(&name, &dataType); err != nil {
			return nil, nil, err
		}
		ident := dialect.Identifier(name)
		if dataType == "binary varying" {
			// FROM_VARBYTE may insert line breaks for large base64 outputs.
			// Strip them so snapshots and downstream base64 decoders see a
			// single contiguous string. CHR(10) is LF, CHR(13) is CR.
			selectExprs = append(selectExprs, fmt.Sprintf("REPLACE(REPLACE(FROM_VARBYTE(%s, 'base64'), CHR(10), ''), CHR(13), '') AS %s", ident, ident))
		} else {
			selectExprs = append(selectExprs, ident)
		}
	}
	if err := colRows.Err(); err != nil {
		return nil, nil, err
	}
	if len(selectExprs) == 0 {
		return nil, nil, fmt.Errorf("no columns found for %s.%s", tableSchema, tableName)
	}

	tableIdentifier := dialect.Identifier(path...)
	query := fmt.Sprintf("SELECT %s FROM %s;", strings.Join(selectExprs, ", "), tableIdentifier)
	return sql.SnapshotTestTableQuery(ctx, db, tableIdentifier, query)
}

func (c *client) Close() {
	c.db.Close()
}

func (c *client) withDB(fn func(*stdsql.DB) error) error {
	var db, err = stdsql.Open("pgx", c.cfg.toURI())
	if err != nil {
		return err
	}
	defer db.Close()
	return fn(db)
}

// compressBytes gzips b, matching the format decodeLegacyCheckpoint expects.
func compressBytes(b []byte) ([]byte, error) {
	var gzb bytes.Buffer
	w := gzip.NewWriter(&gzb)
	if _, err := w.Write(b); err != nil {
		return nil, fmt.Errorf("compressing bytes: %w", err)
	} else if err := w.Close(); err != nil {
		return nil, fmt.Errorf("closing gzip writer: %w", err)
	}
	return gzb.Bytes(), nil
}

func maybeDecompressBytes(b []byte) ([]byte, error) {
	if len(b) >= 2 && b[0] == 0x1f && b[1] == 0x8b { // Valid gzip header bytes
		var out bytes.Buffer
		if r, err := gzip.NewReader(bytes.NewReader(b)); err != nil {
			return nil, fmt.Errorf("decompressing bytes: %w", err)
		} else if _, err = io.Copy(&out, r); err != nil {
			return nil, fmt.Errorf("reading decompressed bytes: %w", err)
		} else if err := r.Close(); err != nil {
			return nil, fmt.Errorf("closing gzip reader: %w", err)
		}
		return out.Bytes(), nil
	} else {
		log.Info("loaded uncompressed bytes")
		return b, nil
	}
}
