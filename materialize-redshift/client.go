package main

import (
	"bytes"
	"compress/gzip"
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
)

var _ sql.SchemaManager = (*client)(nil)

type client struct {
	db  *stdsql.DB
	cfg *config
	ep  *sql.Endpoint
}

func newClient(ctx context.Context, ep *sql.Endpoint) (sql.Client, error) {
	cfg := ep.Config.(*config)

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

func (c *client) InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error) {
	catalog := c.cfg.Database
	if catalog == "" {
		// An endpoint-level database configuration is not required, so query for the active
		// database if that's the case.
		if err := c.db.QueryRowContext(ctx, "select current_database();").Scan(&catalog); err != nil {
			return nil, fmt.Errorf("querying for connected database: %w", err)
		}
	}

	return sql.StdFetchInfoSchema(ctx, c.db, c.ep.Dialect, catalog, resourcePaths)
}

func (c *client) PutSpec(ctx context.Context, updateSpec sql.MetaSpecsUpdate) error {
	// Compress the spec bytes to store, since we are using a VARBYTE column with a limit of 1MB.
	// TODO(whb): This will go away when we start passing in the last validated spec to Validate
	// calls and stop needing to persist a spec at all.
	// updateSpec.Parameters
	specB64 := updateSpec.Parameters[1] // The second parameter is the spec
	specBytes, err := base64.StdEncoding.DecodeString(specB64.(string))
	if err != nil {
		return fmt.Errorf("decoding base64 spec prior to compressing: %w", err)
	}

	// Sanity check that this is indeed a spec. By all rights this is totally unnecessary but it
	// makes me feel a little better about indexing updateSpec.Parameters up above.
	var spec pf.MaterializationSpec
	if err := spec.Unmarshal(specBytes); err != nil {
		return fmt.Errorf("application logic error - specBytes was not a spec: %w", err)
	}

	compressed, err := compressBytes(specBytes)
	if err != nil {
		return fmt.Errorf("compressing spec bytes: %w", err)
	}

	updateSpec.Parameters[1] = base64.StdEncoding.EncodeToString(compressed)

	_, err = c.db.ExecContext(ctx, updateSpec.ParameterizedQuery, updateSpec.Parameters...)
	return err
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

func (c *client) AlterTable(ctx context.Context, ta sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	// Redshift only allows a single column to be added per ALTER TABLE statement. Also, we will
	// never need to drop nullability constraints, since Redshift does not allow dropping
	// nullability and we don't ever create columns as NOT NULL as a result.
	if len(ta.DropNotNulls) != 0 { // sanity check
		return "", nil, fmt.Errorf("redshift cannot drop nullability constraints but got %d DropNotNulls for table %s", len(ta.DropNotNulls), ta.Identifier)
	}

	statements := []string{}
	for _, c := range ta.AddColumns {
		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s %s;",
			ta.Identifier,
			c.Identifier,
			c.NullableDDL,
		))
	}

	if len(ta.ColumnTypeChanges) > 0 {
		for _, m := range ta.ColumnTypeChanges {
			if steps, err := sql.StdColumnTypeMigration(ctx, c.ep.Dialect, ta.Table, m); err != nil {
				return "", nil, fmt.Errorf("rendering column migration steps: %w", err)
			} else {
				statements = append(statements, steps...)
			}
		}
	}

	// Each table column addition statement is a separate statement, but will be grouped
	// together in a single multi-statement query.
	alterStmt := "BEGIN; " + strings.Join(statements, "\n") + " END; "

	return alterStmt, func(ctx context.Context) error {
		_, err := c.db.ExecContext(ctx, alterStmt)
		return err
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

	return out, nil
}

func (c *client) CreateSchema(ctx context.Context, schemaName string) error {
	return sql.StdCreateSchema(ctx, c.db, c.ep.Dialect, schemaName)
}

func preReqs(ctx context.Context, conf any, tenant string) *sql.PrereqErr {
	errs := &sql.PrereqErr{}

	cfg := conf.(*config)

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
		var pgErr *pgconn.PgError
		var netConnErr *net.DNSError
		var netOpErr *net.OpError

		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "28000":
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

	s3client, err := cfg.toS3Client(ctx)
	if err != nil {
		// This is not caused by invalid S3 credentials, and would most likely be a logic error in
		// the connector code.
		errs.Err(err)
		return errs
	}

	// Test creating, reading, and deleting an object from the configured bucket and bucket path.
	testKey := path.Join(cfg.BucketPath, uuid.NewString())

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
				err = fmt.Errorf("not authorized to write to %q", path.Join(cfg.Bucket, cfg.BucketPath))
			}
		}
		errs.Err(err)
	} else if _, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to read from %q", path.Join(cfg.Bucket, cfg.BucketPath))
		}
		errs.Err(err)
	} else if _, err := s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to delete from %q", path.Join(cfg.Bucket, cfg.BucketPath))
		}
		errs.Err(err)
	}

	return errs
}

func (c *client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (string, string, error) {
	var version, spec string

	if err := c.db.QueryRowContext(
		ctx,
		fmt.Sprintf(
			"SELECT version, spec FROM %s WHERE materialization = %s;",
			specs.Identifier,
			specs.Keys[0].Placeholder,
		),
		materialization.String(),
	).Scan(&version, &spec); err != nil {
		return "", "", err
	}

	if hexBytes, err := hex.DecodeString(spec); err != nil {
		return "", "", fmt.Errorf("hex.DecodeString: %w", err)
	} else if specBytes, err := base64.StdEncoding.DecodeString(string(hexBytes)); err != nil {
		return "", "", fmt.Errorf("base64.DecodeString: %w", err)
	} else if specBytes, err = maybeDecompressBytes(specBytes); err != nil {
		return "", "", fmt.Errorf("decompressing spec: %w", err)
	} else {
		return base64.StdEncoding.EncodeToString(specBytes), version, nil
	}
}

func (c *client) ExecStatements(ctx context.Context, statements []string) error {
	return c.withDB(func(db *stdsql.DB) error { return sql.StdSQLExecStatements(ctx, db, statements) })
}

func (c *client) InstallFence(ctx context.Context, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	if err := c.withDB(func(db *stdsql.DB) error {
		var err error
		fence, err = installFence(ctx, db, checkpoints, fence)
		if err != nil {
			return fmt.Errorf("installing fence: %w", err)
		}
		return nil
	}); err != nil {
		return sql.Fence{}, err
	}

	return fence, nil
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
	if b[0] == 0x1f && b[1] == 0x8b { // Valid gzip header bytes
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

// installFence is a modified version of sql.StdInstallFence that handles
// compression of the checkpoint and reading varbyte values from Redshift.
func installFence(ctx context.Context, db *stdsql.DB, checkpoints sql.Table, fence sql.Fence) (sql.Fence, error) {
	// TODO(whb): With the historical usage of sql.StdInstallFence, we were actually
	// base64 encoding the checkpoint bytes and then sending that UTF8 string to
	// Redshift, which stores those characters as bytes in the VARBYTE column. A
	// slightly more direct & efficient way to handle this would be to store the
	// bytes directly using TO_VARBYTE(checkpoint, 'base64'). This would require
	// handling for the pre-existing checkpoints that were encoded in the previous
	// way, and is not being implemented right now.
	var txn, err = db.BeginTx(ctx, nil)
	if err != nil {
		return sql.Fence{}, fmt.Errorf("db.BeginTx: %w", err)
	}
	defer func() {
		if txn != nil {
			_ = txn.Rollback()
		}
	}()

	// Increment the fence value of _any_ checkpoint which overlaps our key range.
	if _, err = txn.Exec(
		fmt.Sprintf(`
			UPDATE %s
				SET fence=fence+1
				WHERE materialization=%s
				AND key_end>=%s
				AND key_begin<=%s
			;
			`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	); err != nil {
		return sql.Fence{}, fmt.Errorf("incrementing fence: %w", err)
	}

	// Read the checkpoint with the narrowest [key_begin, key_end] which fully overlaps our range.
	var readBegin, readEnd uint32
	var checkpoint string

	if err = txn.QueryRow(
		fmt.Sprintf(`
			SELECT fence, key_begin, key_end, checkpoint
				FROM %s
				WHERE materialization=%s
				AND key_begin<=%s
				AND key_end>=%s
				ORDER BY key_end - key_begin ASC
				LIMIT 1
			;
			`,
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
	).Scan(&fence.Fence, &readBegin, &readEnd, &checkpoint); err == stdsql.ErrNoRows {
		// Set an invalid range, which compares as unequal to trigger an insertion below.
		readBegin, readEnd = 1, 0
	} else if err != nil {
		return sql.Fence{}, fmt.Errorf("scanning fence and checkpoint: %w", err)
	} else if hexBytes, err := hex.DecodeString(checkpoint); err != nil {
		return sql.Fence{}, fmt.Errorf("hex.DecodeString(checkpoint): %w", err)
	} else if base64Bytes, err := base64.StdEncoding.DecodeString(string(hexBytes)); err != nil {
		return sql.Fence{}, fmt.Errorf("base64.Decode(string(decompressed)): %w", err)
	} else if fence.Checkpoint, err = maybeDecompressBytes(base64Bytes); err != nil {
		return sql.Fence{}, fmt.Errorf("maybeDecompressBytes(fenceHexBytes): %w", err)
	}

	// If a checkpoint for this exact range doesn't exist then insert it now.
	if readBegin == fence.KeyBegin && readEnd == fence.KeyEnd {
		// Exists; no-op.
	} else if compressedCheckpoint, err := compressBytes(fence.Checkpoint); err != nil {
		return sql.Fence{}, fmt.Errorf("compressing checkpoint: %w", err)
	} else if _, err = txn.Exec(
		fmt.Sprintf(
			"INSERT INTO %s (materialization, key_begin, key_end, fence, checkpoint) VALUES (%s, %s, %s, %s, %s);",
			checkpoints.Identifier,
			checkpoints.Keys[0].Placeholder,
			checkpoints.Keys[1].Placeholder,
			checkpoints.Keys[2].Placeholder,
			checkpoints.Values[0].Placeholder,
			checkpoints.Values[1].Placeholder,
		),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
		fence.Fence,
		base64.StdEncoding.EncodeToString(compressedCheckpoint),
	); err != nil {
		return sql.Fence{}, fmt.Errorf("inserting fence: %w", err)
	}

	err = txn.Commit()
	txn = nil // Disable deferred rollback.

	if err != nil {
		return sql.Fence{}, fmt.Errorf("txn.Commit: %w", err)
	}
	return fence, nil
}

// updateFence updates a fence and reports if the materialization instance was
// fenced off. It handles compression of the checkpoint, and is used instead of
// the typical templated fence update query because of that.
func updateFence(ctx context.Context, txn pgx.Tx, dialect sql.Dialect, fence sql.Fence) error {
	if compressedCheckpoint, err := compressBytes(fence.Checkpoint); err != nil {
		return fmt.Errorf("compressing checkpoint: %w", err)
	} else if res, err := txn.Exec(ctx, fmt.Sprintf(
		"UPDATE %s SET checkpoint = $1 WHERE materialization = $2 AND key_begin = $3 AND key_end = $4 AND fence = $5;",
		dialect.Identifier(fence.TablePath...),
	),
		base64.StdEncoding.EncodeToString(compressedCheckpoint),
		fence.Materialization,
		fence.KeyBegin,
		fence.KeyEnd,
		fence.Fence,
	); err != nil {
		return fmt.Errorf("fetching fence update rows: %w", err)
	} else if res.RowsAffected() != 1 {
		return fmt.Errorf("this instance was fenced off by another")
	}

	return nil
}
