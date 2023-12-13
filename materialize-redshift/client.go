package main

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
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
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"golang.org/x/sync/errgroup"
)

type client struct {
	uri string
}

func (c client) InfoSchema(ctx context.Context, ep *sql.Endpoint, resourcePaths [][]string) (is *boilerplate.InfoSchema, err error) {
	cfg := ep.Config.(*config)

	if err := c.withDB(func(db *stdsql.DB) error {
		catalog := cfg.Database
		if catalog == "" {
			// An endpoint-level database configuration is not required, so query for the active
			// database if that's the case.
			if err := db.QueryRowContext(ctx, "select current_database();").Scan(&catalog); err != nil {
				return fmt.Errorf("querying for connected database: %w", err)
			}
		}

		baseSchema := cfg.Schema
		if baseSchema == "" {
			baseSchema = "public"
		}

		is, err = sql.StdFetchInfoSchema(ctx, db, ep.Dialect, catalog, baseSchema, resourcePaths)
		return err
	}); err != nil {
		return nil, err
	}
	return
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

	resolved, err := sql.ResolveActions(ctx, db, actions, rsDialect, catalog)
	if err != nil {
		return "", fmt.Errorf("resolving apply actions: %w", err)
	}

	statements := []string{}
	for _, tc := range resolved.CreateTables {
		statements = append(statements, tc.TableCreateSql)
	}

	// Redshift only allows a single column to be added per ALTER TABLE statement. Also, we will
	// never need to drop nullability constraints, since Redshift does not allow dropping
	// nullability and we don't ever create columns as NOT NULL as a result.
	for _, ta := range resolved.AlterTables {
		if len(ta.DropNotNulls) != 0 { // sanity check
			return "", fmt.Errorf("logic error: redshift cannot drop nullability constraints but got %d DropNotNulls", len(ta.DropNotNulls))
		}
		if len(ta.AddColumns) > 0 {
			addColStmts := []string{}
			for _, c := range ta.AddColumns {
				addColStmts = append(addColStmts, fmt.Sprintf(
					"ALTER TABLE %s ADD COLUMN %s %s;",
					ta.Identifier,
					c.Identifier,
					c.NullableDDL,
				))
			}
			// Each table column addition statement is a separate statement, but will be grouped
			// together in a single multi-statement query.
			statements = append(statements, strings.Join(addColStmts, "\n"))
		}
	}

	for _, tr := range resolved.ReplaceTables {
		statements = append(statements, tr.TableReplaceSql)
	}

	// The spec will get updated last, after all the other actions are complete, but include it in
	// the description of actions.
	action := strings.Join(append(statements, updateSpec.QueryString), "\n")
	if req.DryRun {
		return action, nil
	}

	if len(resolved.ReplaceTables) > 0 {
		if err := sql.StdIncrementFence(ctx, db, ep, req.Materialization.Name.String()); err != nil {
			return "", err
		}
	}

	// Execute statements in parallel for efficiency. Each statement acts on a single table, and
	// everything that needs to be done for a given table is contained in that statement.
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(5)

	for _, s := range statements {
		s := s
		group.Go(func() error {
			if _, err := db.ExecContext(groupCtx, s); err != nil {
				return fmt.Errorf("executing apply statement: %w", err)
			}
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return "", err
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
	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if db, err := stdsql.Open("pgx", cfg.toURI()); err != nil {
		errs.Err(err)
	} else if err := db.PingContext(pingCtx); err != nil {
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
	} else {
		db.Close()
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

func (c client) FetchSpecAndVersion(ctx context.Context, specs sql.Table, materialization pf.Materialization) (specB64, version string, err error) {
	err = c.withDB(func(db *stdsql.DB) error {
		var specHex string
		specHex, version, err = sql.StdFetchSpecAndVersion(ctx, db, specs, materialization)
		if err != nil {
			return err
		}

		specBytes, err := hex.DecodeString(specHex)
		if err != nil {
			return err
		}

		specB64 = string(specBytes)
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
		fence, err = sql.StdInstallFence(ctx, db, checkpoints, fence, func(fenceHex string) ([]byte, error) {
			fenceHexBytes, err := hex.DecodeString(fenceHex)
			if err != nil {
				return nil, err
			}

			return base64.StdEncoding.DecodeString(string(fenceHexBytes))
		})
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
