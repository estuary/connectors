package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/estuary/connectors/materialize-firebolt/firebolt"
	"github.com/estuary/connectors/materialize-firebolt/schemalate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// Transactions implements the DriverServer interface.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	return pm.RunTransactions(stream, func(ctx context.Context, open pm.TransactionRequest_Open) (pm.Transactor, *pm.TransactionResponse_Opened, error) {

		var cfg config
		if err := pf.UnmarshalStrict(open.Materialization.EndpointSpecJson, &cfg); err != nil {
			return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
		}

		var checkpoint FireboltCheckpoint
		if open.DriverCheckpointJson != nil {
			if err := json.Unmarshal(open.DriverCheckpointJson, &checkpoint); err != nil {
				return nil, nil, fmt.Errorf("parsing driver config: %w", err)
			}
		}

		var fb, err = firebolt.New(firebolt.Config{
			EngineURL: cfg.EngineURL,
			Database:  cfg.Database,
			Username:  cfg.Username,
			Password:  cfg.Password,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("creating firebolt client: %w", err)
		}

		if err = applyCheckpoint(ctx, fb, checkpoint); err != nil {
			return nil, nil, fmt.Errorf("applying recovered checkpoint: %w", err)
		}

		var bindings []*binding
		for _, b := range open.Materialization.Bindings {
			var res resource
			if err := pf.UnmarshalStrict(b.ResourceSpecJson, &res); err != nil {
				return nil, nil, fmt.Errorf("parsing resource config: %w", err)
			}
			bindings = append(bindings,
				&binding{
					table: res.Table,
					spec:  b,
				})
		}

		queries, err := schemalate.GetQueriesBundle(open.Materialization)
		if err != nil {
			return nil, nil, fmt.Errorf("building firebolt search schema: %w", err)
		}

		var transactor = &transactor{
			fb:           fb,
			queries:      queries,
			bindings:     bindings,
			awsKeyId:     cfg.AWSKeyId,
			awsSecretKey: cfg.AWSSecretKey,
			awsRegion:    cfg.AWSRegion,
			bucket:       cfg.S3Bucket,
			prefix:       fmt.Sprintf("%s/%s/", CleanPrefix(cfg.S3Prefix), open.Materialization.Materialization),
		}

		return transactor, &pm.TransactionResponse_Opened{}, nil
	})
}

type binding struct {
	table string
	spec  *pf.MaterializationSpec_Binding
}

type TemporaryFileRecord struct {
	Bucket string
	Key    string
}

type FireboltCheckpoint struct {
	Files   []TemporaryFileRecord
	Queries []string
}

type transactor struct {
	fb           *firebolt.Client
	queries      *schemalate.QueriesBundle
	awsKeyId     string
	awsSecretKey string
	awsRegion    string
	bucket       string
	prefix       string
	bindings     []*binding
}

// firebolt is delta-update only, so loading of data from Firebolt is not necessary
func (t *transactor) Load(it *pm.LoadIterator, loaded func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("delta updates have no loads")
	}
	return nil
}

func (t *transactor) buildCheckpoint() (FireboltCheckpoint, error) {
	var queries []string
	var files []TemporaryFileRecord
	for i := range t.bindings {
		var randomKey, err = uuid.NewRandom()
		if err != nil {
			return FireboltCheckpoint{}, fmt.Errorf("generating random key for file: %w", err)
		}
		var key = fmt.Sprintf("%s%s.json", t.prefix, randomKey)
		files = append(files, TemporaryFileRecord{
			Bucket: t.bucket,
			Key:    key,
		})

		var insertQuery = t.queries.Bindings[i].InsertFromTable
		var values = fmt.Sprintf("'%s'", key)
		queries = append(queries, strings.Replace(insertQuery, "?", values, -1))
	}
	return FireboltCheckpoint{
		Queries: queries,
		Files:   files,
	}, nil

}

func (t *transactor) projectDocument(spec *pf.MaterializationSpec_Binding, keys tuple.Tuple, values tuple.Tuple) ([]byte, error) {
	var document = make(map[string]interface{})

	// Add the keys to the document.
	for i, value := range keys {
		var propName = spec.FieldSelection.Keys[i]
		document[propName] = value
	}

	// Add the non-keys to the document.
	for i, value := range values {
		var propName = spec.FieldSelection.Values[i]

		if raw, ok := value.([]byte); ok {
			var nestedObject = make(map[string]interface{})
			err := json.Unmarshal(raw, &nestedObject)

			// If we can parse this raw json as an object, we store it as a
			// stringified JSON object, since Firebolt does not support JSON objects.
			if err == nil {
				document[propName] = string(raw)
			} else {
				document[propName] = json.RawMessage(raw)
			}
		} else {
			document[propName] = value
		}
	}

	jsonDoc, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize the addition document: %w", err)
	}

	return jsonDoc, nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	var awsConfig = aws.Config{
		Credentials: credentials.NewStaticCredentials(t.awsKeyId, t.awsSecretKey, ""),
		Region:      &t.awsRegion,
	}
	var session = session.Must(session.NewSession(&awsConfig))
	var uploader = s3manager.NewUploader(session)
	var pipes = make([]*io.PipeWriter, len(t.bindings))
	var group, groupCtx = errgroup.WithContext(it.Context())

	var checkpoint, err = t.buildCheckpoint()
	if err != nil {
		return nil, fmt.Errorf("building checkpoint: %w", err)
	}

	for it.Next() {
		var doc, err = t.projectDocument(t.bindings[it.Binding].spec, it.Key, it.Values)
		if err != nil {
			return nil, fmt.Errorf("projecting new store json: %w", err)
		}

		if pipes[it.Binding] == nil {
			var reader, writer = io.Pipe()
			pipes[it.Binding] = writer

			var cp = checkpoint.Files[it.Binding]
			group.Go(func() error {
				var _, err = uploader.UploadWithContext(groupCtx, &s3manager.UploadInput{
					Bucket: aws.String(cp.Bucket),
					Key:    aws.String(cp.Key),
					Body:   reader,
				})
				return err
			})
		}
		var pipe = pipes[it.Binding]

		pipe.Write(doc)
		pipe.Write([]byte("\n"))
	}

	for _, pipe := range pipes {
		if pipe != nil {
			pipe.Close()
		}
	}

	if err = group.Wait(); err != nil {
		return nil, err
	}

	// Return a StartCommitFunc closure which returns our encoded `checkpoint`,
	// and will apply it only after the runtime acknowledges its commit.
	return func(ctx context.Context, _ []byte, runtimeAckCh <-chan struct{}) (*pf.DriverCheckpoint, pf.OpFuture) {
		var checkpointJSON, err = json.Marshal(checkpoint)
		if err != nil {
			return nil, pf.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		var commitOp = pf.RunAsyncOperation(func() error {
			select {
			case <-runtimeAckCh:
				return applyCheckpoint(ctx, t.fb, checkpoint)
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		return &pf.DriverCheckpoint{DriverCheckpointJson: checkpointJSON}, commitOp
	}, nil
}

// applyCheckpoint loads stored files from external table to main table
func applyCheckpoint(ctx context.Context, fb *firebolt.Client, checkpoint FireboltCheckpoint) error {
	if len(checkpoint.Files) < 1 {
		return nil
	}

	// Acknowledge step might end up being run by a separate process
	// different from the original, and the new transactor process might be initialised with
	// a new version of the MaterializationSpec. This means it's possible to have an Acknowledge
	// with MaterializationSpec v1 be run by a transactor initialised with MaterializationSpec v2.
	// As such, we keep a copy of the queries generated from the MaterializationSpec of the
	// transaction to which this Acknowledge belongs in the checkpoint to avoid inconsistencies.
	for _, query := range checkpoint.Queries {
		_, err := fb.Query(query)
		if err != nil {
			return fmt.Errorf("moving files from external to main table: %w", err)
		}
	}

	return nil
}

func (t *transactor) Destroy() {}
