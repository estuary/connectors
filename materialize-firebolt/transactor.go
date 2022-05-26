package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

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
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// Transactions implements the DriverServer interface.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	var cfg config
	if err := pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	var checkpoint FireboltCheckpoint
	if open.Open.DriverCheckpointJson != nil {
		if err := json.Unmarshal(open.Open.DriverCheckpointJson, &checkpoint); err != nil {
			return fmt.Errorf("parsing driver config: %w", err)
		}
	}

	fb, err := firebolt.New(firebolt.Config{
		EngineURL: cfg.EngineURL,
		Database:  cfg.Database,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})
	if err != nil {
		return fmt.Errorf("creating firebolt client: %w", err)
	}

	var bindings []*binding
	for _, b := range open.Open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("parsing resource config: %w", err)
		}
		bindings = append(bindings,
			&binding{
				table: res.Table,
				spec:  b,
			})
	}

	queries, err := schemalate.GetQueriesBundle(open.Open.Materialization)
	if err != nil {
		return fmt.Errorf("building firebolt search schema: %w", err)
	}

	var transactor = &transactor{
		fb:           fb,
		queries:      queries,
		checkpoint:   checkpoint,
		bindings:     bindings,
		awsKeyId:     cfg.AWSKeyId,
		awsSecretKey: cfg.AWSSecretKey,
		awsRegion:    cfg.AWSRegion,
		bucket:       cfg.S3Bucket,
		prefix:       strings.TrimLeft(fmt.Sprintf("%s/%s/", cfg.S3Prefix, open.Open.Materialization.Materialization.String()), "/"),
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	log.SetLevel(log.DebugLevel)
	var log = log.WithField("materialization", "firebolt")
	return pm.RunTransactions(stream, transactor, log)
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
	checkpoint   FireboltCheckpoint
	awsKeyId     string
	awsSecretKey string
	awsRegion    string
	bucket       string
	prefix       string
	bindings     []*binding
}

// firebolt is delta-update only, so loading of data from Firebolt is not necessary
func (t *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	panic("delta updates have no load phase")
}

func (t *transactor) Prepare(_ context.Context, _ pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	var queries []string
	var files []TemporaryFileRecord
	for i, _ := range t.bindings {
		var randomKey, err = uuid.NewRandom()
		if err != nil {
			return pf.DriverCheckpoint{}, fmt.Errorf("generating random key for file: %w", err)
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
	checkpoint := FireboltCheckpoint{
		Queries: queries,
		Files:   files,
	}

	jsn, err := json.Marshal(checkpoint)
	if err != nil {
		return pf.DriverCheckpoint{}, fmt.Errorf("creating checkpoint json: %w", err)
	}
	t.checkpoint = checkpoint

	return pf.DriverCheckpoint{DriverCheckpointJson: jsn}, nil
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

func (t *transactor) Store(it *pm.StoreIterator) error {
	var awsConfig = aws.Config{
		Credentials: credentials.NewStaticCredentials(t.awsKeyId, t.awsSecretKey, ""),
		Region:      &t.awsRegion,
	}
	var sess = session.Must(session.NewSession(&awsConfig))
	var uploader = s3manager.NewUploader(sess)

	var pipes = make([]*io.PipeWriter, len(t.bindings))
	g, ctx := errgroup.WithContext(it.Context())

	for it.Next() {
		var doc, err = t.projectDocument(t.bindings[it.Binding].spec, it.Key, it.Values)
		if err != nil {
			return fmt.Errorf("projecting new store json: %w", err)
		}

		if pipes[it.Binding] == nil {
			var reader, writer = io.Pipe()
			pipes[it.Binding] = writer

			cp := t.checkpoint.Files[it.Binding]
			g.Go(func() error {
				var _, err = uploader.UploadWithContext(ctx, &s3manager.UploadInput{
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

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (t *transactor) Commit(ctx context.Context) error {
	return nil
}

// Acknowledge loads stored files from external table to main table
func (t *transactor) Acknowledge(context.Context) error {

	if len(t.checkpoint.Files) < 1 {
		return nil
	}

	// Acknowledge step might end up being run by a separate process
	// different from the original, and the new transactor process might be initialised with
	// a new version of the MaterializationSpec. This means it's possible to have an Acknowledge
	// with MaterializationSpec v1 be run by a transactor initialised with MaterializationSpec v2.
	// As such, we keep a copy of the queries generated from the MaterializationSpec of the
	// transaction to which this Acknowledge belongs in the checkpoint to avoid inconsistencies.
	for _, query := range t.checkpoint.Queries {
		_, err := t.fb.Query(query)
		if err != nil {
			return fmt.Errorf("moving files from external to main table: %w", err)
		}
	}

	t.checkpoint.Files = []TemporaryFileRecord{}
	t.checkpoint.Queries = []string{}

	return nil
}

func randomDocumentKey() string {
	const charset = "abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const length = 32

	var seededRand *rand.Rand = rand.New(
		rand.NewSource(time.Now().UnixNano()))

	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func (t *transactor) Destroy() {}
