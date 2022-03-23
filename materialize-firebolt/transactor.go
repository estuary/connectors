package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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
	log "github.com/sirupsen/logrus"
)

// Transactions implements the DriverServer interface.
func (d driver) Transactions(stream pm.Driver_TransactionsServer) error {
	log.Info("FIREBOLT Transactions")
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
		prefix:       cfg.S3Prefix,
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
	Bucket  string
	Key     string
	Binding int
}

type FireboltCheckpoint struct {
	Files   []TemporaryFileRecord
	Queries schemalate.QueriesBundle
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
	log.Info("FIREBOLT Prepare")
	checkpoint := FireboltCheckpoint{
		Queries: *t.queries,
	}
	for i, _ := range t.bindings {
		checkpoint.Files = append(checkpoint.Files, TemporaryFileRecord{
			Bucket:  t.bucket,
			Key:     fmt.Sprintf("%s%s.json", t.prefix, randomDocumentKey()),
			Binding: i,
		})
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
			document[propName] = json.RawMessage(raw)
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

// write file to S3 and keep a record of them to be copied to the table
func (t *transactor) Store(it *pm.StoreIterator) error {
	log.Info("FIREBOLT Store")
	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentials(t.awsKeyId, t.awsSecretKey, ""),
		Region:      &t.awsRegion,
	}
	sess := session.Must(session.NewSession(&awsConfig))
	uploader := s3manager.NewUploader(sess)

	uploads := []s3manager.BatchUploadObject{}

	files := make(map[int][]byte)

	for it.Next() {
		doc, err := t.projectDocument(t.bindings[it.Binding].spec, it.Key, it.Values)
		if err != nil {
			return fmt.Errorf("projecting new store json: %w", err)
		}

		if _, ok := files[it.Binding]; ok {
			files[it.Binding] = append(files[it.Binding], byte('\n'))
			files[it.Binding] = append(files[it.Binding], doc...)
		} else {
			files[it.Binding] = doc
		}
	}

	for bindingIndex, data := range files {
		cp := t.checkpoint.Files[bindingIndex]
		log.Info("FIREBOLT STORE WITH KEY", cp.Key)

		uploads = append(uploads, s3manager.BatchUploadObject{
			Object: &s3manager.UploadInput{
				Bucket: aws.String(cp.Bucket),
				Key:    aws.String(cp.Key),
				Body:   bytes.NewReader(data),
			},
		})
	}

	uploadIterator := s3manager.UploadObjectsIterator{
		Objects: uploads,
	}

	err := uploader.UploadWithIterator(aws.BackgroundContext(), &uploadIterator)
	if err != nil {
		return fmt.Errorf("uploading files to s3: %w", err)
	}

	return nil
}

func (t *transactor) Commit(ctx context.Context) error {
	log.Info("FIREBOLT Commit")
	return nil
}

// load stored file into table
func (t *transactor) Acknowledge(context.Context) error {
	log.Info("FIREBOLT Acknowledge")

	if len(t.checkpoint.Files) < 1 {
		return nil
	}

	sourceFileNames := map[int][]string{}
	for _, item := range t.checkpoint.Files {
		binding := item.Binding
		fileName := item.Key

		if _, ok := sourceFileNames[binding]; ok {
			sourceFileNames[binding] = append(sourceFileNames[binding], fileName)
		} else {
			sourceFileNames[binding] = []string{fileName}
		}
	}

	// Acknowledge step might end up being run by a separate process
	// different from the original, and the new transactor process might be initialised with
	// a new version of the MaterializationSpec. This means it's possible to have an Acknowledge
	// with MaterializationSpec v1 be run by a transactor initialised with MaterializationSpec v2.
	// As such, we keep a copy of the queries generated from the MaterializationSpec of the
	// transaction to which this Acknowledge belongs in the checkpoint to avoid inconsistencies.
	for binding, fileNames := range sourceFileNames {
		log.Info("FIREBOLT moving files from binding ", binding, " for ", sourceFileNames)
		insertQuery := t.checkpoint.Queries.Bindings[binding].InsertFromTable
		values := fmt.Sprintf("'%s'", strings.Join(fileNames, "','"))
		_, err := t.fb.Query(strings.Replace(insertQuery, "?", values, 1))
		if err != nil {
			return fmt.Errorf("moving files from external to main table: %w", err)
		}
	}

	t.checkpoint.Files = []TemporaryFileRecord{}

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
