package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/alecthomas/jsonschema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-firebolt/firebolt"
	"github.com/estuary/connectors/materialize-firebolt/schemalate"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	proto "github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
)

type config struct {
	EngineURL    string `json:"engine_url"`
	Database     string `json:"database"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	AWSKeyId     string `json:"aws_key_id,omitempty"`
	AWSSecretKey string `json:"aws_secret_key,omitempty"`
	AWSRegion    string `json:"aws_region,omitempty"`
	S3Bucket     string `json:"s3_bucket"`
	S3Prefix     string `json:"s3_prefix,omitempty"`
}

func (c config) Validate() error {
	if c.EngineURL == "" {
		return fmt.Errorf("missing required engine_url")
	}
	if c.Database == "" {
		return fmt.Errorf("missing required database")
	}
	if c.Username == "" {
		return fmt.Errorf("missing required username")
	}
	if c.Password == "" {
		return fmt.Errorf("missing required password")
	}
	if c.S3Bucket == "" {
		return fmt.Errorf("missing required bucket")
	}
	return nil
}

type resource struct {
	Table        string `json:"table"`
	TableType    string `json:"table_type"`
	PrimaryIndex string `json:"primary_index"`
}

func (r resource) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing required table")
	}

	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Table":
		return "Name of the Firebolt table to store materialized results in. The external table will be named after this table with an `_external` suffix."
	case "Bucket":
		return "Name of S3 bucket where the intermediate files for external table will be stored. Should be an empty S3 bucket."
	case "Prefix":
		return "A prefix for files stored in the bucket."
	case "AWSKeyId":
		return "AWS Key ID for accessing the S3 bucket."
	case "AWSSecretKey":
		return "AWS Secret Key for accessing the S3 bucket."
	case "Region":
		return "AWS Region the bucket is in."
	default:
		return ""
	}
}

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	log.Info("FIREBOLT Spec")
	endpointSchema, err := jsonschema.Reflect(&config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := jsonschema.Reflect(&resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev/reference/Connectors/materialization-connectors/Firebolt/",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	log.Info("FIREBOLT Validate")
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	_, err := firebolt.New(firebolt.Config{
		EngineURL: cfg.EngineURL,
		Database:  cfg.Database,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("creating firebolt client: %w", err)
	}

	awsConfig := aws.Config{
		Credentials: credentials.NewStaticCredentials(cfg.AWSKeyId, cfg.AWSSecretKey, ""),
		Region:      &cfg.AWSRegion,
	}
	sess := session.Must(session.NewSession(&awsConfig))
	downloader := s3manager.NewDownloader(sess)
	buf := aws.NewWriteAtBuffer([]byte{})
	existingSpecKey := fmt.Sprintf("%s%s.flow.materialization_spec", cfg.S3Prefix, req.Materialization)

	var existing pf.MaterializationSpec
	haveExisting := false

	_, err = downloader.Download(buf, &s3.GetObjectInput{
		Bucket: &cfg.S3Bucket,
		Key:    &existingSpecKey,
	})

	if err != nil {
		// The file not existing is fine, otherwise it's an actual error
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() != s3.ErrCodeNoSuchKey {
			return nil, fmt.Errorf("downloading existing spec failed: %w", err)
		}
	} else {
		err = proto.Unmarshal(buf.Bytes(), &existing)
		if err != nil {
			return nil, fmt.Errorf("parsing existing materialization spec: %w", err)
		}
		haveExisting = true
	}

	var out []*pm.ValidateResponse_Binding
	for _, proposed := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(proposed.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// TODO: Make sure we have read/write access to the S3 path with the given credentials
		// TODO: Make sure the table, if it already exists, is valid with our expected schema

		// TODO: First load any existing MaterializationSpec and validate against that, otherwise validate new
		// Make sure the specified resource is valid to build
		var constraints map[string]*pm.Constraint

		if haveExisting {
			var existingBinding *pf.MaterializationSpec_Binding = nil
			for _, b := range existing.Bindings {
				var r resource
				if err := pf.UnmarshalStrict(proposed.ResourceSpecJson, &r); err != nil {
					return nil, fmt.Errorf("parsing resource config: %w", err)
				}
				if r.Table == res.Table {
					existingBinding = b
					break
				}
			}

			if existingBinding != nil {
				constraints, err = schemalate.ValidateExistingProjection(existingBinding, proposed)
			} else {
				// TODO error or just validate new projection?
			}
		} else {
			constraints, err = schemalate.ValidateNewProjection(proposed)
		}

		if err != nil {
			return nil, fmt.Errorf("building firebolt schema: %w", err)
		} else {
			out = append(out, &pm.ValidateResponse_Binding{
				Constraints:  constraints,
				DeltaUpdates: true,
				ResourcePath: []string{res.Table},
			})
		}
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

func (d driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	log.Info("FIREBOLT ApplyUpsert")
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	fb, err := firebolt.New(firebolt.Config{
		EngineURL: cfg.EngineURL,
		Database:  cfg.Database,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})

	if err != nil {
		return nil, fmt.Errorf("creating firebolt client: %w", err)
	}

	var tables []string

	if queries, err := schemalate.GetQueriesBundle(
		req.Materialization,
	); err != nil {
		return nil, fmt.Errorf("building firebolt search schema: %w", err)
	} else {
		for i, bundle := range queries.Bindings {
			log.Info("FIREBOLT Queries ", bundle.CreateExternalTable, bundle.CreateTable, bundle.InsertFromTable)
			_, err := fb.Query(bundle.CreateExternalTable)
			if err != nil {
				return nil, fmt.Errorf("running table creation query: %w", err)
			}

			_, err = fb.Query(bundle.CreateTable)
			if err != nil {
				return nil, fmt.Errorf("running table creation query: %w", err)
			}

			tables = append(tables, string(req.Materialization.Bindings[i].ResourceSpecJson))
		}

		awsConfig := aws.Config{
			Credentials: credentials.NewStaticCredentials(cfg.AWSKeyId, cfg.AWSSecretKey, ""),
			Region:      &cfg.AWSRegion,
		}
		sess := session.Must(session.NewSession(&awsConfig))
		uploader := s3manager.NewUploader(sess)
		materializationBytes, err := proto.Marshal(req.Materialization)
		if err != nil {
			return nil, fmt.Errorf("marshalling materialization spec: %w", err)
		}
		specKey := fmt.Sprintf("%s%s.flow.materialization_spec", cfg.S3Prefix, req.Materialization.Materialization)

		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: &cfg.S3Bucket,
			Key:    &specKey,
			Body:   bytes.NewReader(materializationBytes),
		})

		if err != nil {
			return nil, fmt.Errorf("uploading materialization spec %s: %w", specKey, err)
		}
	}

	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("created tables: ", strings.Join(tables, ","))}, nil
}

func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	log.Info("FIREBOLT ApplyDelete")
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	fb, err := firebolt.New(firebolt.Config{
		EngineURL: cfg.EngineURL,
		Database:  cfg.Database,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})

	if err != nil {
		return nil, fmt.Errorf("creating firebolt client: %w", err)
	}

	var tables []string
	if queries, err := schemalate.GetQueriesBundle(
		req.Materialization,
	); err != nil {
		return nil, fmt.Errorf("building firebolt search schema: %w", err)
	} else {
		for i, bundle := range queries.Bindings {
			log.Info("FIREBOLT Queries ", bundle.DropTable, bundle.DropExternalTable)
			_, err := fb.Query(bundle.DropTable)
			if err != nil {
				return nil, fmt.Errorf("running table drop query: %w", err)
			}

			_, err = fb.Query(bundle.DropExternalTable)
			if err != nil {
				return nil, fmt.Errorf("running table drop query: %w", err)
			}

			tables = append(tables, string(req.Materialization.Bindings[i].ResourceSpecJson))
		}
	}

	if req.DryRun {
		return &pm.ApplyResponse{ActionDescription: fmt.Sprint("to delete tables: ", strings.Join(tables, ","))}, nil
	}

	// Delete table and external table
	return &pm.ApplyResponse{ActionDescription: fmt.Sprint("deleted tables: ", strings.Join(tables, ","))}, nil
}

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

	queries, err := schemalate.GetQueriesBundle(
		open.Open.Materialization,
	)
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
	Files []TemporaryFileRecord
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
	checkpoint := FireboltCheckpoint{}
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

	for binding, fileNames := range sourceFileNames {
		log.Info("FIREBOLT moving files from binding ", binding, " for ", sourceFileNames)
		insertQuery := t.queries.Bindings[binding].InsertFromTable
		values := fmt.Sprintf("'%s'", strings.Join(fileNames, "','"))
		_, err := t.fb.Query(strings.Replace(insertQuery, "?", values, 1))
		if err != nil {
			return fmt.Errorf("moving files from external to main table: %w", err)
		}
	}

	t.checkpoint.Files = []TemporaryFileRecord{}

	return nil
}

func documentId(tuple tuple.Tuple) string {
	return base64.RawStdEncoding.EncodeToString(tuple.Pack())
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

func main() { boilerplate.RunMain(new(driver)) }
