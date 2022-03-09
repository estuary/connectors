package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/alecthomas/jsonschema"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-firebolt/firebolt"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type config struct {
	EngineURL    string `json:"engine_url"`
	Database     string `json:"database"`
	Username     string `json:"username,omitempty"`
	Password     string `json:"password,omitempty"`
	AWSKeyId     string `json:"aws_key_id"`
	AWSSecretKey string `json:"aws_secret_key"`
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
	return nil
}

type resource struct {
	Table     string `json:"table"`
	Bucket    string `json:"bucket"`
	Prefix    string `json:"prefix,omitempty"`
	AWSRegion string `json:"aws_region"`
}

func (r resource) Validate() error {
	if r.Table == "" {
		return fmt.Errorf("missing required table")
	}
	if r.Bucket == "" {
		return fmt.Errorf("missing required bucket")
	}
	if r.AWSRegion == "" {
		return fmt.Errorf("missing required region")
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

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Make sure we have read/write access to the S3 path with the given credentials
		/*awsConfig := aws.Config{
			Credentials: credentials.NewStaticCredentials(res.AWSKeyId, res.AWSSecretKey, ""),
		}
		sess, err := session.NewSession()
		if err != nil {
			return nil, fmt.Errorf("building s3 session: %w", err)
		}
		s3Client := s3.New(sess, &awsConfig)*/

		// Make sure the table, if it already exists, is valid with our expected schema
		// Make sure the specified resource is valid to build
		/*if schema, err := schemabuilder.RunSchemaBuilder(
			binding.Collection.SchemaJson,
		); err != nil {
			return nil, fmt.Errorf("building firebolt schema: %w", err)
		}*/

		var constraints = make(map[string]*pm.Constraint)

		for _, projection := range binding.Collection.Projections {
			var constraint = &pm.Constraint{}
			switch {
			case projection.IsRootDocumentProjection():
			case projection.IsPrimaryKey:
				constraint.Type = pm.Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document and primary key fields are needed."
			default:
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Non-root document fields and non-primary key fields are not needed."
			}
			constraints[projection.Field] = constraint
		}
		out = append(out, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.Table},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

func (driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	log.Info("FIREBOLT ApplyUpsert")
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var tables []string
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		// Create table and external table
		//if elasticSearchSchema, err := schemabuilder.RunSchemaBuilder(
		//binding.Collection.SchemaJson,
		//res.FieldOverides,
		//); err != nil {
		//return nil, fmt.Errorf("building elastic search schema: %w", err)
		//} else if err = elasticSearch.CreateIndex(res.Index, res.NumOfShards, res.NumOfReplicas, elasticSearchSchema, false); err != nil {
		//return nil, fmt.Errorf("creating elastic search index: %w", err)
		//}

		tables = append(tables, res.Table)
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

	var tables []string
	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		tables = append(tables, res.Table)
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
				table:        res.Table,
				bucket:       res.Bucket,
				prefix:       res.Prefix,
				awsKeyId:     cfg.AWSKeyId,
				awsSecretKey: cfg.AWSSecretKey,
				awsRegion:    res.AWSRegion,
			})
	}

	var transactor = &transactor{
		fb:              fb,
		bindings:        bindings,
		filesLeftToCopy: make([]temporaryFileRecord, 0),
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
	table        string
	bucket       string
	prefix       string
	awsKeyId     string
	awsSecretKey string
	awsRegion    string
}

type temporaryFileRecord struct {
	bucket       string
	key          string
	table        string
	awsKeyId     string
	awsSecretKey string
	awsRegion    string
}

type transactor struct {
	fb              *firebolt.Client
	bindings        []*binding
	filesLeftToCopy []temporaryFileRecord
}

// firebolt is delta-update only, so loading of data from Firebolt is not necessary
func (t *transactor) Load(it *pm.LoadIterator, _ <-chan struct{}, _ <-chan struct{}, loaded func(int, json.RawMessage) error) error {
	log.Info("FIREBOLT Load")
	return nil
}

func (t *transactor) Prepare(_ context.Context, _ pm.TransactionRequest_Prepare) (pf.DriverCheckpoint, error) {
	log.Info("FIREBOLT Prepare")
	if len(t.filesLeftToCopy) != 0 {
		panic("non-empty filesLeftToCopy") // Invariant: previous call is finished.
	}
	return pf.DriverCheckpoint{}, nil
}

// write file to S3 and keep a record of them to be copied to the table
func (t *transactor) Store(it *pm.StoreIterator) error {
	log.Info("FIREBOLT Store")
	for it.Next() {
		var b = t.bindings[it.Binding]

		awsConfig := aws.Config{
			Credentials: credentials.NewStaticCredentials(b.awsKeyId, b.awsSecretKey, ""),
			Region:      &b.awsRegion,
		}
		sess := session.Must(session.NewSession(&awsConfig))
		log.Info("FIREBOLT REGION", b.awsRegion)
		uploader := s3manager.NewUploader(sess)
		key := fmt.Sprintf("%s%s.json", b.prefix, documentId(it.Key))
		log.Info("FIREBOLT STORE WITH KEY", key)

		// delete _meta field for now. Once we have the proper schema translation
		// this won't be necessary
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(it.RawJSON), &m); err != nil {
			return fmt.Errorf("parsing store json: %w", err)
		}
		delete(m, "_meta")
		newJSONBytes, err := json.Marshal(m)

		if err != nil {
			return fmt.Errorf("marshalling new store json: %w", err)
		}

		item := &s3manager.UploadInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(newJSONBytes),
		}
		_, err = uploader.Upload(item)
		if err != nil {
			return fmt.Errorf("uploading file to s3: %w", err)
		}

		t.filesLeftToCopy = append(t.filesLeftToCopy, temporaryFileRecord{
			bucket:       b.bucket,
			key:          key,
			table:        b.table,
			awsKeyId:     b.awsKeyId,
			awsSecretKey: b.awsSecretKey,
		})
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
	for _, item := range t.filesLeftToCopy {
		queryString := fmt.Sprintf("INSERT INTO %s SELECT *, source_file_name FROM %s_external WHERE source_file_name='%s';", item.table, item.table, item.key)
		log.Info("QUERY STRING", queryString)
		resp, err := t.fb.Query(strings.NewReader(queryString))

		if err != nil {
			return fmt.Errorf("running data copy query: %w", err)
		}

		respBuf := new(strings.Builder)
		_, err = io.Copy(respBuf, resp.Body)
		resp.Body.Close()
		if err != nil {
			return fmt.Errorf("reading response of query failed: %w", err)
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("response error code %d, %s", resp.StatusCode, respBuf)
		}

		log.Info(respBuf)
	}

	t.filesLeftToCopy = t.filesLeftToCopy[:0]
	return nil
}

func documentId(tuple tuple.Tuple) string {
	return base64.RawStdEncoding.EncodeToString(tuple.Pack())
}

func (t *transactor) Destroy() {}

func main() { boilerplate.RunMain(new(driver)) }
