package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/benbjohnson/clock"
	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-s3-parquet/checkpoint"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/xitongsys/parquet-go/parquet"
)

type config struct {
	AWSAccessKeyID     string `json:"awsAccessKeyId" jsonschema:"title=Access Key ID" jsonschema_extras:"order=0"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey" jsonschema:"title=Secret Access Key" jsonschema_extras:"secret=true,order=1"`
	Region             string `json:"region" jsonschema:"title=Region" jsonschema_extras:"order=2"`
	Bucket             string `json:"bucket" jsonschema:"title=Bucket" jsonschema_extras:"order=3"`
	// The driver batches materialization results to local files first,
	// and uploads the local files to cloud (S3) on a schedule specified by
	// UploadIntervalInSeconds, which is the mimimal wait time (in seconds) between two
	// consecutive upload-to-cloud actions.
	UploadIntervalInSeconds int            `json:"uploadIntervalInSeconds" jsonschema:"title=Upload Interval in Seconds" jsonschema_extras:"order=4"`
	Advanced                advancedConfig `json:"advanced,omitempty" jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	Endpoint string `json:"endpoint,omitempty" jsonschema:"title=Endpoint"`
}

func (config) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "AWSAccessKeyID":
		return "AWS credential used to connect to S3."
	case "AWSSecretAccessKey":
		return "AWS credential used to connect to S3."
	case "Bucket":
		return "Name of the S3 bucket."
	case "Region":
		return "The name of the AWS region where the S3 bucket is located."
	case "UploadIntervalInSeconds":
		return "Time interval, in seconds, at which to upload data from Flow to S3."
	case "Advanced":
		return "Options for advanced users. You should not typically need to modify these."
	default:
		return ""
	}
}

func (advancedConfig) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Endpoint":
		return "The endpoint URI to connect to. Useful if you're connecting to a S3-compatible API that isn't provided by AWS."
	default:
		return ""
	}
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("missing region")
	}
	if c.Bucket == "" {
		return fmt.Errorf("missing bucket")
	}
	if c.AWSAccessKeyID == "" && c.AWSSecretAccessKey != "" {
		return fmt.Errorf("missing awsAccessKeyID")
	}
	if c.AWSAccessKeyID != "" && c.AWSSecretAccessKey == "" {
		return fmt.Errorf("missing awsSecretAccessKey")
	}
	if c.UploadIntervalInSeconds < 0 {
		return fmt.Errorf("UploadIntervalInSeconds should be non-negative")
	}

	return nil
}

// resource specifies a materialization destinaion in S3, and the resulting parquet file configuration.
type resource struct {
	PathPrefix string `json:"pathPrefix" jsonschema_extras:"x-collection-name=true"`
	// The method used for compressing data in parquet.
	CompressionType string `json:"compressionType,omitempty"`
}

var compressionTypeToCodec = map[string]parquet.CompressionCodec{
	"none":   parquet.CompressionCodec_UNCOMPRESSED,
	"snappy": parquet.CompressionCodec_SNAPPY,
	"gzip":   parquet.CompressionCodec_GZIP,
	"lz4":    parquet.CompressionCodec_LZ4,
	"zstd":   parquet.CompressionCodec_ZSTD,
}

func (r resource) Validate() error {
	if r.PathPrefix == "" {
		return fmt.Errorf("pathPrefix in a resource should not be None")
	}

	if r.CompressionType != "" {
		if _, ok := compressionTypeToCodec[r.CompressionType]; !ok {
			return fmt.Errorf("invalid compressionType, expecting one of %v", reflect.ValueOf(compressionTypeToCodec).MapKeys())
		}
	}

	return nil
}

func (r resource) CompressionCodec() parquet.CompressionCodec {
	if r.CompressionType == "" {
		return parquet.CompressionCodec_SNAPPY
	}

	return compressionTypeToCodec[r.CompressionType]
}

// Creates a driver checkpoint, and encodes it into a json.RawMessage to populate the DriverCheckPointJson field in `prepared` response.
func marshalDriverCheckpointJSON(flowCheckpoint []byte, nextSeqNumList []int) (json.RawMessage, error) {
	if len(flowCheckpoint) == 0 {
		panic("empty checkpoint received")
	}

	dcp := &checkpoint.DriverCheckpoint{
		B64EncodedFlowCheckpoint: base64.StdEncoding.EncodeToString(flowCheckpoint),
		NextSeqNumList:           nextSeqNumList,
	}

	md, err := json.Marshal(dcp)
	if err != nil {
		return nil, err
	}

	return md, nil
}

// Decodes a DriverCheckpointJson received from the Open txn request.
func unmarshalDriverCheckpointJSON(raw json.RawMessage) (flowCheckpoint []byte, nextSeqNumList []int, err error) {
	if len(raw) == 0 {
		return
	}

	var parsed checkpoint.DriverCheckpoint
	if err = pf.UnmarshalStrict(raw, &parsed); err != nil {
		return
	}

	nextSeqNumList = parsed.NextSeqNumList
	flowCheckpoint, err = base64.StdEncoding.DecodeString(parsed.B64EncodedFlowCheckpoint)
	return
}

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	endpointSchema, err := schemagen.GenerateSchema("S3 Connection", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("S3 Prefix", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/materialize-s3-parquet",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var out []*pm.ValidateResponse_Binding
	for _, binding := range req.Bindings {

		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		var constraints = make(map[string]*pm.Constraint)
		for _, projection := range binding.Collection.Projections {
			var isComplexField = false
			for _, ty := range projection.Inference.Types {
				if ty == pf.JsonTypeArray || ty == pf.JsonTypeObject {
					isComplexField = true
					break
				}
			}

			var constraint = &pm.Constraint{}
			switch {
			case projection.IsRootDocumentProjection():
				// TODO(jixiang) update MaterializationSpec_Binding.Validate and remove the required document field here.
				constraint.Type = pm.Constraint_FIELD_REQUIRED
				constraint.Reason = "The root document is needed."
			case isComplexField:
				// TODO(jixiang): support Array and Object fields.
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Array and Object fields are not supported."
			case projection.Inference.IsSingleType():
				constraint.Type = pm.Constraint_FIELD_REQUIRED
				constraint.Reason = "The projection has a single scalar type."
			default:
				// Fields with multiple types e.g. ["int", "string"] are forbidden.
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Cannot materialize this field."
			}
			constraints[projection.Field] = constraint
		}
		out = append(out, &pm.ValidateResponse_Binding{
			Constraints: constraints,
			// Only delta updates are supported by file materializations.
			DeltaUpdates: true,
			ResourcePath: []string{cfg.Bucket, res.PathPrefix},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

// ApplyUpsert is a no-op.
func (driver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

// ApplyDelete is a no-op.
// TODO(johnny): Should this remove data ?
func (driver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	return pm.RunTransactions(stream, func(ctx context.Context, open pm.TransactionRequest_Open) (pm.Transactor, *pm.TransactionResponse_Opened, error) {
		var cfg config
		if err := pf.UnmarshalStrict(open.Materialization.EndpointSpecJson, &cfg); err != nil {
			return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
		}

		runtimeCheckpoint, nextSeqNumList, err := unmarshalDriverCheckpointJSON(open.DriverCheckpointJson)
		if err != nil {
			// TODO(jixiang): How to resume the flow if there is a corrupt checkpoint? Always reset in code, or after a manual evaluation?
			return nil, nil, fmt.Errorf("parsing CheckpointJson: %w", err)
		}

		s3Uploader, err := NewS3Uploader(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("creating s3 uploader: %w", err)
		}

		var fileProcessor FileProcessor
		fileProcessor, err = NewParquetFileProcessor(ctx, s3Uploader, nextSeqNumList, &open)
		if err != nil {
			return nil, nil, fmt.Errorf("creating parquet file processor: %w", err)
		}

		var clock = clock.New()

		var fileProcessorProxy = NewFileProcessorProxy(
			ctx,
			fileProcessor,
			// The time interval for the proxy to trigger an upload-to-cloud action is set to be twice as long as the interval of the driver.
			// To make sure - as long as the files are uploaded on a reasonable schedule from the driver, no upload is triggered by the proxy.
			time.Duration(cfg.UploadIntervalInSeconds*2)*time.Second,
			clock,
		)

		var transactor = &transactor{
			clock:             clock,
			fileProcessor:     fileProcessorProxy,
			runtimeCheckpoint: runtimeCheckpoint,
			uploadInterval:    time.Duration(cfg.UploadIntervalInSeconds) * time.Second,
			lastUploadTime:    clock.Now(),
		}

		return transactor, &pm.TransactionResponse_Opened{RuntimeCheckpoint: runtimeCheckpoint}, nil
	})
}

// transactor implements the Transactor interface.
type transactor struct {
	clock                clock.Clock
	fileProcessor        FileProcessor
	driverCheckpointJSON json.RawMessage
	runtimeCheckpoint    []byte
	uploadInterval       time.Duration
	lastUploadTime       time.Time
}

func (t *transactor) Load(it *pm.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("Load should never be called for materialize-s3-parquet.Driver")
	}
	return nil
}

func (t *transactor) Store(it *pm.StoreIterator) (pm.StartCommitFunc, error) {
	for it.Next() {
		if err := t.fileProcessor.Store(it.Binding, it.Key, it.Values); err != nil {
			return nil, err
		}
	}

	return func(ctx context.Context, runtimeCheckpoint []byte, runtimeAckCh <-chan struct{}) (*pf.DriverCheckpoint, pf.OpFuture) {
		// Retain runtime checkpoint for it to be potentially encoded in a
		// driverCheckpointJSON produced as part of a future call to maybeUpload().
		t.runtimeCheckpoint = runtimeCheckpoint

		// We don't await `runtimeAckCh` because it's not required for correctness.
		// If this file is uploaded but the recovery log commit fails, we'll resume
		// with a previous driver checkpoint which will re-produce and upload this
		// same file.
		var err = t.maybeUpload()
		return &pf.DriverCheckpoint{DriverCheckpointJson: t.driverCheckpointJSON},
			pf.FinishedOperation(err)
	}, nil
}

func (t *transactor) maybeUpload() error {
	var now = t.clock.Now()
	if now.Sub(t.lastUploadTime) >= t.uploadInterval {
		// Uploads the local file to cloud.
		if nextSeqNumList, err := t.fileProcessor.Commit(); err != nil {
			return fmt.Errorf("uploading to cloud: %w,", err)
		} else if t.driverCheckpointJSON, err = marshalDriverCheckpointJSON(
			t.runtimeCheckpoint, nextSeqNumList,
		); err != nil {
			return fmt.Errorf("encoding driverCheckpointJson: %w", err)
		}

		t.lastUploadTime = now
	}

	return nil
}

func (t *transactor) Acknowledge(context.Context) error {
	return nil
}

func (t *transactor) Destroy() {
	t.fileProcessor.Destroy()
}

func main() { boilerplate.RunMain(new(driver)) }
