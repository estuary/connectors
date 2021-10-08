package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/alecthomas/jsonschema"
	"github.com/benbjohnson/clock"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go/parquet"
)

type config struct {
	AWSAccessKeyID     string `json:"awsAccessKeyId"`
	AWSSecretAccessKey string `json:"awsSecretAccessKey"`
	Endpoint           string `json:"endpoint"`
	Region             string `json:"region"`
	// The driver batches materialization results to local files first,
	// and uploads the local files to cloud (S3) on a schedule specified by
	// UploadIntervalInSeconds, which is the mimimal wait time (in seconds) between two
	// consecutive upload-to-cloud actions.
	UploadIntervalInSeconds int `json:"uploadIntervalInSeconds"`
}

// Validate returns an error if the config is not well-formed.
func (c config) Validate() error {
	if c.Region == "" && c.Endpoint == "" {
		return fmt.Errorf("must supply one of 'region' or 'endpoint'")
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
	Bucket     string `json:"bucket"`
	PathPrefix string `json:"pathPrefix"`
	// The number of parallel go-routines spawned when writing to parquet files.
	ParallelNumber int `json:"parallelNumber"`
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
	if r.Bucket == "" {
		return fmt.Errorf("bucket in a resource should not be None")
	}
	if r.PathPrefix == "" {
		return fmt.Errorf("pathPrefix in a resource should not be None")
	}

	if r.CompressionType != "" {
		if _, ok := compressionTypeToCodec[r.CompressionType]; !ok {
			return fmt.Errorf("invalid compressionType, expecting one of %v", reflect.ValueOf(compressionTypeToCodec).MapKeys())
		}
	}

	if r.ParallelNumber <= 0 {
		return fmt.Errorf("parallelNumber must be positive")
	}

	return nil
}

func (r resource) CompressionCodec() parquet.CompressionCodec {
	if r.CompressionType == "" {
		return parquet.CompressionCodec_SNAPPY
	}

	return compressionTypeToCodec[r.CompressionType]
}

// The structure of a driver checkpoint.
type driverCheckpoint struct {
	// The flow checkpoint (base64-encoded), which marks the txn that has been successfully
	// materialized and stored in the cloud. If the materialization process is stopped
	// for any reason, this is the checkpoint to resume from.
	B64EncodedFlowCheckpoint string `json:"b64EncodedFlowCheckpoint"`
	// The sequence number used to name the next files to be uploaded to the cloud.
	// To be specific, the next parquet file from the i-th binding is named using the deterministic pattern of
	// "<KeyBegin>_<KeyEnd>_<NextSeqNumList[i]>.pq".
	// The NextSeqNumList[i] is increased by 1 after each successful upload from the i-th binding.
	NextSeqNumList []int `json:"nextSeqNumList"`
}

func (dcp driverCheckpoint) Validate() error { return nil }

// Creates a driver checkpoint, and encodes it into a json.RawMessage to populate the DriverCheckPointJson field in `prepared` response.
func marshalDriverCheckpointJSON(flowCheckpoint []byte, nextSeqNumList []int) (json.RawMessage, error) {
	if flowCheckpoint != nil && len(flowCheckpoint) == 0 {
		panic("empty checkpoint received.")
	}

	dcp := &driverCheckpoint{
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
	flowCheckpoint, nextSeqNumList, err = nil, nil, nil

	if raw == nil {
		return
	}

	var parsed driverCheckpoint
	if err = pf.UnmarshalStrict(raw, &parsed); err != nil {
		return
	}

	nextSeqNumList = parsed.NextSeqNumList
	if flowCheckpoint, err = base64.StdEncoding.DecodeString(parsed.B64EncodedFlowCheckpoint); err != nil {
		return
	} else if flowCheckpoint != nil && len(flowCheckpoint) == 0 {
		flowCheckpoint = nil
	}

	return
}

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	endpointSchema, err := jsonschema.Reflect(new(config)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := jsonschema.Reflect(new(resource)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev#FIXME",
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

			var constraint = new(pm.Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				// Document field is not required by this driver.
				// However MaterializationSpec_Binding.Validate requires the document field to be present.
				constraint.Type = pm.Constraint_FIELD_REQUIRED
				constraint.Reason = "The root document is needed."
			case isComplexField:
				// TODO(jixiang): support complex fields.
				constraint.Type = pm.Constraint_FIELD_FORBIDDEN
				constraint.Reason = "Complex fields are not supported."
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
			ResourcePath: []string{res.Bucket, res.PathPrefix},
		})
	}

	return &pm.ValidateResponse{Bindings: out}, nil
}

// Apply is a no-op.
func (driver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	return &pm.ApplyResponse{}, nil
}

func (driver) Transactions(stream pm.Driver_TransactionsServer) error {
	open, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	var cfg config
	if err = pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, &cfg); err != nil {
		return fmt.Errorf("parsing endpoint config: %w", err)
	}

	flowCheckpoint, nextSeqNumList, err := unmarshalDriverCheckpointJSON(open.Open.DriverCheckpointJson)
	if err != nil {
		// TODO(jixiang): How to resume the flow if there is a corrupt checkpoint? Always reset in code, or after a manual evaluation?
		return fmt.Errorf("parsing CheckpointJson: %w", err)
	}

	var ctx = stream.Context()

	s3Uploader, err := NewS3Uploader(cfg)
	if err != nil {
		return fmt.Errorf("creating s3 uploader: %w", err)
	}

	var fileProcessor FileProcessor
	fileProcessor, err = NewParquetFileProcessor(ctx, s3Uploader, nextSeqNumList, open.Open)
	if err != nil {
		return fmt.Errorf("creating parquet file processor: %w", err)
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
		ctx:                  ctx,
		clock:                clock,
		fileProcessor:        fileProcessorProxy,
		driverCheckpointJSON: open.Open.DriverCheckpointJson,
		flowCheckpoint:       flowCheckpoint,
		uploadInterval:       time.Duration(cfg.UploadIntervalInSeconds) * time.Second,
		lastUploadTime:       clock.Now(),
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: flowCheckpoint},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	var log = log.WithField("materialization", "s3parquet")
	return pm.RunTransactions(stream, transactor, log)
}

// transactor implements the Transactor interface.
type transactor struct {
	ctx                  context.Context
	clock                clock.Clock
	fileProcessor        FileProcessor
	driverCheckpointJSON json.RawMessage
	flowCheckpoint       []byte
	uploadInterval       time.Duration
	lastUploadTime       time.Time
}

func (t *transactor) Load(_ *pm.LoadIterator, _ <-chan struct{}, _ func(int, json.RawMessage) error) error {
	panic("Load should never be called for materialize-s3-parquet.Driver")
}

func (t *transactor) Prepare(req *pm.TransactionRequest_Prepare) (*pm.TransactionResponse_Prepared, error) {
	t.flowCheckpoint = req.FlowCheckpoint
	return &pm.TransactionResponse_Prepared{DriverCheckpointJson: t.driverCheckpointJSON}, nil
}

func (t *transactor) Store(it *pm.StoreIterator) error {
	for it.Next() {
		if err := t.fileProcessor.Store(it.Binding, it.Key, it.Values); err != nil {
			return err
		}
	}
	return nil
}

func (t *transactor) Commit() error {
	var now = t.clock.Now()
	if now.Sub(t.lastUploadTime) >= t.uploadInterval {
		// Uploads the local file to cloud.
		if nextSeqNumList, err := t.fileProcessor.Commit(); err != nil {
			return fmt.Errorf("uploading to cloud: %w,", err)
		} else if t.driverCheckpointJSON, err = marshalDriverCheckpointJSON(
			t.flowCheckpoint, nextSeqNumList,
		); err != nil {
			return fmt.Errorf("encoding driverCheckpointJson: %w", err)
		}

		t.lastUploadTime = now
	}

	return nil
}

func (t *transactor) Destroy() {
	t.fileProcessor.Destroy()
}

func main() { boilerplate.RunMain(new(driver)) }
