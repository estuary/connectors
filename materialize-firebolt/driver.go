package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

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

func main() { boilerplate.RunMain(new(driver)) }
