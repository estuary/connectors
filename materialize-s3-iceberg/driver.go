package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	"github.com/estuary/connectors/filesink"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	iso8601 "github.com/senseyeio/duration"
	log "github.com/sirupsen/logrus"
)

// There is an equivalent pydantic model in iceberg-ctl, and the config schema is generated from
// that. The fields of this struct must be compatible with that model.
type config struct {
	Bucket             string `json:"bucket"`
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	Namespace          string `json:"namespace"`
	Region             string `json:"region"`
	UploadInterval     string `json:"upload_interval"`
	Prefix             string `json:"prefix,omitempty"`
}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"bucket", c.Bucket},
		{"aws_access_key_id", c.AWSAccessKeyID},
		{"aws_secret_access_key", c.AWSSecretAccessKey},
		{"namespace", c.Namespace},
		{"region", c.Region},
		{"upload_interval", c.UploadInterval},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if strings.Contains(c.Namespace, ".") {
		return fmt.Errorf("namespace %q must not contain dots", c.Namespace)
	} else if _, err := parse8601(c.UploadInterval); err != nil {
		return err
	}

	if c.Prefix != "" {
		if strings.HasPrefix(c.Prefix, "/") {
			return fmt.Errorf("prefix %q cannot start with /", c.Prefix)
		}
	}

	return nil
}

func parse8601(in string) (time.Duration, error) {
	parsed, err := iso8601.ParseISO8601(in)
	if err != nil {
		return 0, err
	}

	var dur time.Duration
	dur += time.Duration(parsed.TH * int(time.Hour))
	dur += time.Duration(parsed.TM * int(time.Minute))
	dur += time.Duration(parsed.TS * int(time.Second))

	if dur > 4*time.Hour || parsed.Y != 0 || parsed.M != 0 || parsed.W != 0 || parsed.D != 0 {
		return 0, fmt.Errorf("upload_interval %q is invalid: must be no greater than 4 hours", in)
	}

	return dur, nil
}

type resource struct {
	Table     string `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Namespace string `json:"namespace,omitempty" jsonschema:"title=Alternative Namespace,description=Alternative namespace for this table (optional)."`
	Delta     bool   `json:"delta_updates,omitempty" jsonschema:"default=true,title=Delta Update,description=Should updates to this table be done via delta updates. Currently this connector only supports delta updates."`
}

func newResource(cfg config) resource {
	return resource{
		// Default to the endpoint Namespace. This will be over-written by a present `namespace`
		// property within `raw` when unmarshalling into the returned resource.
		Namespace: cfg.Namespace,
	}
}

func (r resource) path() []string {
	return []string{r.Namespace, r.Table}
}

func pathToFQN(p []string) string {
	return strings.Join(p, ".")
}

func (r resource) Validate() error {
	var requiredProperties = [][]string{
		{"table", r.Table},
		{"namespace", r.Namespace},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if strings.Contains(r.Table, ".") {
		return fmt.Errorf("table %q must not contain dots", r.Table)
	} else if strings.Contains(r.Namespace, ".") {
		return fmt.Errorf("namespace %q must not contain dots", r.Namespace)
	} else if !r.Delta {
		return fmt.Errorf("connector only supports delta update mode: delta update must be enabled")
	}

	return nil
}

type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := runIcebergctl(nil, "print-config-schema")
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("ResourceConfig", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-s3-iceberg",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	// Test creating, reading, and deleting an object from the configured bucket and prefix.
	errs := &sql.PrereqErr{}

	s3store, err := filesink.NewS3Store(ctx, filesink.S3StoreConfig{
		Bucket:             cfg.Bucket,
		AWSAccessKeyID:     cfg.AWSAccessKeyID,
		AWSSecretAccessKey: cfg.AWSSecretAccessKey,
		Region:             cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("creating s3 store: %w", err)
	}

	s3client := s3store.Client()

	testKey := path.Join(cfg.Prefix, uuid.NewString())

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
				err = fmt.Errorf("not authorized to write to %q", path.Join(cfg.Bucket, cfg.Prefix))
			}
		}
		errs.Err(err)
	} else if _, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to read from %q", path.Join(cfg.Bucket, cfg.Prefix))
		}
		errs.Err(err)
	} else if _, err := s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to delete from %q", path.Join(cfg.Bucket, cfg.Prefix))
		}
		errs.Err(err)
	}
	if errs.Len() != 0 {
		return nil, cerrors.NewUserError(nil, errs.Error())
	}

	catalog := newGlueCatalog(cfg, req.LastMaterialization)

	is, err := catalog.infoSchema()
	if err != nil {
		return nil, err
	}

	// AWS Glue prohibits field names longer than 255 characters, and considers "thisColumn" to be
	// in conflict with "ThisColumn" etc.
	validator := boilerplate.NewValidator(icebergConstrainter{}, is, 255, true)

	var out []*pm.Response_Validated_Binding
	for _, binding := range req.Bindings {
		res := newResource(cfg)
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		constraints, err := validator.ValidateBinding(
			res.path(),
			res.Delta,
			binding.Backfill,
			binding.Collection,
			binding.FieldConfigJsonMap,
			req.LastMaterialization,
		)
		if err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.Delta,
			ResourcePath: res.path(),
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	catalog := newGlueCatalog(cfg, req.LastMaterialization)

	existingNamespaces, err := catalog.listNamespaces()
	if err != nil {
		return nil, err
	}

	requiredNamespaces := make(map[string]struct{})
	for _, b := range req.Materialization.Bindings {
		requiredNamespaces[b.ResourcePath[0]] = struct{}{}
	}

	for r := range requiredNamespaces {
		if !slices.Contains(existingNamespaces, r) {
			if err := catalog.createNamespace(r); err != nil {
				return nil, fmt.Errorf("catalog creating namespace '%s': %w", r, err)
			}
			log.WithField("namespace", r).Info("created namespace")
		}
	}

	is, err := catalog.infoSchema()
	if err != nil {
		return nil, err
	}

	return boilerplate.ApplyChanges(ctx, req, catalog, is, false)
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (m.Transactor, *pm.Response_Opened, error) {
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, fmt.Errorf("unmarshalling endpoint config: %w", err)
	}

	var bindings []binding
	for _, b := range open.Materialization.Bindings {
		res := newResource(cfg)
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, fmt.Errorf("unmarshalling resource config: %w", err)
		}

		bindings = append(bindings, binding{
			path:       b.ResourcePath,
			pqSchema:   schemaWithOptions(b.FieldSelection.AllFields(), b.Collection),
			includeDoc: b.FieldSelection.Document != "",
			stateKey:   b.StateKey,
		})
	}

	s3store, err := filesink.NewS3Store(ctx, filesink.S3StoreConfig{
		Bucket:             cfg.Bucket,
		AWSAccessKeyID:     cfg.AWSAccessKeyID,
		AWSSecretAccessKey: cfg.AWSSecretAccessKey,
		Region:             cfg.Region,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("creating s3 store: %w", err)
	}

	interval, err := parse8601(cfg.UploadInterval)
	if err != nil {
		return nil, nil, err
	}

	return &transactor{
		catalog:        newGlueCatalog(cfg, open.Materialization),
		bindings:       bindings,
		bucket:         cfg.Bucket,
		prefix:         cfg.Prefix,
		store:          s3store,
		uploadInterval: interval,
	}, &pm.Response_Opened{}, nil
}

func main() { boilerplate.RunMain(new(driver)) }
