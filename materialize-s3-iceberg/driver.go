package main

import (
	"context"
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
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	iso8601 "github.com/senseyeio/duration"
)

var featureFlagDefaults = map[string]bool{}

type catalogType string

const (
	catalogTypeGlue catalogType = "AWS Glue"
	catalogTypeRest catalogType = "Iceberg REST Server"
)

// There is an equivalent pydantic model in iceberg-ctl, and the config schema is generated from
// that. The fields of this struct must be compatible with that model.
type config struct {
	Bucket             string         `json:"bucket"`
	AWSAccessKeyID     string         `json:"aws_access_key_id"`
	AWSSecretAccessKey string         `json:"aws_secret_access_key"`
	Namespace          string         `json:"namespace"`
	Region             string         `json:"region"`
	UploadInterval     string         `json:"upload_interval"`
	Prefix             string         `json:"prefix,omitempty"`
	Catalog            catalogConfig  `json:"catalog"`
	Advanced           advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extra:"advanced=true"`
}

type catalogConfig struct {
	CatalogType catalogType `json:"catalog_type"`

	// Rest catalog configuration.
	URI        string `json:"uri,omitempty"`
	Credential string `json:"credential,omitempty"`
	Token      string `json:"token,omitempty"`
	Warehouse  string `json:"warehouse,omitempty"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
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

	if c.Catalog.CatalogType == "" {
		return fmt.Errorf("missing 'catalog_type'")
	}

	if c.Catalog.CatalogType == catalogTypeRest {
		var requiredProperties = [][]string{
			{"uri", c.Catalog.URI},
			{"warehouse", c.Catalog.Warehouse},
		}
		for _, req := range requiredProperties {
			if req[1] == "" {
				return fmt.Errorf("REST catalog config missing '%s'", req[0])
			}
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

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
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
	Delta     *bool  `json:"delta_updates,omitempty" jsonschema:"default=true,title=Delta Update,description=Should updates to this table be done via delta updates. Currently this connector only supports delta updates."`
}

func pathToFQN(p []string) string {
	return strings.Join(p, ".")
}

func (r resource) Validate() error {
	var requiredProperties = [][]string{
		{"table", r.Table},
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
	} else if r.Delta != nil && !*r.Delta {
		return fmt.Errorf("connector only supports delta update mode: delta update must be enabled")
	}

	return nil
}

func (r resource) WithDefaults(cfg config) resource {
	if r.Namespace == "" {
		r.Namespace = cfg.Namespace
	}
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	return []string{strings.ToLower(r.Namespace), strings.ToLower(r.Table)}, true, nil
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := runIcebergctl(ctx, nil, "print-config-schema")
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("ResourceConfig", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-s3-iceberg", endpointSchema, resourceSchema)
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (d driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg     config
	catalog *catalog
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	catalog := newCatalog(cfg)

	return &materialization{
		cfg:     cfg,
		catalog: catalog,
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	interval, err := parse8601(d.cfg.UploadInterval)
	if err != nil {
		interval = time.Hour
	}

	return boilerplate.MaterializeCfg{
		MaxFieldLength:        255,
		CaseInsensitiveFields: true,
		ConcurrentApply:       true,
		MaterializeOptions: boilerplate.MaterializeOptions{
			AckSchedule: &boilerplate.AckScheduleOption{
				Config: boilerplate.ScheduleConfig{
					SyncFrequency: interval.String(),
				},
			},
			ExtendedLogging: true,
		},
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	ns, err := d.catalog.listNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("listing namespaces: %w", err)
	}
	for _, namespace := range ns {
		is.PushNamespace(namespace)
	}

	return d.catalog.populateInfoSchema(ctx, is, resourcePaths)
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	s3store, err := filesink.NewS3Store(ctx, filesink.S3StoreConfig{
		Bucket:             d.cfg.Bucket,
		AWSAccessKeyID:     d.cfg.AWSAccessKeyID,
		AWSSecretAccessKey: d.cfg.AWSSecretAccessKey,
		Region:             d.cfg.Region,
	})
	if err != nil {
		errs.Err(fmt.Errorf("creating s3 store: %w", err))
		return errs
	}

	s3client := s3store.Client()
	testKey := path.Join(d.cfg.Prefix, uuid.NewString())

	var awsErr *awsHttp.ResponseError
	if _, err := s3client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("testing"),
	}); err != nil {
		if errors.As(err, &awsErr) {
			if awsErr.Response.Response.StatusCode == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", d.cfg.Bucket)
			} else if awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to %q", path.Join(d.cfg.Bucket, d.cfg.Prefix))
			}
		}
		errs.Err(err)
	} else if _, err := s3client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to read from %q", path.Join(d.cfg.Bucket, d.cfg.Prefix))
		}
		errs.Err(err)
	} else if _, err := s3client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(d.cfg.Bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to delete from %q", path.Join(d.cfg.Bucket, d.cfg.Prefix))
		}
		errs.Err(err)
	}

	return errs
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	_, isNumeric := boilerplate.AsFormattedNumeric(&p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	s, err := projectionToParquetSchemaElement(p.Projection, fc)
	if err != nil {
		return mappedType{}, nil
	}

	return mappedType{icebergType: parquetTypeToIcebergType(s.DataType)}, nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return fmt.Sprintf("create namespace %q", ns), d.catalog.createNamespace(ctx, ns)
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	return d.catalog.CreateResource(ctx, &res.MaterializationSpec_Binding)
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	return d.catalog.DeleteResource(ctx, resourcePath)
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	return d.catalog.UpdateResource(ctx, update)
}

func (d *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	var resourcePaths [][]string
	var bindings []binding

	for _, b := range mappedBindings {
		pqSchema, err := parquetSchema(b.FieldSelection.AllFields(), b.Collection, b.FieldSelection.FieldConfigJsonMap)
		if err != nil {
			return nil, err
		}

		resourcePaths = append(resourcePaths, b.ResourcePath)
		bindings = append(bindings, binding{
			path:       b.ResourcePath,
			pqSchema:   pqSchema,
			includeDoc: b.FieldSelection.Document != "",
			stateKey:   b.StateKey,
		})
	}

	tablePaths, err := d.catalog.tablePaths(ctx, resourcePaths)
	if err != nil {
		return nil, fmt.Errorf("looking up table paths: %w", err)
	}

	for idx := range bindings {
		bindings[idx].catalogTablePath = tablePaths[idx]
	}

	s3store, err := filesink.NewS3Store(ctx, filesink.S3StoreConfig{
		Bucket:             d.cfg.Bucket,
		AWSAccessKeyID:     d.cfg.AWSAccessKeyID,
		AWSSecretAccessKey: d.cfg.AWSSecretAccessKey,
		Region:             d.cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("creating s3 store: %w", err)
	}

	return &transactor{
		materialization: req.Materialization.Name.String(),
		catalog:         d.catalog,
		bindings:        bindings,
		bucket:          d.cfg.Bucket,
		prefix:          d.cfg.Prefix,
		store:           s3store,
	}, nil
}

func (d *materialization) Close(ctx context.Context) {}

func main() { boilerplate.RunMain(driver{}) }
