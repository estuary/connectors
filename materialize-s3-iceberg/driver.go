package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	"github.com/estuary/connectors/filesink"
	"github.com/estuary/connectors/go/blob"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/estuary/connectors/go/writer"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/invopop/jsonschema"
	iso8601 "github.com/senseyeio/duration"
	log "github.com/sirupsen/logrus"
)

var featureFlagDefaults = map[string]bool{
	"retain_existing_data_on_backfill": false,

	// An alternate naming style for the Iceberg table data.  By default (when
	// false), the naming is:
	//   <base_location>/<namespace>/<table>_<hash>
	// When this flag is enabled:
	//   <base_location>/<namespace>/<table>.<hash>
	"nested_dot_hash_location_style": false,
}

type catalogType string

const (
	catalogTypeGlue catalogType = "AWS Glue"
	catalogTypeRest catalogType = "Iceberg REST Server"
)

// Field ordering preserves the original schema layout
// (legacy aws_* keys first, bucket/prefix/region/namespace next) so existing
// users do not see fields rearranged on next view.
//
// TODO: AWSAccessKeyID, AWSSecretAccessKey, Advanced, and
// advancedConfig.FeatureFlags are pointers so the generated schema emits a
// null branch (oneOf: [original, {type: null}]) for backwards-compatibility
// with any prod config that stores literal null in these fields. Convert back
// to plain value types + `omitempty` (the convention used by
// materialize-bigquery / materialize-snowflake / etc.) once we are confident
// no live config depends on the union-with-null shape, and remove the deref /
// nil-check sites in Validate / s3StoreConfig / FeatureFlags / catalog.go.
type config struct {
	AWSAccessKeyID     *string                     `json:"aws_access_key_id,omitempty" jsonschema:"title=AWS Access Key ID,description=Access Key ID for accessing AWS services (legacy).,nullable" jsonschema_extras:"order=0,x-hidden-field=true"`
	AWSSecretAccessKey *string                     `json:"aws_secret_access_key,omitempty" jsonschema:"title=AWS Secret Access Key,description=Secret Access Key for accessing AWS services (legacy).,nullable" jsonschema_extras:"order=1,secret=true,x-hidden-field=true"`
	Credentials        *filesink.CredentialsConfig `json:"credentials" jsonschema:"title=Authentication" jsonschema_extras:"order=2,x-iam-auth=true"`
	Bucket             string                      `json:"bucket" jsonschema:"title=Bucket,description=The S3 bucket to write data files to." jsonschema_extras:"order=3"`
	Prefix             string                      `json:"prefix,omitempty" jsonschema:"title=Prefix,description=Optional prefix that will be used to store objects." jsonschema_extras:"order=4"`
	Region             string                      `json:"region" jsonschema:"title=Region,description=AWS region." jsonschema_extras:"order=5"`
	Namespace          string                      `json:"namespace" jsonschema:"title=Namespace,description=Namespace for bound collection tables (unless overridden within the binding resource configuration)." jsonschema_extras:"order=6,pattern=^[^.]*$"`
	UploadInterval     string                      `json:"upload_interval,omitempty" jsonschema:"title=Upload Interval,description=Frequency at which files will be uploaded. Must be a valid ISO8601 duration string no greater than 4 hours.,default=PT5M,format=duration" jsonschema_extras:"order=7"`
	S3Endpoint         string                      `json:"s3_endpoint,omitempty" jsonschema:"title=S3 Endpoint,description=Custom S3 endpoint URL. The default AWS S3 endpoint for the specified region is used if not provided." jsonschema_extras:"order=8"`
	Catalog            catalogConfig               `json:"catalog" jsonschema:"title=Catalog" jsonschema_extras:"order=9"`
	Advanced           *advancedConfig             `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these.,nullable" jsonschema_extras:"advanced=true,order=10"`
}

// strVal dereferences an optional string config field, returning "" when nil.
func strVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

type catalogConfig struct {
	CatalogType catalogType `json:"catalog_type"`

	// Glue catalog configuration.
	GlueID string `json:"glue_id,omitempty"`

	// Rest catalog configuration.
	URI        string `json:"uri,omitempty"`
	Credential string `json:"credential,omitempty"`
	Token      string `json:"token,omitempty"`
	Warehouse  string `json:"warehouse,omitempty"`
	Scope      string `json:"scope,omitempty"`
}

// restCatalogProps and glueCatalogProps drive ONLY schema generation via
// catalogConfig.JSONSchema. JSON-decoded REST/Glue config values land in
// catalogConfig (above), so the optional *string fields here only affect the
// emitted JSON Schema (they preserve the prior ["string","null"] type union).
// See the TODO on `config` above.
type restCatalogProps struct {
	URI        string  `json:"uri" jsonschema:"title=URI,description=URI identifying the REST catalog. Format: 'https://yourserver.com/catalog'." jsonschema_extras:"order=1"`
	Credential *string `json:"credential,omitempty" jsonschema:"title=Credential,description=Credential for connecting to the catalog.,nullable" jsonschema_extras:"order=2,secret=true"`
	Token      *string `json:"token,omitempty" jsonschema:"title=Token,description=Token for connecting to the catalog.,nullable" jsonschema_extras:"order=3,secret=true"`
	Warehouse  string  `json:"warehouse" jsonschema:"title=Warehouse,description=Warehouse to connect to." jsonschema_extras:"order=4"`
	Scope      *string `json:"scope,omitempty" jsonschema:"title=Scope,description=Desired scope of the requested security token.,nullable" jsonschema_extras:"order=5"`
}

type glueCatalogProps struct {
	GlueID *string `json:"glue_id,omitempty" jsonschema:"title=Glue Catalog ID,description=Glue Catalog ID to use. Defaults to the account ID of the configured credentials when not set.,nullable" jsonschema_extras:"order=1"`
}

// JSONSchema returns the catalog discriminator schema. Struct tags on
// catalogConfig itself are NOT consulted — the schema is built entirely from
// restCatalogProps and glueCatalogProps via OneOfSchema.
func (catalogConfig) JSONSchema() *jsonschema.Schema {
	return schemagen.OneOfSchema("Catalog", "", "catalog_type", string(catalogTypeRest),
		schemagen.OneOfSubSchema("REST", restCatalogProps{}, string(catalogTypeRest)),
		schemagen.OneOfSubSchema("AWS Glue", glueCatalogProps{}, string(catalogTypeGlue)),
	)
}

type advancedConfig struct {
	FeatureFlags *string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support.,nullable"`
}

func (c config) s3StoreConfig() filesink.S3StoreConfig {
	cfg := filesink.S3StoreConfig{
		Bucket:   c.Bucket,
		Region:   c.Region,
		Endpoint: c.S3Endpoint,
		// Custom S3 endpoints (R2, S3-compatible) typically require
		// path-style addressing; AWS S3 still accepts it.
		UsePathStyle: c.S3Endpoint != "",
	}

	if c.Credentials != nil {
		cfg.Credentials = c.Credentials
	} else {
		cfg.AWSAccessKeyID = strVal(c.AWSAccessKeyID)
		cfg.AWSSecretAccessKey = strVal(c.AWSSecretAccessKey)
	}

	return cfg
}

// normalizeCredentials folds legacy top-level aws_access_key_id/
// aws_secret_access_key into cfg.Credentials as an AWSAccessKey credential,
// mirroring the removed Python `transform_legacy_credentials`. Downstream code
// then reads only cfg.Credentials, so the REST and Glue catalog paths treat
// legacy and modern configs identically. When both are supplied, the explicit
// credentials win and the legacy keys are ignored.
func (c *config) normalizeCredentials() {
	legacyAccessKey := strVal(c.AWSAccessKeyID)
	legacySecretKey := strVal(c.AWSSecretAccessKey)
	c.AWSAccessKeyID = nil
	c.AWSSecretAccessKey = nil

	if c.Credentials != nil {
		if legacyAccessKey != "" || legacySecretKey != "" {
			log.Warn("both legacy aws_access_key_id/aws_secret_access_key and credentials are set; the former will be ignored in favour of the latter")
		}
		return
	}

	if legacyAccessKey == "" && legacySecretKey == "" {
		return
	}

	c.Credentials = &filesink.CredentialsConfig{
		AuthType: filesink.AWSAccessKey,
		AccessKeyCredentials: filesink.AccessKeyCredentials{
			AWSAccessKeyID:     legacyAccessKey,
			AWSSecretAccessKey: legacySecretKey,
		},
	}
}

func (c config) Validate() error {
	requiredProperties := [][]string{
		{"bucket", c.Bucket},
		{"namespace", c.Namespace},
		{"region", c.Region},
		{"upload_interval", c.UploadInterval},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	legacyAccessKey := strVal(c.AWSAccessKeyID)
	legacySecretKey := strVal(c.AWSSecretAccessKey)

	if c.Credentials != nil {
		if err := c.Credentials.Validate(); err != nil {
			return err
		}
	} else if legacyAccessKey != "" || legacySecretKey != "" {
		if legacyAccessKey == "" {
			return fmt.Errorf("missing 'aws_access_key_id'")
		}
		if legacySecretKey == "" {
			return fmt.Errorf("missing 'aws_secret_access_key'")
		}
	} else {
		return fmt.Errorf("must provide credentials")
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

	if err := blob.ValidateBucketPath(c.Prefix); err != nil {
		return fmt.Errorf("prefix %w", err)
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return ""
}

func (c config) FeatureFlags() (string, map[string]bool) {
	if c.Advanced == nil {
		return "", featureFlagDefaults
	}
	return strVal(c.Advanced.FeatureFlags), featureFlagDefaults
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

// partitionField is one entry in a table's Iceberg partition spec.
type partitionField struct {
	Field string `json:"field" jsonschema:"title=Field,description=Name of a materialized (selected) field to partition by."`
	// Transform is an Iceberg partition transform string as accepted by
	// iceberg.ParseTransform: identity, bucket[N], truncate[W], year, month,
	// day, hour, or void.
	Transform string `json:"transform" jsonschema:"title=Transform,description=Iceberg partition transform to apply to the field. One of: identity; bucket[N]; truncate[W]; year; month; day; hour; or void.,default=identity"`
}

type resource struct {
	Table                     string            `json:"table" jsonschema:"title=Table,description=Name of the database table." jsonschema_extras:"x-collection-name=true"`
	Namespace                 string            `json:"namespace,omitempty" jsonschema:"title=Alternative Namespace,description=Alternative namespace for this table (optional)."`
	Delta                     *bool             `json:"delta_updates,omitempty" jsonschema:"default=true,title=Delta Update,description=Should updates to this table be done via delta updates. Currently this connector only supports delta updates."`
	AdditionalTableProperties map[string]string `json:"additional_table_properties,omitempty" jsonschema:"title=Additional Table Properties,description=Additional Iceberg table properties to set when the table is created. These are set only at creation time and cannot be changed afterwards. Example: {'write.parquet.compression-codec': 'zstd'}"`
	PartitionFields           []partitionField  `json:"partition_fields,omitempty" jsonschema:"title=Partition Fields,description=Iceberg partition spec for this table. Set only at table creation time; changing it on an existing table requires recreating the table."`
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

	// Whether a partition field references a selected field and whether its
	// transform applies to that field's type can only be checked once the
	// resolved schema is available; those checks live in buildPartitionSpec.
	seen := make(map[partitionField]struct{}, len(r.PartitionFields))
	for _, pf := range r.PartitionFields {
		if pf.Field == "" {
			return fmt.Errorf("partition field is missing 'field'")
		}
		transform, err := iceberg.ParseTransform(pf.Transform)
		if err != nil {
			return fmt.Errorf("partition field %q: %w", pf.Field, err)
		}
		if err := validatePartitionTransform(transform); err != nil {
			return fmt.Errorf("partition field %q: %w", pf.Field, err)
		}
		if _, ok := seen[pf]; ok {
			return fmt.Errorf("duplicate partition field: field %q, transform %q", pf.Field, pf.Transform)
		}
		seen[pf] = struct{}{}
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
	endpointSchemaObj := schemagen.GenerateSchema("EndpointConfig", &config{})
	collapseNullableScalars(endpointSchemaObj)
	endpointSchema, err := endpointSchemaObj.MarshalJSON()
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

func (d driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg     config
	catalog *catalog
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	if strings.Contains(cfg.Catalog.URI, "r2.cloudflarestorage.com") {
		if !strings.HasPrefix(cfg.Prefix, "__r2_data_catalog/") {
			cfg.Prefix = "__r2_data_catalog/" + cfg.Prefix
		}
	}
	locationStyle := NestedLocationStyle
	if featureFlags["nested_dot_hash_location_style"] {
		locationStyle = NestedDotHashLocationStyle
	}

	catalog, err := newCatalog(ctx, cfg, locationStyle)
	if err != nil {
		return nil, fmt.Errorf("creating iceberg catalog: %w", err)
	}

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
		NoTruncateResources:   true,
		MaterializeOptions: m.MaterializeOptions{
			AckSchedule: &m.AckScheduleOption{
				Config: m.ScheduleConfig{
					SyncFrequency: interval.String(),
				},
			},
			ExtendedLogging: true,
		},
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	return d.catalog.populateInfoSchema(ctx, is, resourcePaths)
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	s3store, err := filesink.NewS3Store(ctx, d.cfg.s3StoreConfig())
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
	_, isNumeric := m.AsFormattedNumeric(&p)

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
		// The only error here is ignoreStringFormat being set on a non-string
		// field, where it is a no-op. Map by the field's native type rather
		// than returning a zero mappedType, whose nil iceberg.Type would panic
		// in String/Compatible. The misconfiguration still surfaces with a
		// clear error at table creation (parquetSchema).
		s, _ = projectionToParquetSchemaElement(p.Projection, fieldConfig{})
	}

	// Clamp date and timestamp values to years 1-9999 before they reach the
	// writer. See clampDate/clampTimestamp for the bound's origin.
	var elementConverter boilerplate.ElementConverter
	switch s.DataType {
	case writer.LogicalTypeTimestamp:
		elementConverter = clampTimestamp
	case writer.LogicalTypeDate:
		elementConverter = clampDate
	}

	return mappedType{icebergType: parquetTypeToIcebergType(s.DataType)}, elementConverter
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return fmt.Sprintf("create namespace %q", ns), d.catalog.createNamespace(ctx, ns)
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	return d.catalog.CreateResource(ctx, &res.MaterializationSpec_Binding, res.Config)
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	return d.catalog.DeleteResource(ctx, resourcePath)
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	panic("not supported")
}

func (d *materialization) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	return d.catalog.UpdateResource(ctx, update)
}

func (d *materialization) NewTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *m.BindingEvents,
) (m.Transactor, error) {
	var resourcePaths [][]string
	var bindings []binding

	for i := range mappedBindings {
		b := &mappedBindings[i]
		pqSchema, err := parquetSchema(b.FieldSelection.AllFields(), b.Collection, b.FieldSelection.FieldConfigJsonMap)
		if err != nil {
			return nil, err
		}

		partitionSpec, partitionCols, err := buildPartitionSpec(b.FieldSelection.AllFields(), pqSchema, b.Config.PartitionFields)
		if err != nil {
			return nil, err
		}

		resourcePaths = append(resourcePaths, b.ResourcePath)
		bindings = append(bindings, binding{
			path:          b.ResourcePath,
			pqSchema:      pqSchema,
			stateKey:      b.StateKey,
			mapped:        b,
			partitionSpec: partitionSpec,
			partitionCols: partitionCols,
		})
	}

	tables, err := d.catalog.loadResourceTables(ctx, resourcePaths)
	if err != nil {
		return nil, fmt.Errorf("looking up tables: %w", err)
	}

	for idx := range bindings {
		bindings[idx].catalogTablePath = tables[idx].Location()
		// The partition spec is immutable after creation; refuse to write if the
		// table's spec no longer matches the configured one.
		existing := tables[idx].Spec()
		if err := partitionSpecMismatch(&existing, bindings[idx].partitionSpec); err != nil {
			return nil, fmt.Errorf("table %q %w", pathToFQN(bindings[idx].path), err)
		}
	}

	s3store, err := filesink.NewS3Store(ctx, d.cfg.s3StoreConfig())
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

func (d *materialization) ListTestTasks(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (d *materialization) CleanupTestTask(ctx context.Context, taskName string) error {
	return nil
}

func (d *materialization) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	tablePaths, err := d.catalog.tablePaths(ctx, [][]string{path})
	if err != nil {
		return nil, nil, fmt.Errorf("getting table path for %s.%s: %w", path[0], path[1], err)
	}

	tableLocation := tablePaths[0]
	if tableLocation == "" {
		return nil, nil, nil
	}

	// The data directory is at {tableLocation}/data/
	// tableLocation is like s3://{bucket}/{prefix}/...
	dataPrefix := strings.TrimPrefix(tableLocation, "s3://"+d.cfg.Bucket+"/")
	dataPrefix = dataPrefix + "/data/"

	s3store, err := filesink.NewS3Store(ctx, d.cfg.s3StoreConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("creating s3 store: %w", err)
	}
	s3client := s3store.Client()

	listOut, err := s3client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.cfg.Bucket),
		Prefix: aws.String(dataPrefix),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("listing objects at %s: %w", dataPrefix, err)
	}

	var parquetKeys []string
	for _, obj := range listOut.Contents {
		if strings.HasSuffix(*obj.Key, ".parquet") {
			parquetKeys = append(parquetKeys, *obj.Key)
		}
	}

	if len(parquetKeys) == 0 {
		return nil, nil, nil
	}

	sort.Strings(parquetKeys)

	// Read parquet files using duckdb.
	var allRows []map[string]any
	for _, key := range parquetKeys {
		getOut, err := s3client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(d.cfg.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, nil, fmt.Errorf("getting object %s: %w", key, err)
		}

		data, err := io.ReadAll(getOut.Body)
		getOut.Body.Close()
		if err != nil {
			return nil, nil, fmt.Errorf("reading object %s: %w", key, err)
		}

		tmpFile, err := os.CreateTemp("", "iceberg-test-*.parquet")
		if err != nil {
			return nil, nil, err
		}
		tmpPath := tmpFile.Name()
		if _, err := tmpFile.Write(data); err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
			return nil, nil, err
		}
		tmpFile.Close()

		out, err := exec.CommandContext(ctx, "duckdb", "-json", ":memory:",
			fmt.Sprintf("SET timezone TO 'UTC'; SELECT * FROM '%s' ORDER BY flow_published_at;", tmpPath),
		).Output()
		os.Remove(tmpPath)
		if err != nil {
			return nil, nil, fmt.Errorf("running duckdb on %s: %w", key, err)
		}

		var rows []map[string]any
		if err := json.Unmarshal(out, &rows); err != nil {
			return nil, nil, fmt.Errorf("parsing duckdb output: %w", err)
		}
		allRows = append(allRows, rows...)
	}

	if len(allRows) == 0 {
		return nil, nil, nil
	}

	// Extract column names.
	colSet := make(map[string]struct{})
	for _, row := range allRows {
		for k := range row {
			colSet[k] = struct{}{}
		}
	}
	columns := make([]string, 0, len(colSet))
	for k := range colSet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	// Build rows in column order.
	var result [][]any
	for _, row := range allRows {
		r := make([]any, len(columns))
		for i, col := range columns {
			r[i] = row[col]
		}
		result = append(result, r)
	}

	// Sort for determinism.
	sort.Slice(result, func(i, j int) bool {
		for c := range columns {
			vi := fmt.Sprint(result[i][c])
			vj := fmt.Sprint(result[j][c])
			if vi != vj {
				return vi < vj
			}
		}
		return false
	})

	return columns, result, nil
}

func (d *materialization) Close(ctx context.Context) {}

func main() { boilerplate.RunMain(driver{}) }
