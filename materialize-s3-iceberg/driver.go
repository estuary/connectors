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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsHttp "github.com/aws/smithy-go/transport/http"
	"github.com/estuary/connectors/filesink"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	iso8601 "github.com/senseyeio/duration"
)

var featureFlagDefaults = map[string]bool{
	"retain_existing_data_on_backfill": false,
}

type catalogType string

const (
	catalogTypeGlue catalogType = "AWS Glue"
	catalogTypeRest catalogType = "Iceberg REST Server"
)

// There is an equivalent pydantic model in iceberg-ctl, and the config schema is generated from
// that. The fields of this struct must be compatible with that model.
type config struct {
	Bucket             string                      `json:"bucket"`
	AWSAccessKeyID     string                      `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey string                      `json:"aws_secret_access_key,omitempty"`
	Credentials        *filesink.CredentialsConfig `json:"credentials,omitempty"`
	Namespace          string                      `json:"namespace"`
	Region             string                      `json:"region"`
	UploadInterval     string                      `json:"upload_interval"`
	Prefix             string                      `json:"prefix,omitempty"`
	S3Endpoint         string                      `json:"s3_endpoint,omitempty"`
	Catalog            catalogConfig               `json:"catalog"`
	Advanced           advancedConfig              `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
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

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

func (c config) s3StoreConfig() filesink.S3StoreConfig {
	cfg := filesink.S3StoreConfig{
		Bucket:   c.Bucket,
		Region:   c.Region,
		Endpoint: c.S3Endpoint,
	}

	if c.Credentials != nil {
		cfg.Credentials = c.Credentials
	} else {
		cfg.AWSAccessKeyID = c.AWSAccessKeyID
		cfg.AWSSecretAccessKey = c.AWSSecretAccessKey
	}

	return cfg
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

	hasLegacy := c.AWSAccessKeyID != "" || c.AWSSecretAccessKey != ""
	hasNew := c.Credentials != nil

	if hasLegacy && hasNew {
		return fmt.Errorf("cannot specify both top-level aws_access_key_id/aws_secret_access_key and credentials")
	} else if !hasLegacy && !hasNew {
		return fmt.Errorf("must provide either credentials or aws_access_key_id/aws_secret_access_key")
	} else if hasLegacy {
		if c.AWSAccessKeyID == "" {
			return fmt.Errorf("missing 'aws_access_key_id'")
		}
		if c.AWSSecretAccessKey == "" {
			return fmt.Errorf("missing 'aws_secret_access_key'")
		}
	} else if err := c.Credentials.Validate(); err != nil {
		return err
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

func (c config) DefaultNamespace() string {
	return ""
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

func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string, allTables bool) error {
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
	namespace := path[0]
	table := path[1]

	// Use iceberg-ctl's table-paths to find the data location.
	fqn := namespace + "." + table
	tableNamesJson, err := json.Marshal([]string{fqn})
	if err != nil {
		return nil, nil, err
	}

	b, err := runIcebergctl(ctx, &d.cfg, "table-paths", string(tableNamesJson))
	if err != nil {
		return nil, nil, fmt.Errorf("getting table path for %s: %w", fqn, err)
	}

	fqnToPath := make(map[string]string)
	if err := json.Unmarshal(b, &fqnToPath); err != nil {
		return nil, nil, fmt.Errorf("parsing table paths: %w", err)
	}

	tableLocation := fqnToPath[fqn]
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
