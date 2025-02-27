package main

import (
	"bytes"
	"context"
	"embed"
	"errors"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	awsHttp "github.com/aws/smithy-go/transport/http"
	"github.com/cespare/xxhash/v2"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

var _ boilerplate.Connector = &driver{}
var _ boilerplate.Materializer[config, fieldConfig, resource, mapped] = &driver{}

type driver struct {
	cfg       config
	catalog   *catalog.Catalog
	s3Client  *s3.Client
	emrClient *emr.Client
}

func newDriver(ctx context.Context, cfg config) (boilerplate.Materializer[config, fieldConfig, resource, mapped], error) {
	catalog, err := cfg.toCatalog(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating catalog: %w", err)
	}

	s3, err := cfg.toS3Client(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}

	emr, err := cfg.toEmrClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating EMR client: %w", err)
	}

	return &driver{
		cfg:       cfg,
		catalog:   catalog,
		s3Client:  s3,
		emrClient: emr,
	}, nil
}

func (d *driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("configSchema", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("resourceConfigSchema", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-iceberg", endpointSchema, resourceSchema)
}

func (d *driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newDriver)
}

func (d *driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	_, res, err := boilerplate.RunApply(ctx, req, newDriver)
	return res, err
}

func (d *driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newDriver)
}

func (d *driver) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		ConcurrentApply:       true,
		MaxFieldLength:        255,
		CaseInsensitiveFields: true,
		MaterializeOptions: boilerplate.MaterializeOptions{
			ExtendedLogging: true,
			AckSchedule: &boilerplate.AckScheduleOption{
				Config: d.cfg.Schedule,
				Jitter: []byte(d.cfg.Compute.ApplicationId),
			},
			DBTJobTrigger: &d.cfg.DBTJobTrigger,
		},
	}
}

func (d *driver) NewResource(c config, res resource) resource {
	if res.Namespace == "" {
		res.Namespace = c.Namespace
	}

	return res
}

func (d *driver) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	relevantPaths := make(map[string][]string)
	for _, path := range resourcePaths {
		relevantPaths[path[0]] = append(relevantPaths[path[0]], path[1])
	}

	existingNamespaces, err := d.catalog.ListNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("listing namespaces: %w", err)
	}

	for _, ns := range existingNamespaces {
		is.PushNamespace(ns)
	}

	var workerLimit = 5
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workerLimit)
	var mu sync.Mutex

	processTableMetadata := func(ctx context.Context, ns string, t string) error {
		meta, err := d.catalog.TableMetadata(ctx, ns, t)
		if err != nil {
			if errors.Is(err, catalog.ErrTableNotFound) {
				// Most likely cause is this is not an Iceberg table, or it has
				// been deleted very recently.
				return nil
			}
			return fmt.Errorf("getting table: %w", err)
		}

		mu.Lock()
		defer mu.Unlock()

		res := is.PushResource(ns, t)
		res.Meta = meta

		for _, f := range meta.CurrentSchema().Fields() {
			res.PushField(boilerplate.ExistingField{
				Name:       f.Name,
				Nullable:   !f.Required,
				Type:       f.Type.Type(),
				HasDefault: f.WriteDefault != nil,
			})
		}

		return nil
	}

	for _, ns := range existingNamespaces {
		relevantTables, ok := relevantPaths[ns]
		if !ok {
			continue
		}

		existingTables, err := d.catalog.ListTables(ctx, ns)
		if err != nil {
			return fmt.Errorf("listing tables: %w", err)
		}

		for _, t := range existingTables {
			if !slices.Contains(relevantTables, t) {
				continue
			}

			group.Go(func() error {
				return processTableMetadata(groupCtx, ns, t)
			})
		}
	}

	return group.Wait()
}

func (d *driver) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	bucket := d.cfg.Compute.Bucket
	bucketWithPath := path.Join(bucket, d.cfg.Compute.BucketPath)
	testKey := path.Join(d.cfg.Compute.BucketPath, uuid.NewString())

	var awsErr *awsHttp.ResponseError
	if _, err := d.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
		Body:   strings.NewReader("testing"),
	}); err != nil {
		if errors.As(err, &awsErr) {
			if awsErr.Response.Response.StatusCode == http.StatusNotFound {
				err = fmt.Errorf("bucket %q does not exist", bucket)
			} else if awsErr.Response.Response.StatusCode == http.StatusForbidden {
				err = fmt.Errorf("not authorized to write to %q", bucketWithPath)
			}
		}
		errs.Err(err)
	} else if _, err := d.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to read from %q", bucketWithPath)
		}
		errs.Err(err)
	} else if _, err := d.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: func() *string {
			if d.cfg.Compute.BucketPath != "" {
				return aws.String(d.cfg.Compute.BucketPath)
			}
			return nil
		}(),
		MaxKeys: 1,
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to list bucket %q", bucket)
		}
		errs.Err(err)
	} else if _, err := d.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(testKey),
	}); err != nil {
		if errors.As(err, &awsErr) && awsErr.Response.Response.StatusCode == http.StatusForbidden {
			err = fmt.Errorf("not authorized to delete from %q", bucketWithPath)
		}
		errs.Err(err)
	}

	return errs
}

func (d *driver) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
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

func (d *driver) MapType(p boilerplate.Projection, fc fieldConfig) (mapped, boilerplate.ElementConverter) {
	return mapProjection(p)
}

func (d *driver) Compatible(existing boilerplate.ExistingField, proposed mapped) bool {
	return strings.EqualFold(existing.Type, string(proposed.type_.Type()))
}

func (d *driver) DescriptionForType(prop mapped) string {
	return string(prop.type_.Type())
}

func (d *driver) CreateNamespace(ctx context.Context, ns string) error {
	return d.catalog.CreateNamespace(ctx, ns)
}

func (d *driver) CreateResource(ctx context.Context, res boilerplate.MappedBinding[mapped, resource]) (string, boilerplate.ActionApplyFn, error) {
	ns := res.ResourcePath[0]
	name := res.ResourcePath[1]
	schema := computeSchemaForNewTable(res)

	return fmt.Sprintf("created table %q.%q as %s", ns, name, schema.String()), func(ctx context.Context) error {
		return d.catalog.CreateTable(ctx, ns, name, schema, d.cfg.Location)
	}, nil
}

func (d *driver) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	ns := resourcePath[0]
	name := resourcePath[1]

	return fmt.Sprintf("deleted table %q.%q", ns, name), func(ctx context.Context) error {
		return d.catalog.DeleteTable(ctx, ns, name)
	}, nil
}

func (d *driver) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.MaterializerBindingUpdate[mapped],
) (string, boilerplate.ActionApplyFn, error) {
	if len(update.NewProjections) == 0 && len(update.NewlyNullableFields) == 0 {
		return "", nil, nil
	}

	ns := resourcePath[0]
	name := resourcePath[1]
	current := existing.Meta.(*catalog.TableMetadata).CurrentSchema()
	next := computeSchemaForUpdatedTable(current, update)
	reqs := []catalog.TableRequirement{catalog.AssertCurrentSchemaID(current.ID)}
	upds := []catalog.TableUpdate{catalog.AddSchemaUpdate(next), catalog.SetCurrentSchemaUpdate(next.ID)}

	return fmt.Sprintf("updated table %q.%q schema from %s to %s", ns, name, current.String(), next.String()), func(ctx context.Context) error {
		return d.catalog.UpdateTable(ctx, ns, name, reqs, upds)
	}, nil
}

//go:embed python
var pyFilesFS embed.FS

func (d *driver) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[mapped, resource],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	storageClient := newFileClient(d.s3Client, d.cfg.Compute.emrConfig.Bucket)

	t := &transactor{
		be:         be,
		cfg:        d.cfg,
		s3Client:   d.s3Client,
		emrClient:  d.emrClient,
		templates:  parseTemplates(),
		bindings:   make([]binding, 0, len(mappedBindings)),
		loadFiles:  boilerplate.NewStagedFiles(storageClient, fileSizeLimit, d.cfg.Compute.emrConfig.BucketPath, false, false),
		storeFiles: boilerplate.NewStagedFiles(storageClient, fileSizeLimit, d.cfg.Compute.emrConfig.BucketPath, true, true),
	}

	// TODO: Make this something based on the materialization name and shard
	// Maybe do it during apply?
	// Don't do it on apply, and give it a random thing maybe
	commonPyFileKey := path.Join(d.cfg.Compute.emrConfig.BucketPath, sanitizedId(req.Materialization.Name.String()), "common.py")
	loadPyFileKey := path.Join(d.cfg.Compute.emrConfig.BucketPath, sanitizedId(req.Materialization.Name.String()), "load.py")
	mergePyFileKey := path.Join(d.cfg.Compute.emrConfig.BucketPath, sanitizedId(req.Materialization.Name.String()), "merge.py")

	if common, err := pyFilesFS.ReadFile("python/common.py"); err != nil {
		return nil, fmt.Errorf("reading common.py: %w", err)
	} else if _, err := t.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(d.cfg.Compute.emrConfig.Bucket),
		Key:    aws.String(commonPyFileKey),
		Body:   bytes.NewReader(common),
	}); err != nil {
		return nil, fmt.Errorf("uploading common.py: %w", err)
	}

	if load, err := pyFilesFS.ReadFile("python/load.py"); err != nil {
		return nil, fmt.Errorf("reading load.py: %w", err)
	} else if _, err := t.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(d.cfg.Compute.emrConfig.Bucket),
		Key:    aws.String(loadPyFileKey),
		Body:   bytes.NewReader(load),
	}); err != nil {
		return nil, fmt.Errorf("uploading load.py: %w", err)
	}

	if merge, err := pyFilesFS.ReadFile("python/merge.py"); err != nil {
		return nil, fmt.Errorf("reading merge.py: %w", err)
	} else if _, err := t.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(d.cfg.Compute.emrConfig.Bucket),
		Key:    aws.String(mergePyFileKey),
		Body:   bytes.NewReader(merge),
	}); err != nil {
		return nil, fmt.Errorf("uploading merge.py: %w", err)
	}

	t.pyFiles.commonURI = "s3://" + path.Join(d.cfg.Compute.emrConfig.Bucket, commonPyFileKey)
	t.pyFiles.loadURI = "s3://" + path.Join(d.cfg.Compute.emrConfig.Bucket, loadPyFileKey)
	t.pyFiles.mergeURI = "s3://" + path.Join(d.cfg.Compute.emrConfig.Bucket, mergePyFileKey)

	for idx, mapped := range mappedBindings {
		t.loadFiles.AddBinding(idx, nil)
		t.storeFiles.AddBinding(idx, nil)

		b := binding{
			Idx:    idx,
			Mapped: &mapped,
		}

		for _, p := range mapped.SelectedProjections() {
			pyField := python.NestedField{
				Name: p.Field,
				Type: p.Mapped.type_.Type(),
			}
			if m, ok := p.Mapped.type_.(*iceberg.ListType); ok {
				pyField.Element = m.Element.Type()
			}

			if p.IsPrimaryKey {
				b.load.keys = append(b.load.keys, pyField)
			}
			b.store.columns = append(b.store.columns, pyField)
		}

		b.load.mergeBounds = newMergeBoundsBuilder(mapped.Keys)
		b.store.mergeBounds = newMergeBoundsBuilder(mapped.Keys)

		t.bindings = append(t.bindings, b)
	}

	return t, nil
}

var sanitizerRegexp = regexp.MustCompile(`[^\-_0-9a-zA-Z]`)

// TODO: Might only need a single input string, not a variadic list.
func sanitizedId(in ...string) string {
	joined := strings.Join(in, "_")
	sanitized := sanitizerRegexp.ReplaceAllString(joined, "_")
	if len(sanitized) > 64 {
		sanitized = sanitized[:64]
	}

	return fmt.Sprintf("%s_%016X", sanitized, xxhash.Sum64String(joined))
}
