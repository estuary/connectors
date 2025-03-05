package connector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"net/http"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	emr "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go/aws"
	awsHttp "github.com/aws/smithy-go/transport/http"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-iceberg/catalog"
	"github.com/estuary/connectors/materialize-iceberg/python"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

const (
	// metadataPrefix is where metadata for the materialization is stored in the
	// configured staging file bucket. It should not be considered permanent,
	// although it is useful for files to persist in this bucket for some time.
	// As of right now only the PySpark scripts are stored here, but in the
	// future metadata related to table maintenance may be as well.
	metadataPrefix = "flow_metadata_v1"

	// The number of concurrent workers reading from S3 objects that contain
	// loaded document data. It probably won't take many of these to max out the
	// connector CPU.
	loadFileWorkers = 3

	// Generated name of a file representing the status of an EMR job. Output by
	// the various PySpark files.
	statusFile = "status.json"
)

type Driver struct{}

var _ boilerplate.Connector = &Driver{}

func (Driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
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

func (Driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (Driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	mtz, _, res, err := boilerplate.RunApply[*materialization](ctx, req, newMaterialization)
	if err != nil {
		return nil, err
	}

	if mtz.cfg.CatalogAuthentication.CatalogAuthType == catalogAuthTypeClientCredential {
		secretName := clientCredSecretName(mtz.cfg.Compute.SystemsManagerPrefix, req.Materialization.Name.String())
		if err := ensureEmrSecret(ctx, mtz.ssmClient, secretName, mtz.cfg.CatalogAuthentication.Credential); err != nil {
			return nil, fmt.Errorf("resolving client credential secret for EMR in Systems Manager: %w", err)
		}
	}

	return res, nil
}

func (Driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg       config
	catalog   *catalog.Catalog
	s3Client  *s3.Client
	emrClient *emr.Client
	ssmClient *ssm.Client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mapped] = &materialization{}

func newMaterialization(ctx context.Context, cfg config) (boilerplate.Materializer[config, fieldConfig, resource, mapped], error) {
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

	ssmClient, err := cfg.toSsmClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating EMR client: %w", err)
	}

	return &materialization{
		cfg:       cfg,
		catalog:   catalog,
		s3Client:  s3,
		emrClient: emr,
		ssmClient: ssmClient,
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
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

func (d *materialization) SetResourceDefaults(c config, res resource) resource {
	if res.Namespace == "" {
		res.Namespace = c.Namespace
	}

	return res
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
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
		table, err := d.catalog.GetTable(ctx, ns, t)
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
		res.Meta = table.Metadata

		for _, f := range table.Metadata.CurrentSchema().Fields() {
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

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	checkEmrPrereqs(ctx, d, errs)

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

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mapped, boilerplate.ElementConverter) {
	return mapProjection(p)
}

func (d *materialization) Compatible(existing boilerplate.ExistingField, proposed mapped) bool {
	return strings.EqualFold(existing.Type, string(proposed.type_.String()))
}

func (d *materialization) DescriptionForType(prop mapped) string {
	return string(prop.type_.String())
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	if err := d.catalog.CreateNamespace(ctx, ns); err != nil {
		return "", fmt.Errorf("creating namespace: %w", err)
	}

	return fmt.Sprintf("created namespace %s", ns), nil
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mapped]) (string, boilerplate.ActionApplyFn, error) {
	ns := res.ResourcePath[0]
	name := res.ResourcePath[1]
	schema := computeSchemaForNewTable(res)

	var location *string
	if d.cfg.BaseLocation != "" {
		base := strings.TrimSuffix(d.cfg.BaseLocation, "/") + "/" + sanitizeAndAppendHash(name)
		location = &base
	}

	return fmt.Sprintf("created table %q.%q as %s", ns, name, schema.String()), func(ctx context.Context) error {
		// The list of IdentifierFieldIDs will be empty for delta updates
		// tables. For standard updates it is populated in collection key order.
		// Collection keys are never allowed to change, and neither are the keys
		// that were initially selected for a materialization.
		return d.catalog.CreateTable(ctx, ns, name, schema, schema.IdentifierFieldIDs, location)
	}, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	ns := resourcePath[0]
	name := resourcePath[1]

	return fmt.Sprintf("deleted table %q.%q", ns, name), func(ctx context.Context) error {
		return d.catalog.DeleteTable(ctx, ns, name)
	}, nil
}

func (d *materialization) UpdateResource(
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
	current := existing.Meta.(table.Metadata).CurrentSchema()
	next := computeSchemaForUpdatedTable(current, update)
	reqs := []catalog.TableRequirement{catalog.AssertCurrentSchemaID(current.ID)}
	upds := []catalog.TableUpdate{catalog.AddSchemaUpdate(next), catalog.SetCurrentSchemaUpdate(next.ID)}

	return fmt.Sprintf("updated table %q.%q schema from %s to %s", ns, name, current.String(), next.String()), func(ctx context.Context) error {
		return d.catalog.UpdateTable(ctx, ns, name, reqs, upds)
	}, nil
}

//go:embed python
var pyFilesFS embed.FS

func (d *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mapped],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	storageClient := newFileClient(d.s3Client, d.cfg.Compute.emrConfig.Bucket)

	t := &transactor{
		materializationName: req.Materialization.Name.String(),
		be:                  be,
		cfg:                 d.cfg,
		s3Client:            d.s3Client,
		emrClient:           d.emrClient,
		templates:           parseTemplates(),
		bindings:            make([]binding, 0, len(mappedBindings)),
		loadFiles:           boilerplate.NewStagedFiles(storageClient, fileSizeLimit, d.cfg.Compute.emrConfig.BucketPath, false, false),
		storeFiles:          boilerplate.NewStagedFiles(storageClient, fileSizeLimit, d.cfg.Compute.emrConfig.BucketPath, true, true),
	}

	if d.cfg.CatalogAuthentication.CatalogAuthType == catalogAuthTypeClientCredential {
		t.emrAuth.credentialSecretName = clientCredSecretName(d.cfg.Compute.SystemsManagerPrefix, req.Materialization.Name.String())
		t.emrAuth.scope = d.cfg.CatalogAuthentication.Scope
	}

	// PySpark scripts are uploaded to the staging bucket metadata location
	// under a prefix that include a hash of their contents. If the scripts are
	// changed because of connector updates a new set of files will be written,
	// but this is not expected to happen very often so the number of excessive
	// files should be minimal.
	pyFiles := []struct {
		name  string
		tProp *string
	}{
		{name: "common", tProp: &t.pyFiles.commonURI},
		{name: "load", tProp: &t.pyFiles.loadURI},
		{name: "merge", tProp: &t.pyFiles.mergeURI},
	}
	pyBytes := make([][]byte, 0, len(pyFiles))
	hasher := sha256.New()

	for _, pf := range pyFiles {
		bs, err := pyFilesFS.ReadFile(fmt.Sprintf("python/%s.py", pf.name))
		if err != nil {
			return nil, fmt.Errorf("reading embedded python script %s: %w", pf.name, err)
		}
		if _, err := hasher.Write(bs); err != nil {
			return nil, fmt.Errorf("computing hash of embedded python script %s: %w", pf.name, err)
		}
		pyBytes = append(pyBytes, bs)
	}

	fullMetaPrefix := path.Join(d.cfg.Compute.BucketPath, metadataPrefix, "pySparkFiles", fmt.Sprintf("%x", hasher.Sum(nil)))
	for idx, pf := range pyFiles {
		key := path.Join(fullMetaPrefix, fmt.Sprintf("%s.py", pf.name))
		if _, err := t.s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(d.cfg.Compute.emrConfig.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(pyBytes[idx]),
		}); err != nil {
			return nil, fmt.Errorf("uploading %s: %w", pf.name, err)
		}

		*pf.tProp = "s3://" + path.Join(d.cfg.Compute.emrConfig.Bucket, key)
		log.WithField("uri", *pf.tProp).Debug("uploaded PySpark script")
	}

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
