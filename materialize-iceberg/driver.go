package connector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/estuary/connectors/go/blob"
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
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (Driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg       config
	catalog   *catalog.Catalog
	bucket    blob.Bucket
	ssmClient *ssm.Client
	emrClient *emrClient
	templates templates
	pyFiles   *pyFileURIs // populated in Setup and NewMaterializerTransactor
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mapped] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config) (boilerplate.Materializer[config, fieldConfig, resource, mapped], error) {
	catalog, err := cfg.toCatalog(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating catalog: %w", err)
	}

	bucket, err := cfg.toBucket(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating bucket: %w", err)
	}

	ssmClient, err := cfg.toSsmClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating EMR client: %w", err)
	}

	emr, err := cfg.toEmrClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("creating EMR client: %w", err)
	}

	return &materialization{
		cfg:     cfg,
		catalog: catalog,
		bucket:  bucket,
		emrClient: &emrClient{
			cfg:                 cfg.Compute.emrConfig,
			catalogAuth:         cfg.CatalogAuthentication,
			catalogURL:          cfg.URL,
			warehouse:           cfg.Warehouse,
			materializationName: materializationName,
			c:                   emr,
			bucket:              bucket,
			ssmClient:           ssmClient,
		},
		ssmClient: ssmClient,
		templates: parseTemplates(),
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	var translate boilerplate.TranslateFieldFn
	if d.cfg.Advanced.LowercaseColumnNames {
		translate = func(f string) string { return strings.ToLower(f) }
	}

	return boilerplate.MaterializeCfg{
		Translate:             translate,
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
				Type:       f.Type.String(),
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

	d.emrClient.checkPrereqs(ctx, errs)

	if err := d.bucket.CheckPermissions(ctx, blob.CheckPermissionsConfig{
		Prefix: d.cfg.Compute.BucketPath,
		Lister: true,
	}); err != nil {
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
	return mapProjection(p, d.cfg.Advanced.LowercaseColumnNames)
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	if d.cfg.CatalogAuthentication.CatalogAuthType == catalogAuthTypeClientCredential {
		if err := d.emrClient.ensureSecret(ctx, d.cfg.CatalogAuthentication.Credential); err != nil {
			return "", err
		}
	}

	pyFiles, err := d.putPyFiles(ctx)
	if err != nil {
		return "", fmt.Errorf("putting PySpark scripts: %w", err)
	}
	d.pyFiles = pyFiles

	return "", nil
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
	if len(update.NewProjections) == 0 && len(update.NewlyNullableFields) == 0 && len(update.FieldsToMigrate) == 0 {
		return "", nil, nil
	}

	ns := resourcePath[0]
	name := resourcePath[1]
	table := existing.Meta.(table.Metadata)
	current := table.CurrentSchema()
	next := computeSchemaForUpdatedTable(table.LastColumnID(), current, update)
	reqs := []catalog.TableRequirement{catalog.AssertCurrentSchemaID(current.ID)}
	upds := []catalog.TableUpdate{catalog.AddSchemaUpdate(next), catalog.SetCurrentSchemaUpdate(next.ID)}
	action := fmt.Sprintf("updated table %q.%q schema from %s to %s", ns, name, current.String(), next.String())

	var afterMigrateReqs []catalog.TableRequirement
	var afterMigrateUpds []catalog.TableUpdate
	if len(update.FieldsToMigrate) > 0 {
		// If there are migrations, the table is initially updated to |next|,
		// which will add temporary columns that existing data will be cast
		// into. After the compute job is run to cast the existing column data
		// into the temporary column, the table is updated to |afterMigrate|,
		// which will delete the original columns and rename the temporary
		// columns to be the name of the original columns in an atomic schema
		// update.
		//
		// The AssertSchemaID requirements throughout these two table updates
		// ensure that no other process comes in and modifies the table schema
		// while this is happening. If the schema IDs aren't as expected, the
		// connector will crash and start over, which should work fine.
		afterMigrate := computeSchemaForCompletedMigrations(next, update.FieldsToMigrate)
		afterMigrateReqs = []catalog.TableRequirement{catalog.AssertCurrentSchemaID(next.ID)}
		afterMigrateUpds = []catalog.TableUpdate{catalog.AddSchemaUpdate(afterMigrate), catalog.SetCurrentSchemaUpdate(afterMigrate.ID)}
		action = fmt.Sprintf("updated table %q.%q schema from %s to %s", ns, name, current.String(), afterMigrate.String())
	}

	return action, func(ctx context.Context) error {
		if err := d.catalog.UpdateTable(ctx, ns, name, reqs, upds); err != nil {
			return err
		}

		if len(update.FieldsToMigrate) > 0 {
			input := migrateInput{
				ResourcePath: resourcePath,
				Migrations:   make([]migrateColumn, 0, len(update.FieldsToMigrate)),
			}
			for _, migration := range update.FieldsToMigrate {
				input.Migrations = append(input.Migrations, migrateColumn{
					Name:       migration.From.Name,
					FromType:   migration.From.Type,
					TargetType: migration.To.Mapped.type_,
				})
			}

			var q strings.Builder
			if err := d.templates.migrateQuery.Execute(&q, input); err != nil {
				return err
			}

			outputPrefix := path.Join(d.emrClient.cfg.BucketPath, uuid.NewString())
			defer func() {
				if err := cleanPrefixOnceFn(ctx, d.bucket, outputPrefix)(); err != nil {
					log.WithError(err).Warn("failed to clean up status file after running a column migration job")
				}
			}()

			ll := log.WithFields(log.Fields{
				"table":      fmt.Sprintf("%s.%s", ns, name),
				"numColumns": len(update.FieldsToMigrate),
			})
			ll.Info("running column migration job")
			ts := time.Now()
			if err := d.emrClient.runJob(
				ctx,
				python.ExecInput{Query: q.String()},
				d.pyFiles.exec,
				d.pyFiles.common,
				fmt.Sprintf("column migration for: %s", d.emrClient.materializationName),
				outputPrefix,
			); err != nil {
				return fmt.Errorf("failed to run column migration job: %w", err)
			}
			ll.WithField("took", time.Since(ts).String()).Info("column migration job complete")

			if err := d.catalog.UpdateTable(ctx, ns, name, afterMigrateReqs, afterMigrateUpds); err != nil {
				return fmt.Errorf("failed to update table %s.%s after migration job: %w", ns, name, err)
			}

		}

		return nil
	}, nil
}

func (d *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mapped],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	pyFiles, err := d.putPyFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("putting PySpark scripts: %w", err)
	}

	t := &transactor{
		materializationName: d.emrClient.materializationName,
		be:                  be,
		cfg:                 d.cfg,
		bucket:              d.bucket,
		emrClient:           d.emrClient,
		templates:           d.templates,
		bindings:            make([]binding, 0, len(mappedBindings)),
		loadFiles:           boilerplate.NewStagedFiles(stagedFileClient{}, d.bucket, fileSizeLimit, d.cfg.Compute.emrConfig.BucketPath, false, false),
		storeFiles:          boilerplate.NewStagedFiles(stagedFileClient{}, d.bucket, fileSizeLimit, d.cfg.Compute.emrConfig.BucketPath, true, true),
		pyFiles:             *pyFiles,
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

//go:embed python
var pyFilesFS embed.FS

type pyFileURIs struct {
	common string
	exec   string
	load   string
	merge  string
}

// putPyFiles uploads PySpark scripts to the staging bucket metadata location
// under a prefix that includes a hash of their contents. If the scripts are
// changed because of connector updates a new set of files will be written, but
// this is not expected to happen very often so the number of excessive files
// should be minimal.
func (d *materialization) putPyFiles(ctx context.Context) (*pyFileURIs, error) {
	var out pyFileURIs

	pyFiles := []struct {
		name string
		prop *string
	}{
		{name: "common", prop: &out.common},
		{name: "exec", prop: &out.exec},
		{name: "load", prop: &out.load},
		{name: "merge", prop: &out.merge},
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

	fullMetaPrefix := path.Join(d.cfg.Compute.emrConfig.BucketPath, metadataPrefix, "pySparkFiles", fmt.Sprintf("%x", hasher.Sum(nil)))
	for idx, pf := range pyFiles {
		key := path.Join(fullMetaPrefix, fmt.Sprintf("%s.py", pf.name))
		if err := d.bucket.Upload(ctx, key, bytes.NewReader(pyBytes[idx])); err != nil {
			return nil, fmt.Errorf("uploading %s: %w", pf.name, err)
		}

		*pf.prop = "s3://" + path.Join(d.cfg.Compute.emrConfig.Bucket, key)
		log.WithField("uri", *pf.prop).Debug("uploaded PySpark script")
	}

	d.pyFiles = &out
	return d.pyFiles, nil
}
