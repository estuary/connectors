package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergglue "github.com/apache/iceberg-go/catalog/glue"
	icebergrest "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud" // registers s3, gs, abfs schemes
	icebergtable "github.com/apache/iceberg-go/table"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/estuary/connectors/filesink"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// flowCheckpointsKey is the table property used to track per-materialization
// checkpoints for the Recovery Log with Idempotent Apply pattern.
const flowCheckpointsKey = "flow_checkpoints_v1"

type catalog struct {
	cfg           *config
	locationStyle LocationStyle
	cat           icebergcatalog.Catalog
}

func newCatalog(ctx context.Context, cfg config, locationStyle LocationStyle) (*catalog, error) {
	cfg.normalizeCredentials()

	var cat icebergcatalog.Catalog
	var err error
	switch cfg.Catalog.CatalogType {
	case catalogTypeRest:
		cat, err = buildRestCatalog(ctx, &cfg)
	case catalogTypeGlue:
		cat, err = buildGlueCatalog(ctx, &cfg)
	default:
		return nil, fmt.Errorf("unsupported catalog type %q", cfg.Catalog.CatalogType)
	}
	if err != nil {
		return nil, err
	}

	return &catalog{
		cfg:           &cfg,
		locationStyle: locationStyle,
		cat:           cat,
	}, nil
}

func buildRestCatalog(ctx context.Context, cfg *config) (icebergcatalog.Catalog, error) {
	var opts []icebergrest.Option
	if cfg.Catalog.Credential != "" {
		opts = append(opts, icebergrest.WithCredential(cfg.Catalog.Credential))
	}
	if cfg.Catalog.Token != "" {
		opts = append(opts, icebergrest.WithOAuthToken(cfg.Catalog.Token))
	}
	if cfg.Catalog.Warehouse != "" {
		opts = append(opts, icebergrest.WithWarehouseLocation(cfg.Catalog.Warehouse))
	}
	if cfg.Catalog.Scope != "" {
		opts = append(opts, icebergrest.WithScope(cfg.Catalog.Scope))
	}

	// The S3 region (and any custom endpoint) is a client-side fact that is
	// independent of how credentials are obtained: even when the catalog vends
	// per-table S3 credentials, iceberg-go's FileIO still needs a region to
	// resolve the S3 endpoint. Always pass it so the catalog-vended path (AWS
	// Lakekeeper / Polaris, used by AWSIAM) can reach S3. Without this, vended
	// reads/writes fail with "A region must be set when sending requests to S3".
	fileIOProps := iceberg.Properties{}
	if cfg.Region != "" {
		fileIOProps[icebergio.S3Region] = cfg.Region
	}
	if cfg.S3Endpoint != "" {
		fileIOProps[icebergio.S3EndpointURL] = cfg.S3Endpoint
	}

	// Whenever the user supplied AWS access keys, bypass catalog credential
	// vending and pass the keys straight to iceberg-go's S3 FileIO. This
	// handles both S3-compatible setups (rustfs, MinIO) and non-vending REST
	// catalogs (e.g. Nessie) on real AWS. AWSIAM users fall through to the
	// catalog-vended path, which is the model AWS Lakekeeper / Polaris assume.
	directS3Creds := cfg.Credentials != nil &&
		cfg.Credentials.AuthType == filesink.AWSAccessKey &&
		cfg.Region != ""

	if directS3Creds {
		for k, v := range s3PropsForDirectCreds(cfg) {
			fileIOProps[k] = v
		}
		// Some catalogs (e.g. Polaris) gate _WITH_WRITE_DELEGATION ops on a
		// privilege the connector does not need when it has its own S3
		// credentials. The vended-credentials header is hardcoded in
		// iceberg-go's REST session, so strip it from outgoing requests.
		opts = append(opts, icebergrest.WithCustomTransport(&stripDelegationTransport{base: http.DefaultTransport}))
	}

	if len(fileIOProps) > 0 {
		opts = append(opts, icebergrest.WithAdditionalProps(fileIOProps))
	}

	return icebergrest.NewCatalog(ctx, "default", cfg.Catalog.URI, opts...)
}

func buildGlueCatalog(ctx context.Context, cfg *config) (icebergcatalog.Catalog, error) {
	credProvider, err := cfg.s3StoreConfig().CredentialsProvider(ctx)
	if err != nil {
		return nil, fmt.Errorf("building AWS credentials: %w", err)
	}
	awsCfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion(cfg.Region),
		awsConfig.WithCredentialsProvider(credProvider),
	)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	glueProps := icebergglue.AwsProperties{}
	if cfg.Catalog.GlueID != "" {
		glueProps[icebergglue.CatalogIdKey] = cfg.Catalog.GlueID
	}
	for k, v := range s3PropsForDirectCreds(cfg) {
		glueProps[k] = v
	}

	exportGlueFileIOEnv(cfg)

	return icebergglue.NewCatalog(
		icebergglue.WithAwsConfig(awsCfg),
		icebergglue.WithAwsProperties(glueProps),
	), nil
}

// exportGlueFileIOEnv publishes the resolved S3 region and credentials into the
// process environment so iceberg-go's S3 FileIO can find them on the Glue path.
//
// iceberg-go v0.6.0's only S3 FileIO is the gocloud backend, which reads its
// region and credentials solely from the FileIO properties it is handed. The
// Glue catalog loads tables with an empty property set (LoadFSFunc(nil, ...))
// and never forwards its AWS config, so those reads/writes would otherwise fall
// back to the default AWS credential chain with no region and fail with a 301
// redirect. (The REST catalog sidesteps this by passing WithAdditionalProps.)
// The gocloud opener builds its client from config.LoadDefaultConfig, which does
// consult the environment, so seeding these variables is the available hook.
//
// This is deliberately process-global but low-blast-radius: filesink's own S3
// access uses explicit credential providers and ignores the environment, and
// Flow restarts the connector when injected IAM session credentials rotate, so
// the values captured here stay valid for the process lifetime.
func exportGlueFileIOEnv(cfg *config) {
	if cfg.Region != "" {
		os.Setenv("AWS_REGION", cfg.Region)
	}
	props := s3PropsForDirectCreds(cfg)
	for prop, env := range map[string]string{
		icebergio.S3AccessKeyID:     "AWS_ACCESS_KEY_ID",
		icebergio.S3SecretAccessKey: "AWS_SECRET_ACCESS_KEY",
		icebergio.S3SessionToken:    "AWS_SESSION_TOKEN",
	} {
		if v := props[prop]; v != "" {
			os.Setenv(env, v)
		}
	}
}

// s3PropsForDirectCreds builds iceberg-go S3 IO properties when the connector
// is using its own S3 credentials directly (legacy or new AWSAccessKey/AWSIAM
// credentials). Returns an empty map if there are no direct credentials, in
// which case the catalog vends credentials via its standard flow.
func s3PropsForDirectCreds(cfg *config) iceberg.Properties {
	props := iceberg.Properties{}

	legacyAccessKey := strVal(cfg.AWSAccessKeyID)
	legacySecretKey := strVal(cfg.AWSSecretAccessKey)

	switch {
	case legacyAccessKey != "" && legacySecretKey != "":
		props[icebergio.S3AccessKeyID] = legacyAccessKey
		props[icebergio.S3SecretAccessKey] = legacySecretKey
		props[icebergio.S3Region] = cfg.Region
	case cfg.Credentials != nil:
		switch cfg.Credentials.AuthType {
		case filesink.AWSAccessKey:
			props[icebergio.S3AccessKeyID] = cfg.Credentials.AWSAccessKeyID
			props[icebergio.S3SecretAccessKey] = cfg.Credentials.AWSSecretAccessKey
			props[icebergio.S3Region] = cfg.Region
		case filesink.AWSIAM:
			// AWSAccessKeyID / AWSSecretAccessKey / AWSSessionToken under
			// IAMTokens are runtime-injected session credentials produced by
			// assuming the configured role — not user-provided keys. They must
			// be read through .IAMTokens explicitly: CredentialsConfig also
			// embeds AccessKeyCredentials, whose AWSAccessKeyID/
			// AWSSecretAccessKey fields (empty under IAM auth) otherwise shadow
			// these via shallower field promotion.
			props[icebergio.S3AccessKeyID] = cfg.Credentials.IAMTokens.AWSAccessKeyID
			props[icebergio.S3SecretAccessKey] = cfg.Credentials.IAMTokens.AWSSecretAccessKey
			props[icebergio.S3SessionToken] = cfg.Credentials.IAMTokens.AWSSessionToken
			props[icebergio.S3Region] = cfg.Region
		default:
			return iceberg.Properties{}
		}
	default:
		return iceberg.Properties{}
	}

	if cfg.S3Endpoint != "" {
		props[icebergio.S3EndpointURL] = cfg.S3Endpoint
	}
	return props
}

type stripDelegationTransport struct {
	base http.RoundTripper
}

func (t *stripDelegationTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, ok := req.Header["X-Iceberg-Access-Delegation"]; ok {
		req = req.Clone(req.Context())
		req.Header.Del("X-Iceberg-Access-Delegation")
	}
	return t.base.RoundTrip(req)
}

// loadTables loads each table identifier in parallel (up to 10 concurrent),
// preserving the input order in the returned slice.
func (c *catalog) loadTables(ctx context.Context, idents []icebergtable.Identifier) ([]*icebergtable.Table, error) {
	out := make([]*icebergtable.Table, len(idents))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(10)
	for i, ident := range idents {
		g.Go(func() error {
			tbl, err := c.cat.LoadTable(gctx, ident)
			if err != nil {
				return fmt.Errorf("loading table %s: %w", pathToFQN(ident), err)
			}
			out[i] = tbl
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *catalog) populateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	// info-schema is consulted before namespaces are necessarily created. List
	// the existing namespaces first, both to register them and to scan tables
	// only in namespaces that exist, mirroring the removed Python info_schema.
	// Relying on ListTables to report an absent namespace is not portable: Glue
	// surfaces a raw EntityNotFoundException never mapped to ErrNoSuchNamespace.
	existing, err := c.listNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("listing namespaces: %w", err)
	}
	for _, ns := range existing {
		is.PushNamespace(ns)
	}

	if len(resourcePaths) == 0 {
		// No bindings so there are no tables that we care about; nothing to do.
		return nil
	}

	wantTables := make(map[string]map[string]struct{})
	for _, p := range resourcePaths {
		if wantTables[p[0]] == nil {
			wantTables[p[0]] = map[string]struct{}{}
		}
		wantTables[p[0]][p[1]] = struct{}{}
	}

	log.WithField("count", len(resourcePaths)).Info("info-schema: scanning tables")

	existingNamespaces := make(map[string]struct{}, len(existing))
	for _, ns := range existing {
		existingNamespaces[ns] = struct{}{}
	}

	var matched []icebergtable.Identifier
	for ns, want := range wantTables {
		if _, ok := existingNamespaces[ns]; !ok {
			continue
		}
		nsIdent := icebergtable.Identifier{ns}
		for tblIdent, err := range c.cat.ListTables(ctx, nsIdent) {
			if err != nil {
				return fmt.Errorf("listing tables in %s: %w", ns, err)
			}
			tbl := tblIdent[len(tblIdent)-1]
			if _, ok := want[tbl]; ok {
				matched = append(matched, tblIdent)
			}
		}
	}

	loaded, err := c.loadTables(ctx, matched)
	if err != nil {
		return err
	}

	for i, tbl := range loaded {
		ident := matched[i]
		ns, name := ident[0], ident[len(ident)-1]
		res := is.PushResource(ns, name)
		for _, f := range tbl.Schema().Fields() {
			res.PushField(boilerplate.ExistingField{
				Name:     f.Name,
				Nullable: !f.Required,
				Type:     f.Type.Type(),
			})
		}
	}
	log.WithField("count", len(loaded)).Info("info-schema: found tables")
	return nil
}

// loadResourceTables loads the table for each resource path in parallel,
// preserving input order.
func (c *catalog) loadResourceTables(ctx context.Context, resourcePaths [][]string) ([]*icebergtable.Table, error) {
	idents := make([]icebergtable.Identifier, len(resourcePaths))
	for i, p := range resourcePaths {
		idents[i] = p
	}
	return c.loadTables(ctx, idents)
}

// tablePaths returns the registered storage path for each resource path in a
// list, in the same order as the input.
func (c *catalog) tablePaths(ctx context.Context, resourcePaths [][]string) ([]string, error) {
	loaded, err := c.loadResourceTables(ctx, resourcePaths)
	if err != nil {
		return nil, err
	}
	out := make([]string, len(loaded))
	for i, tbl := range loaded {
		out[i] = tbl.Location()
	}
	return out, nil
}

func (c *catalog) listNamespaces(ctx context.Context) ([]string, error) {
	got, err := c.cat.ListNamespaces(ctx, nil)
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(got))
	for _, ns := range got {
		if len(ns) == 0 {
			continue
		}
		out = append(out, ns[0])
	}
	return out, nil
}

func (c *catalog) createNamespace(ctx context.Context, namespace string) error {
	return c.cat.CreateNamespace(ctx, icebergtable.Identifier{namespace}, nil)
}

func (c *catalog) CreateResource(ctx context.Context, b *pf.MaterializationSpec_Binding, res resource) (string, boilerplate.ActionApplyFn, error) {
	location := tablePath(c.cfg.Bucket, c.cfg.Prefix, b.ResourcePath[0], b.ResourcePath[1], c.locationStyle)

	parquetSchema, err := parquetSchema(b.FieldSelection.AllFields(), b.Collection, b.FieldSelection.FieldConfigJsonMap)
	if err != nil {
		return "", nil, err
	}

	fields := make([]iceberg.NestedField, 0, len(parquetSchema))
	for i, f := range parquetSchema {
		fields = append(fields, iceberg.NestedField{
			ID:       i + 1,
			Name:     f.Name,
			Type:     parquetTypeToIcebergType(f.DataType),
			Required: f.Required,
		})
	}
	schema := iceberg.NewSchema(0, fields...)

	spec, _, err := buildPartitionSpec(b.FieldSelection.AllFields(), parquetSchema, res.PartitionFields)
	if err != nil {
		return "", nil, err
	}

	fqn := pathToFQN(b.ResourcePath)

	action := fmt.Sprintf("create table %q", fqn)
	if spec != nil {
		action += fmt.Sprintf(" partitioned by %s", spec)
	}

	return action, func(ctx context.Context) error {
		nameMappingJSON, err := json.Marshal(schema.NameMapping())
		if err != nil {
			return fmt.Errorf("marshaling name mapping: %w", err)
		}
		props := iceberg.Properties{
			icebergtable.DefaultNameMappingKey: string(nameMappingJSON),
		}
		for k, v := range res.AdditionalTableProperties {
			// Never let a user-supplied key override the name mapping.
			if k == icebergtable.DefaultNameMappingKey {
				continue
			}
			props[k] = v
		}

		opts := []icebergcatalog.CreateTableOpt{
			icebergcatalog.WithLocation(location),
			icebergcatalog.WithProperties(props),
		}
		if spec != nil {
			opts = append(opts, icebergcatalog.WithPartitionSpec(spec))
		}

		if _, err := c.cat.CreateTable(ctx, b.ResourcePath, schema, opts...); err != nil {
			return fmt.Errorf("creating table %q: %w", fqn, err)
		}

		return nil
	}, nil
}

func (c *catalog) DeleteResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	fqn := pathToFQN(path)

	return fmt.Sprintf("drop table %q", fqn), func(ctx context.Context) error {
		if err := c.cat.DropTable(ctx, path); err != nil {
			return fmt.Errorf("dropping table %q: %w", fqn, err)
		}

		return nil
	}, nil
}

func (c *catalog) UpdateResource(ctx context.Context, bindingUpdate boilerplate.BindingUpdate[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	b := bindingUpdate.Binding

	// The partition spec is immutable after creation, so reject a changed
	// partition_fields config before any other alteration is attempted.
	pqSchema, err := parquetSchema(b.FieldSelection.AllFields(), b.Collection, b.FieldSelection.FieldConfigJsonMap)
	if err != nil {
		return "", nil, err
	}
	desiredSpec, _, err := buildPartitionSpec(b.FieldSelection.AllFields(), pqSchema, b.Config.PartitionFields)
	if err != nil {
		return "", nil, err
	}
	if err := c.verifyPartitionSpec(ctx, b.ResourcePath, desiredSpec); err != nil {
		return "", nil, err
	}

	if len(bindingUpdate.NewProjections) == 0 && len(bindingUpdate.NewlyNullableFields) == 0 {
		// Nothing to do, since only adding new columns or dropping nullability
		// constraints is supported currently.
		return "", nil, nil
	}

	type addCol struct {
		name string
		typ  iceberg.Type
	}
	var adds []addCol
	for _, p := range bindingUpdate.NewProjections {
		var fc fieldConfig
		if rawFieldConfig, ok := bindingUpdate.Binding.FieldSelection.FieldConfigJsonMap[p.Field]; ok {
			if err := json.Unmarshal(rawFieldConfig, &fc); err != nil {
				return "", nil, fmt.Errorf("unmarshaling field config for %q: %w", p.Field, err)
			} else if err := fc.Validate(); err != nil {
				return "", nil, fmt.Errorf("validating field config for %q: %w", p.Field, err)
			}
		}

		s, err := projectionToParquetSchemaElement(p.Projection.Projection, fc)
		if err != nil {
			return "", nil, err
		}
		adds = append(adds, addCol{name: s.Name, typ: parquetTypeToIcebergType(s.DataType)})
	}

	relax := make([]string, 0, len(bindingUpdate.NewlyNullableFields))
	for _, f := range bindingUpdate.NewlyNullableFields {
		relax = append(relax, f.Name)
	}

	fqn := pathToFQN(bindingUpdate.Binding.ResourcePath)

	return fmt.Sprintf("alter table %q", fqn), func(ctx context.Context) error {
		tbl, err := c.cat.LoadTable(ctx, bindingUpdate.Binding.ResourcePath)
		if err != nil {
			return fmt.Errorf("loading table %q: %w", fqn, err)
		}

		tx := tbl.NewTransaction()
		// caseSensitive=true matches pyiceberg's update_schema() default that
		// the removed Python tool relied on; the connector's field names are
		// case-sensitive, so column matches during add/relax must be too.
		us := tx.UpdateSchema(true, false)
		for _, a := range adds {
			us = us.AddColumn([]string{a.name}, a.typ, "", false, nil)
		}
		for _, name := range relax {
			us = us.UpdateColumn([]string{name}, icebergtable.ColumnUpdate{
				Required: iceberg.Optional[bool]{Val: false, Valid: true},
			})
		}
		if err := us.Commit(); err != nil {
			return fmt.Errorf("staging schema update for %q: %w", fqn, err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("altering table %q: %w", fqn, err)
		}

		return nil
	}, nil
}

// verifyPartitionSpec loads the table and returns a hard error if its partition
// spec is not compatible with the desired spec (nil means unpartitioned). It is
// a no-op when the table does not yet exist, since CreateResource will create it
// with the desired spec.
func (c *catalog) verifyPartitionSpec(ctx context.Context, resourcePath []string, desired *iceberg.PartitionSpec) error {
	tbl, err := c.cat.LoadTable(ctx, resourcePath)
	if err != nil {
		if errors.Is(err, icebergcatalog.ErrNoSuchTable) {
			return nil
		}
		return fmt.Errorf("loading table %q: %w", pathToFQN(resourcePath), err)
	}
	existing := tbl.Spec()
	if err := partitionSpecMismatch(&existing, desired); err != nil {
		return fmt.Errorf("table %q %w", pathToFQN(resourcePath), err)
	}
	return nil
}

// appendDataFiles appends pre-built parquet files to a partitioned table,
// supplying explicit partition values for each file. Unlike appendFiles (which
// infers partition values from parquet statistics and cannot handle non-linear
// transforms like bucket), this builds DataFile objects directly and stages them
// with Transaction.AddDataFiles. It mirrors appendFiles' idempotency (via the
// flow_checkpoints_v1 property) and retry behavior.
func (c *catalog) appendDataFiles(
	ctx context.Context,
	materialization string,
	tablePath []string,
	files []fileEntry,
	cols []partitionColumn,
	prevCheckpoint string,
	nextCheckpoint string,
) error {
	fqn := pathToFQN(tablePath)

	logger := log.WithFields(log.Fields{
		"table":           fqn,
		"materialization": materialization,
		"prev_checkpoint": prevCheckpoint,
		"next_checkpoint": nextCheckpoint,
		"num_files":       len(files),
	})
	logger.Info("append_data_files: starting")

	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		retry := func(err error) (bool, error) {
			if attempt >= maxAttempts {
				return false, err
			}
			logger.WithError(err).WithField("attempt", attempt).Warn("append_data_files: retrying")
			if err := sleepCtx(ctx, time.Duration(attempt*2)*time.Second); err != nil {
				return false, err
			}
			return true, nil
		}

		tbl, err := c.cat.LoadTable(ctx, tablePath)
		if err != nil {
			if ok, err := retry(fmt.Errorf("loading table %q: %w", fqn, err)); ok {
				continue
			} else {
				return err
			}
		}

		checkpoints := map[string]string{}
		if raw, ok := tbl.Properties()[flowCheckpointsKey]; ok && raw != "" {
			if err := json.Unmarshal([]byte(raw), &checkpoints); err != nil {
				return fmt.Errorf("parsing %s on %q: %w", flowCheckpointsKey, fqn, err)
			}
		}

		if checkpoints[materialization] == nextCheckpoint {
			logger.WithField("attempt", attempt).Info("append_data_files: already at next-checkpoint, skipping")
			return nil
		}

		checkpoints[materialization] = nextCheckpoint
		newProps, err := json.Marshal(checkpoints)
		if err != nil {
			return fmt.Errorf("marshaling checkpoints: %w", err)
		}

		spec := tbl.Spec()
		logicalTypes := partitionLogicalTypes(cols)
		dataFiles := make([]iceberg.DataFile, 0, len(files))
		for _, fe := range files {
			fieldData, err := fe.partitionData(cols)
			if err != nil {
				return fmt.Errorf("decoding partition data for %q: %w", fe.S3Path, err)
			}
			b, err := iceberg.NewDataFileBuilder(
				spec,
				iceberg.EntryContentData,
				fe.S3Path,
				iceberg.ParquetFile,
				fieldData,
				logicalTypes,
				nil, // fieldIDToFixedSize
				fe.RecordCount,
				fe.FileSize,
			)
			if err != nil {
				return fmt.Errorf("building data file for %q: %w", fe.S3Path, err)
			}
			dataFiles = append(dataFiles, b.Build())
		}

		tx := tbl.NewTransaction()
		if err := tx.AddDataFiles(ctx, dataFiles, nil); err != nil {
			if ok, err := retry(fmt.Errorf("staging data files for %q: %w", fqn, err)); ok {
				continue
			} else {
				return err
			}
		}
		if err := tx.SetProperties(iceberg.Properties{flowCheckpointsKey: string(newProps)}); err != nil {
			return fmt.Errorf("setting %s on %q: %w", flowCheckpointsKey, fqn, err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			if ok, err := retry(fmt.Errorf("committing append on %q: %w", fqn, err)); ok {
				continue
			} else {
				return err
			}
		}

		logger.WithField("attempt", attempt).Info("append_data_files: committed")
		return nil
	}
}

func (c *catalog) appendFiles(
	ctx context.Context,
	materialization string,
	tablePath []string,
	filePaths []string,
	prevCheckpoint string,
	nextCheckpoint string,
) error {
	fqn := pathToFQN(tablePath)

	logger := log.WithFields(log.Fields{
		"table":           fqn,
		"materialization": materialization,
		"prev_checkpoint": prevCheckpoint,
		"next_checkpoint": nextCheckpoint,
		"num_files":       len(filePaths),
	})
	logger.Info("append_files: starting")

	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		tbl, err := c.cat.LoadTable(ctx, tablePath)
		if err != nil {
			err = fmt.Errorf("loading table %q: %w", fqn, err)
			if attempt >= maxAttempts {
				return err
			}
			logger.WithError(err).WithField("attempt", attempt).Warn("append_files: retrying")
			if err := sleepCtx(ctx, time.Duration(attempt*2)*time.Second); err != nil {
				return err
			}
			continue
		}

		checkpoints := map[string]string{}
		if raw, ok := tbl.Properties()[flowCheckpointsKey]; ok && raw != "" {
			if err := json.Unmarshal([]byte(raw), &checkpoints); err != nil {
				return fmt.Errorf("parsing %s on %q: %w", flowCheckpointsKey, fqn, err)
			}
		}

		if checkpoints[materialization] == nextCheckpoint {
			logger.WithField("attempt", attempt).Info("append_files: already at next-checkpoint, skipping")
			return nil
		}

		checkpoints[materialization] = nextCheckpoint
		newProps, err := json.Marshal(checkpoints)
		if err != nil {
			return fmt.Errorf("marshaling checkpoints: %w", err)
		}

		tx := tbl.NewTransaction()
		// ignoreDuplicates=false preserves the pyiceberg default
		// (check_duplicate_files): a file already referenced by the table
		// fails the append rather than being silently re-added, which guards
		// against a zombie process double-appending the same paths in the
		// window between the checkpoint-fence read and commit.
		if err := tx.AddFiles(ctx, filePaths, nil, false); err != nil {
			err = fmt.Errorf("staging files for %q: %w", fqn, err)
			if attempt >= maxAttempts {
				return err
			}
			logger.WithError(err).WithField("attempt", attempt).Warn("append_files: retrying")
			if err := sleepCtx(ctx, time.Duration(attempt*2)*time.Second); err != nil {
				return err
			}
			continue
		}
		if err := tx.SetProperties(iceberg.Properties{flowCheckpointsKey: string(newProps)}); err != nil {
			return fmt.Errorf("setting %s on %q: %w", flowCheckpointsKey, fqn, err)
		}
		if _, err := tx.Commit(ctx); err != nil {
			err = fmt.Errorf("committing append on %q: %w", fqn, err)
			if attempt >= maxAttempts {
				return err
			}
			logger.WithError(err).WithField("attempt", attempt).Warn("append_files: retrying")
			if err := sleepCtx(ctx, time.Duration(attempt*2)*time.Second); err != nil {
				return err
			}
			continue
		}

		logger.WithField("attempt", attempt).Info("append_files: committed")
		return nil
	}
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}
