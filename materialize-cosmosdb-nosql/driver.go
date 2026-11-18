package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sort"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type driver struct {
	clientOptions *azcosmos.ClientOptions
}

var _ boilerplate.Connector = &driver{}

func NewDriver() boilerplate.Connector { return driver{} }

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	configSchema, err := schemagen.GenerateSchema("Materialize Cosmos DB for NoSQL Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating config schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("Cosmos DB for NoSQL Container", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-cosmosdb-nosql", configSchema, resourceSchema)
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	type expectedKey struct {
		res      resource
		pk       string
		backfill uint32
	}
	var expected []expectedKey
	var standardUpdates bool
	for _, b := range req.Bindings {
		var res resource
		if err := boilerplate.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, err
		}
		pk, err := res.partitionKey(b.Collection.Key)
		if err != nil {
			return nil, fmt.Errorf("container %q: %w", res.Container, err)
		}
		for _, p := range b.Collection.Projections {
			if p.Ptr == pk && pk != "/id" && p.Inference.Exists != pf.Inference_MUST {
				return nil, fmt.Errorf("container %q: partition key %q must be present in every document; key defaults are not inserted into the stored document. Choose a required collection key pointer as partition_key", res.Container, pk)
			}
		}
		expected = append(expected, expectedKey{res: res, pk: pk, backfill: b.Backfill})
		standardUpdates = standardUpdates || !res.DeltaUpdates
	}

	return boilerplate.RunValidate(ctx, req, func(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
		mat, err := d.newMaterializer(ctx, name, cfg, flags)
		if err != nil {
			return nil, err
		}
		// A backfilled binding's container is dropped and recreated with its
		// new partition key on Apply, so only existing containers that will be
		// kept must already match.
		partitionKeys := make(map[string]string)
		for _, e := range expected {
			path, _, err := e.res.WithDefaults(cfg).Parameters()
			if err != nil {
				return nil, err
			}
			if last := boilerplate.FindLastBinding(path, req.LastMaterialization); last != nil && e.backfill > last.Backfill {
				continue
			}
			partitionKeys[e.res.Container] = e.pk
		}
		mat.(*materialization).partitionKeys = partitionKeys
		mat.(*materialization).standardUpdates = standardUpdates
		return mat, nil
	})
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, d.newMaterializer)
}

func (d driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, d.newMaterializer)
}

type materialization struct {
	cfg           config
	client        *azcosmos.Client
	clientOptions *azcosmos.ClientOptions

	// Validate supplies the expected partition paths before inspecting containers,
	// and whether any binding uses standard updates.
	partitionKeys   map[string]string
	standardUpdates bool
}

// containerMeta describes an existing container in the info schema.
type containerMeta struct {
	partitionKey azcosmos.PartitionKeyDefinition
	resourceID   string
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func NewMaterializer(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	return driver{}.newMaterializer(ctx, name, cfg, flags)
}

func (d driver) newMaterializer(ctx context.Context, name string, cfg config, flags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	client, err := cfg.client(d.clientOptions)
	if err != nil {
		return nil, err
	}
	return &materialization{cfg: cfg, client: client, clientOptions: d.clientOptions}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		ConcurrentApply:     true,
		NoTruncateResources: true,
		SerPolicy:           boilerplate.SerPolicyDisabled,
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	db, err := d.client.NewDatabase(d.cfg.Database)
	if err != nil {
		return err
	}
	if _, err := db.Read(ctx, nil); isStatus(err, http.StatusNotFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("reading database %q: %w", d.cfg.Database, err)
	}
	is.PushNamespace(d.cfg.Database)

	for _, p := range resourcePaths {
		container, err := d.client.NewContainer(p[0], p[1])
		if err != nil {
			return err
		}
		resp, err := container.Read(ctx, nil)
		if isStatus(err, http.StatusNotFound) {
			continue
		} else if err != nil {
			return fmt.Errorf("reading container %q: %w", p[1], err)
		}
		pk := resp.ContainerProperties.PartitionKeyDefinition
		if expected, ok := d.partitionKeys[p[1]]; ok {
			if err := checkPartitionKey(p[1], expected, pk); err != nil {
				return err
			}
		}
		is.PushResource(p...).Meta = containerMeta{partitionKey: pk, resourceID: resp.ContainerProperties.ResourceID}
	}
	return nil
}

func checkPartitionKey(container, expected string, actual azcosmos.PartitionKeyDefinition) error {
	var err error
	if expected, err = cosmosPartitionPath(expected); err != nil {
		return err
	}
	var matches bool
	if len(actual.Paths) == 1 {
		path, err := normalizeCosmosPartitionPath(actual.Paths[0])
		matches = err == nil && path == expected
	}
	if actual.Kind == azcosmos.PartitionKeyKindMultiHash || !matches {
		return fmt.Errorf("container %q has partition key paths %v (kind %s), expected a single partition key at %q; backfill the binding to recreate the container, or delete or rename the existing container if this materialization did not create it", container, actual.Paths, actual.Kind, expected)
	}
	return nil
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}
	if d.standardUpdates {
		if err := d.checkConsistency(ctx); err != nil {
			errs.Err(err)
		}
	}
	return errs
}

// checkConsistency requires an account consistency level at which standard
// updates read their own writes. Weaker levels ignore session tokens, and a
// request can only relax the account's default consistency.
func (d *materialization) checkConsistency(ctx context.Context) error {
	acct, err := d.cfg.accountProperties(ctx, d.clientOptions)
	if err != nil {
		return fmt.Errorf("reading account consistency level: %w", err)
	}
	switch acct.consistency {
	case azcosmos.ConsistencyLevelEventual, azcosmos.ConsistencyLevelConsistentPrefix:
		if acct.isEmulator(d.cfg.Endpoint) {
			log.WithField("consistency", acct.consistency).Warn("skipping the account consistency check on the Cosmos DB emulator")
			return nil
		}
		return fmt.Errorf("the account's default consistency level is %s, but standard updates require Session, Bounded Staleness, or Strong consistency to read their own writes. Change the account's default consistency in Azure, or use delta updates for every binding", acct.consistency)
	}
	return nil
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	var constraint pm.Response_Validated_Constraint
	switch {
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Document keys must be included"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cosmos DB only materializes the full document"
	}
	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mappedType{}, nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) NewTransactor(ctx context.Context, req pm.Request_Open, is boilerplate.InfoSchema, mappedBindings []boilerplate.MappedBinding[config, resource, mappedType], be *m.BindingEvents) (m.Transactor, error) {
	if err := boilerplate.RequireSingleShard(req); err != nil {
		return nil, err
	}

	// Apply builds a transactor from the last spec only to drain its state, so
	// a failed consistency check blocks standard-updates bindings from Load and
	// Store rather than failing the switch to delta updates.
	var consistencyErr error
	if slices.ContainsFunc(mappedBindings, func(b boilerplate.MappedBinding[config, resource, mappedType]) bool { return !b.DeltaUpdates }) {
		consistencyErr = d.checkConsistency(ctx)
	}

	var bindings []binding
	for _, b := range mappedBindings {
		pk, err := b.Config.partitionKey(b.Collection.Key)
		if err != nil {
			return nil, err
		}
		container, err := d.client.NewContainer(b.ResourcePath[0], b.ResourcePath[1])
		if err != nil {
			return nil, err
		}
		// Apply drains pending state with the last spec's bindings, whose
		// containers may be absent from the new spec or recreated with another
		// partition key. Such a binding fails only if it is asked to Load or Store.
		var meta containerMeta
		var unusable error
		if existing := is.GetResource(b.ResourcePath); existing == nil {
			unusable = fmt.Errorf("container %q does not exist", b.ResourcePath[1])
		} else {
			meta = existing.Meta.(containerMeta)
			unusable = checkPartitionKey(b.Config.Container, pk, meta.partitionKey)
		}
		if unusable == nil && !b.DeltaUpdates {
			unusable = consistencyErr
		}
		bindings = append(bindings, binding{
			container:         container,
			path:              b.ResourcePath,
			stateKey:          b.StateKey,
			containerRID:      meta.resourceID,
			unusable:          unusable,
			deltaUpdates:      b.DeltaUpdates,
			partitionKey:      pk,
			partitionKeyIndex: slices.Index(b.Collection.Key, pk),
			sessionTokens:     make(map[string]string),
		})
	}
	return &transactor{cfg: d.cfg, client: d.client, bindings: bindings, be: be}, nil
}

func (d *materialization) ListTestTasks(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (d *materialization) CleanupTestTask(ctx context.Context, taskName string) error {
	return nil
}

func (d *materialization) SnapshotTestResource(ctx context.Context, path []string) ([]string, [][]any, error) {
	container, err := d.client.NewContainer(path[0], path[1])
	if err != nil {
		return nil, nil, err
	}

	var docs []map[string]json.RawMessage
	columnsSet := make(map[string]struct{})
	pager := container.NewQueryItemsPager("SELECT * FROM c", azcosmos.NewPartitionKey(), nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("querying container %q: %w", path[1], err)
		}
		for _, item := range page.Items {
			var doc map[string]json.RawMessage
			if err := json.Unmarshal(item, &doc); err != nil {
				return nil, nil, fmt.Errorf("decoding item: %w", err)
			}
			for _, field := range systemFields {
				delete(doc, field)
			}
			for field := range doc {
				columnsSet[field] = struct{}{}
			}
			docs = append(docs, doc)
		}
	}

	columns := make([]string, 0, len(columnsSet))
	for field := range columnsSet {
		columns = append(columns, field)
	}
	sort.Strings(columns)

	// Keep values as JSON so snapshots preserve large integers and nested values.
	var rows [][]any
	for _, doc := range docs {
		row := make([]any, len(columns))
		for i, field := range columns {
			if value, ok := doc[field]; ok {
				row[i] = value
			}
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		for col := range columns {
			left, right := fmt.Sprint(rows[i][col]), fmt.Sprint(rows[j][col])
			if left != right {
				return left < right
			}
		}
		return false
	})
	return columns, rows, nil
}

func (d *materialization) Close(ctx context.Context) {
	d.client.Close()
}
