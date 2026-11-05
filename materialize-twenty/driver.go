package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

// Driver writes collections to Twenty CRM objects through Twenty's REST API. It finds each
// record by a unique field that holds the collection key, and only does delta updates: Twenty
// is the store, and the connector never loads documents back.
type Driver struct{}

var _ boilerplate.Connector = (*Driver)(nil)

func (Driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	endpointSchema, err := schemagen.GenerateSchema("Twenty", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("Twenty Object", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}
	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-twenty",
	}, nil
}

func (Driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	return validate(ctx, req)
}

// validate is Validate after the request is checked to be well formed.
func validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := parseConfig(req.ConfigJson, req.LastMaterialization)
	if err != nil {
		return nil, err
	}
	c, objects, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var resources []resource
	var collections []string
	for _, b := range req.Bindings {
		var res resource
		if err := boilerplate.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		resources = append(resources, res)
		collections = append(collections, b.Collection.Name.String())
	}
	// Bindings are checked against the fields Apply will create, and creates against Twenty.
	real := objects
	objects, creates, err := withOwnFields(objects, resources, collections)
	if err != nil {
		return nil, userError(err)
	}

	if err := checkAdoptAlone(resources); err != nil {
		return nil, userError(err)
	}
	var out []*pm.Response_Validated_Binding
	byIDField := make(map[[2]string]string) // object and ID field -> the collection writing them
	var deleting []string
	for i, b := range req.Bindings {
		res := resources[i]
		if res.deletes() {
			deleting = append(deleting, b.Collection.Name.String())
		}
		obj, err := findObject(objects, res.Object)
		if err != nil {
			return nil, userError(err)
		}
		keys := b.Collection.Key // a runtime that sends no group-by keys by the collection key
		if len(b.GroupBy) > 0 {
			keys = keyPtrs(&b.Collection, b.GroupBy)
		}
		if err := checkIDFieldKept(req.LastMaterialization, b.Collection.Name.String(), res, keys); err != nil {
			return nil, userError(err)
		}
		if err := checkOwnKeys(byIDField, b.Collection.Name.String(), obj, res); err != nil {
			return nil, userError(err)
		}
		links, err := checkRelations(ctx, c, objects, res, obj)
		if err != nil {
			return nil, relationError(err)
		}
		constraints, err := validateBinding(res, obj, links, req.BindingCollection(b))
		if err != nil {
			return nil, userError(err)
		}
		realObj, err := findObject(real, res.Object)
		if err != nil {
			return nil, userError(err)
		}
		if err := checkCreates(creates, req.BindingCollection(b), res, realObj); err != nil {
			return nil, userError(err)
		}
		out = append(out, &pm.Response_Validated_Binding{
			CaseInsensitiveFields: false,
			ProjectionConstraints: boilerplate.ValidatedConstraints(constraints),
			DeltaUpdates:          true,
			ResourcePath:          res.path(),
		})
	}
	if err := checkDeleteVersion(ctx, c, deleting); err != nil {
		return nil, err
	}
	return &pm.Response_Validated{Bindings: out}, nil
}

// Apply creates the fields that bindings list in create_fields and the objects lack, then checks
// that the objects and fields the bindings write exist. It changes nothing else in Twenty: someone
// with access to Twenty's settings creates objects and the other fields.
func (Driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	return apply(ctx, req)
}

// apply is Apply after the request is checked to be well formed.
func apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	cfg, err := parseConfig(req.Materialization.ConfigJson, req.Materialization)
	if err != nil {
		return nil, err
	}
	c, objects, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	// Before changing Twenty, though open checks again.
	resources, err := specResources(req.Materialization)
	if err != nil {
		return nil, err
	}
	if err := checkDeleteVersion(ctx, c, deletingCollections(req.Materialization, resources)); err != nil {
		return nil, err
	}
	created, err := createFields(ctx, c, objects, req.Materialization)
	if err != nil {
		return nil, err
	}
	// open reads the objects again, with the new fields.
	if _, _, err := open(ctx, req.Materialization); err != nil {
		if len(created) > 0 {
			return nil, fmt.Errorf("%w (%s)", err, appliedDescription(created))
		}
		return nil, err
	}
	return &pm.Response_Applied{ActionDescription: appliedDescription(created)}, nil
}

func (Driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("validating request: %w", err)
	}
	c, bindings, err := open(ctx, req.Materialization)
	if err != nil {
		return nil, nil, nil, err
	}
	t := &transactor{client: c, bindings: bindings, be: be, now: time.Now}
	retryCtx, stop := context.WithCancel(ctx) // ends when the connector is told to stop, or in Destroy
	t.stop = stop
	go t.retryLinks(retryCtx)
	return t, &pm.Response_Opened{DisableLoadOptimization: true}, nil, nil
}

func parseConfig(raw json.RawMessage, spec *pf.MaterializationSpec) (config, error) {
	var cfg config
	if err := boilerplate.UnmarshalStrict(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing endpoint config: %w", err)
	}
	if _, err := boilerplate.ResolveFlags(cfg, spec); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// connect makes a client and reads the workspace's objects, which also checks the API key.
func connect(ctx context.Context, cfg config) (*client, []objectMeta, error) {
	c := newClient(cfg.baseURL(), cfg.APIKey, cfg.requestsPerMinute(), 1)
	objects, err := c.objects(ctx)
	var auth *authError
	var rejected *rejectedError
	if errors.As(err, &rejected) && rejected.status == 400 && slices.ContainsFunc(rejected.raw, func(m string) bool {
		return strings.Contains(m, "does not exist. Valid examples")
	}) {
		// Twenty before v2.30 has no limit on its metadata API (local probe of v2.0).
		err = fmt.Errorf("Twenty refused the connector's request for its objects, which Twenty older than v2.30 does. "+
			"The connector needs Twenty v2.30 or later, and v2.35 or later to delete records: %w", err)
		return nil, nil, userError(err)
	} else if errors.As(err, &auth) {
		return nil, nil, userError(err)
	} else if err != nil {
		return nil, nil, fmt.Errorf("reading Twenty's objects: %w", err)
	}
	return c, objects, nil
}

// open connects and maps each binding of a materialization to its Twenty object.
func open(ctx context.Context, spec *pf.MaterializationSpec) (*client, []*binding, error) {
	cfg, err := parseConfig(spec.ConfigJson, spec)
	if err != nil {
		return nil, nil, err
	}
	c, objects, err := connect(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}
	resources, err := specResources(spec)
	if err != nil {
		return nil, nil, err
	}
	var bindings []*binding
	for i, b := range spec.Bindings {
		res := resources[i]
		obj, err := findObject(objects, res.Object)
		if err != nil {
			return nil, nil, userError(err)
		}
		links, err := checkRelations(ctx, c, objects, res, obj)
		if err != nil {
			return nil, nil, relationError(err)
		}
		mapped, err := newBinding(res, obj, links, spec, b)
		if err != nil {
			return nil, nil, userError(err)
		}
		mapped.phoneCountry = cfg.defaultPhoneCountry()
		bindings = append(bindings, mapped)
	}
	if err := checkAdoptAlone(resources); err != nil {
		return nil, nil, userError(err)
	}
	if err := checkDeleteVersion(ctx, c, deletingCollections(spec, resources)); err != nil {
		return nil, nil, err
	}
	return c, bindings, nil
}

// checkAdoptAlone refuses a binding that adopts records of an object another binding writes:
// each could take a record the other is about to adopt or create.
func checkAdoptAlone(resources []resource) error {
	for i, res := range resources {
		if res.AdoptBy == "" {
			continue
		}
		for j, other := range resources {
			if i != j && other.Object == res.Object {
				return fmt.Errorf("%q: the binding for %q finds existing records by %q, so it must be the only binding that writes %q", "adopt_by", res.Object, res.AdoptBy, res.Object)
			}
		}
	}
	return nil
}

// userError marks an error as the user's to fix, with its own message.
func userError(err error) error {
	return cerrors.NewUserError(err, err.Error())
}

// relationError marks a binding's relations that don't match Twenty as the user's to fix. A
// failed request to Twenty's metadata API stays an internal error, unless Twenty refused the key.
func relationError(err error) error {
	var failed *metadataError
	var auth *authError
	if errors.As(err, &failed) && !errors.As(err, &auth) {
		return err
	}
	return userError(err)
}

// keyPtrs returns the document locations of a binding's key fields, in their order, or nil if
// one isn't a projection of the collection.
func keyPtrs(collection *pf.CollectionSpec, fields []string) []string {
	var out []string
	for _, f := range fields {
		p := collection.GetProjection(f)
		if p == nil {
			return nil
		}
		out = append(out, p.Ptr)
	}
	return out
}

// checkOwnKeys refuses a second binding that finds an object's records by the same field as an
// earlier one. The two would share one key space, and each would overwrite the other's records.
// Flow refuses equal paths too, but this also catches an object named both ways, like "person"
// and "people", and says what to do.
func checkOwnKeys(seen map[[2]string]string, collection string, obj *objectMeta, res resource) error {
	at := [2]string{obj.NamePlural, res.IDField}
	if other, ok := seen[at]; ok {
		return fmt.Errorf("the collections %q and %q both write %s found by %q. Give each its own ID field, or union them in a derivation first", other, collection, obj.NamePlural, res.IDField)
	}
	seen[at] = collection
	return nil
}

// checkIDFieldKept refuses to change the id_field of a published binding of a collection. The
// connector finds a document's record by that field, so with a new one it would create every
// record again. A binding's path holds its ID field, so this matches the published bindings by
// their collection and object instead. A collection can write one object twice, by two ID fields,
// so any of them is kept.
//
// The ID field holds the key's text, so a binding can't change its key's locations, or their
// order, either. keys are the locations of the new binding's key fields, nil when unknown.
func checkIDFieldKept(last *pf.MaterializationSpec, collection string, res resource, keys []string) error {
	if last == nil {
		return nil
	}
	var prevIDField string
	for _, b := range last.Bindings {
		if b.Collection.Name.String() != collection {
			continue
		}
		var prev resource
		if err := json.Unmarshal(b.ResourceConfigJson, &prev); err != nil {
			return fmt.Errorf("parsing the published resource config: %w", err)
		}
		if prev.Object != res.Object || prev.IDField == "" {
			continue
		} else if prev.IDField == res.IDField {
			prevKeys := keyPtrs(&b.Collection, b.FieldSelection.Keys)
			if len(keys) > 0 && len(prevKeys) > 0 && !slices.Equal(keys, prevKeys) {
				return fmt.Errorf("the binding for %q writes the key %s to %q, and can't change it to %s: the ID field would hold other text, and the connector would create every record again. To switch, fill the new key's text in %q on the existing records in Twenty, remove the binding and publish, then add it back",
					res.Object, strings.Join(prevKeys, ", "), res.IDField, strings.Join(keys, ", "), res.IDField)
			}
			return nil
		}
		prevIDField = prev.IDField
	}
	if prevIDField != "" {
		return fmt.Errorf("the binding for %q finds records by %q, and can't change to %q: the connector would create every record again. To switch, fill the new field on the existing records in Twenty, remove the binding and publish, then add it back", res.Object, prevIDField, res.IDField)
	}
	return nil
}
