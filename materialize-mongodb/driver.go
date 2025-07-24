package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	mongoDriver "go.mongodb.org/mongo-driver/x/mongo/driver"
)

type driver struct{}

var _ boilerplate.Connector = &driver{}

func connect(ctx context.Context, cfg config) (*mongo.Client, error) {
	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections.
	if cfg.NetworkTunnel != nil && cfg.NetworkTunnel.SSHForwarding != nil && cfg.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		uri, err := url.Parse(cfg.Address)
		if err != nil {
			return nil, fmt.Errorf("parsing address for network tunnel: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: cfg.NetworkTunnel.SSHForwarding.SSHEndpoint,
			PrivateKey:  []byte(cfg.NetworkTunnel.SSHForwarding.PrivateKey),
			ForwardHost: uri.Hostname(),
			ForwardPort: uri.Port(),
			LocalPort:   "27017",
		}
		var tunnel = sshConfig.CreateTunnel()

		if err := tunnel.Start(); err != nil {
			return nil, err
		}
	}

	// Create a new client and connect to the server
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cfg.ToURI()))
	if err != nil {
		return nil, err
	}

	// Any error other than an authentication error will result in the call to Ping hanging until it
	// times out due to the way the mongo client handles retries. The flow control plane will cancel
	// any RPC after ~30 seconds, so we'll timeout ahead of that in order to produce a more useful
	// error message.
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	// Ping the primary
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		var mongoErr mongoDriver.Error

		if errors.Is(err, context.DeadlineExceeded) {
			return nil, cerrors.NewUserError(err, fmt.Sprintf("cannot connect to address %q: double check your configuration, and make sure Estuary's IP is allowed to connect to your database", cfg.Address))
		} else if errors.As(err, &mongoErr) {
			if mongoErr.Code == 18 {
				// See https://github.com/mongodb/mongo-go-driver/blob/master/docs/common-issues.md#authentication-failed
				// An auth error can occur for any of these reasons, and the mongo driver gives us no way to tell them apart:
				//   - Wrong username
				//   - Wrong password
				//   - Wrong authentication database, which is specified by the query parameter `authSource`
				//   - User doesn't have access to the requested database
				//   - The requested database doesn't exist
				return nil, cerrors.NewUserError(err, "authentication failed: you may have entered an incorrect username or password, the database may not exist, the user may not have access to the database, or the authSource query parameter may be incorrect")
			}
		}

		return nil, err
	}

	return client, nil
}

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	configSchema, err := schemagen.GenerateSchema("Materialize MongoDB Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating config schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("MongoDB Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-mongodb", configSchema, resourceSchema)
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (d driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg    config
	client *mongo.Client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, mappedType] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, mappedType], error) {
	client, err := connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	return &materialization{
		cfg:    cfg,
		client: client,
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		ConcurrentApply:    true,
		NoCreateNamespaces: true,
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	db := d.client.Database(d.cfg.Database)
	collections, err := db.ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return err
	}

	for _, c := range collections {
		is.PushResource(db.Name(), c.Name)
	}

	return nil
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	return nil
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	// MongoDB always materializes the full root document.
	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Document keys must be included"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "MongoDB only materializes the full document"
	}

	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (mappedType, boilerplate.ElementConverter) {
	return mappedType{}, nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	// No-op since new databases and collections are automatically created when data is added to them.
	return "", nil
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, mappedType]) (string, boilerplate.ActionApplyFn, error) {
	// No-op since new databases and collections are automatically created when data is added to them.
	return "", nil, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	dbName := resourcePath[0]
	collectionName := resourcePath[1]

	return fmt.Sprintf("drop collection %q", collectionName), func(ctx context.Context) error {
		return d.client.Database(dbName).Collection(collectionName).Drop(ctx)
	}, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	panic("not supported")
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, mappedType],
) (string, boilerplate.ActionApplyFn, error) {
	// No-op since nothing in particular is currently configured for a created collection.
	return "", nil, nil
}

func (d *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, mappedType],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	var bindings []*binding
	for _, b := range mappedBindings {
		bindings = append(bindings, &binding{
			collection:   d.client.Database(d.cfg.Database).Collection(b.ResourcePath[1]),
			deltaUpdates: b.DeltaUpdates,
		})
	}

	return &transactor{
		cfg:      &d.cfg,
		client:   d.client,
		bindings: bindings,
	}, nil
}

func (d *materialization) Close(ctx context.Context) {}

func main() {
	boilerplate.RunMain(driver{})
}
