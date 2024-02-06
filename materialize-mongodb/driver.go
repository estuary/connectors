package main

import (
	"context"
	"encoding/json"
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

// driver implements the DriverServer interface.
type driver struct{}

func (d *driver) connect(ctx context.Context, cfg config) (*mongo.Client, error) {
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
			return nil, fmt.Errorf("error starting network tunnel: %w", err)
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
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	es := schemagen.GenerateSchema("Materialize MongoDB Spec", &config{})
	endpointSchema, err := es.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("MongoDB Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-mongodb",
		Oauth2:                   nil,
	}, nil
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	cfg, err := resolveEndpointConfig(req.ConfigJson)
	if err != nil {
		return nil, err
	}

	_, err = d.connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	// MongoDB always materializes the full root document.
	var out []*pm.Response_Validated_Binding
	for _, b := range req.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, err
		}

		constraints := make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range b.Collection.Projections {
			var constraint = new(pm.Response_Validated_Constraint)
			switch {
			case projection.IsRootDocumentProjection():
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "The root document must be materialized"
			case projection.IsPrimaryKey:
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
				constraint.Reason = "Document keys must be included"
			default:
				constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
				constraint.Reason = "MongoDB only materializes the full document"
			}
			constraints[projection.Field] = constraint
		}

		resourcePath := []string{cfg.Database, res.Collection}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.DeltaUpdates,
			ResourcePath: resourcePath,
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := d.connect(ctx, cfg)
	if err != nil {
		return nil, err
	}

	is, err := infoSchema(ctx, client.Database(cfg.Database))
	if err != nil {
		return nil, fmt.Errorf("getting infoSchema for apply: %w", err)
	}

	return boilerplate.ApplyChanges(ctx, req, &mongoApplier{
		client: client,
		cfg:    cfg,
	}, is, true)
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (m.Transactor, *pm.Response_Opened, error) {
	var cfg, err = resolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	client, err := d.connect(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to database: %w", err)
	}

	var bindings []*binding
	for _, b := range open.Materialization.Bindings {
		res, err := resolveResourceConfig(b.ResourceConfigJson)
		if err != nil {
			return nil, nil, err
		}
		var collection = client.Database(cfg.Database).Collection(res.Collection)

		bindings = append(bindings, &binding{
			collection:   collection,
			deltaUpdates: b.DeltaUpdates,
		})
	}

	return &transactor{
		client:   client,
		bindings: bindings,
	}, &pm.Response_Opened{}, nil
}

func resolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing MongoDB config: %w", err)
	}

	return cfg, nil
}

func resolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var res = resource{}
	if err := pf.UnmarshalStrict(specJson, &res); err != nil {
		return res, fmt.Errorf("parsing resource config: %w", err)
	}

	return res, nil
}

// infoSchema for MongoDB just includes an empty field for each collection in the configured
// database.
func infoSchema(ctx context.Context, db *mongo.Database) (*boilerplate.InfoSchema, error) {
	is := boilerplate.NewInfoSchema(
		func(rp []string) []string { return rp },
		func(f string) string { return f },
	)

	collections, err := db.ListCollectionSpecifications(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	for _, c := range collections {
		is.PushField(boilerplate.EndpointField{}, db.Name(), c.Name) // NB: ResourcePath includes the database name.
	}

	return is, nil
}

func main() {
	boilerplate.RunMain(driver{})
}
