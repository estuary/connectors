package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	mongoDriver "go.mongodb.org/mongo-driver/x/mongo/driver"
)

// driver implements the DriverServer interface.
type driver struct{}

func (d *driver) connect(ctx context.Context, cfg config) (*mongo.Client, error) {
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

	// MongoDB supports arbitrary JSON documents so there are no constraints on
	// any fields and all fields are marked as "recommended"
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

// Apply checks for bindings that have been removed by comparing them with
// previously persisted spec, and deletes the corresponding collection
func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	cfg, err := resolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	err = d.WriteSpec(ctx, cfg, req.Materialization, req.Version)
	if err != nil {
		return nil, fmt.Errorf("writing spec: %w", err)
	}

	return &pm.Response_Applied{
		ActionDescription: "",
	}, nil
}

// logDatabaseOperations periodically tries to get the current operations that are run
// for the current session, and logs them, to aid in troubleshooting.
func logDatabaseOperations(ctx context.Context, client *mongo.Client) {
	var ticker = time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var command = bson.D{{Key: "currentOp", Value: true}, {Key: "$ownOps", Value: true}}
			var resultDoc bson.M // ugh, M is unordered and D is ordered. This is important for $reasons
			var err = client.Database("admin").RunCommand(ctx, command).Decode(&resultDoc)
			if err != nil {
				log.WithFields(log.Fields{
					"error": resultDoc,
				}).Warn("failed to execute currentOp command (will stop trying)")
				return
			} else {
				log.WithFields(log.Fields{
					"currentOps": resultDoc,
				}).Debug("results of currentOp command")
			}
		}
	}
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
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
			collection: collection,
			res:        res,
		})
	}

	var fenceCollection = client.Database(cfg.Database).Collection(fenceCollectionName)

	var materialization = string(open.Materialization.Name)
	var filter = bson.D{{
		"materialization", bson.D{{"$eq", materialization}},
	}}

	var fence fenceRecord
	if err := fenceCollection.FindOne(ctx, filter, options.FindOne()).Decode(&fence); err != nil && err != mongo.ErrNoDocuments {
		return nil, nil, fmt.Errorf("finding existing fence: %w", err)
	} else if err == mongo.ErrNoDocuments {
		fence.Materialization = materialization
		if _, err = fenceCollection.InsertOne(ctx, fence, options.InsertOne()); err != nil {
			return nil, nil, fmt.Errorf("inserting new fence: %w", err)
		}
	}

	var bump = bson.D{{"$inc", bson.D{{"fence", 1}}}}
	var updateOpts = options.FindOneAndUpdate().SetReturnDocument(options.After)
	if err = fenceCollection.FindOneAndUpdate(ctx, filter, bump, updateOpts).Decode(&fence); err != nil {
		return nil, nil, fmt.Errorf("bumping fence: %w", err)
	}

	var cp = new(protocol.Checkpoint)
	if err := cp.Unmarshal(fence.Checkpoint); err != nil {
		return nil, nil, fmt.Errorf("unmarshalling checkpoint, %w", err)
	}

	var debugCancelFunc context.CancelFunc
	var logLevel = os.Getenv("LOG_LEVEL")
	if strings.Contains(logLevel, "debug") || strings.Contains(logLevel, "trace") {
		var dbgCtx context.Context
		dbgCtx, debugCancelFunc = context.WithCancel(ctx)
		go logDatabaseOperations(dbgCtx, client)
	}

	return &transactor{
			materialization: materialization,
			client:          client,
			bindings:        bindings,
			fenceCollection: fenceCollection,
			fence:           &fence,
			debugCancelFunc: debugCancelFunc,
		}, &pm.Response_Opened{
			RuntimeCheckpoint: cp,
		}, nil
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

func main() {
	boilerplate.RunMain(driver{})
}
