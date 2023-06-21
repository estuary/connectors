package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	mongoDriver "go.mongodb.org/mongo-driver/x/mongo/driver"
)

type resource struct {
	Database   string `json:"database" jsonschema:"title=Database name"`
	Collection string `json:"collection" jsonschema:"title=Collection name"`
}

func (r resource) Validate() error {
	if r.Collection == "" {
		return fmt.Errorf("collection is required")
	}
	return nil
}

// config represents the endpoint configuration for mongodb
type config struct {
	Address  string `json:"address" jsonschema:"title=Address" jsonschema_description:"The connection URI for your database without the username and password. For example mongodb://my-mongo.test?authSource=admin." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database" jsonschema:"title=Database,description=Name of the database to capture from." jsonschema_extras:"order=3"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	var uri, err = url.Parse(c.Address)
	// mongodb+srv:// urls do not support port
	if err == nil && uri.Scheme == "mongodb+srv" && uri.Port() != "" {
		return fmt.Errorf("`mongodb+srv://` addresses do not support specifying the port")
	}

	return nil
}

// ToURI converts the Config to a DSN string.
func (c *config) ToURI() string {
	var address = c.Address
	var uri, err = url.Parse(address)

	if err != nil || uri.Scheme == "" || uri.Host == "" {
		uri = &url.URL{
			Scheme: "mongodb",
			Host:   address,
		}
	}

	uri.User = url.UserPassword(c.User, c.Password)

	if c.Database != "" {
		uri.Path = "/" + c.Database
	}
	return uri.String()
}

type driver struct{}

func (d *driver) Connect(ctx context.Context, cfg config) (*mongo.Client, error) {
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

func (driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var endpointSchema, err = schemagen.GenerateSchema("MongoDB", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("MongoDB Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/source-mongodb",
	}, nil
}

// Validate that store resources and proposed collection bindings are compatible.
func (d *driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config json: %w", err)
	}

	var client, err = d.Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to database: %w", err)
	}

	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	existingDatabases, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("getting list of databases: %w", err)
	}

	var bindings = []*pc.Response_Validated_Binding{}

	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		if !slices.Contains(existingDatabases, res.Database) {
			return nil, fmt.Errorf("database %s does not exist", res.Database)
		}

		var db = client.Database(res.Database)

		collections, err := db.ListCollectionNames(ctx, bson.D{{"name", res.Collection}})
		if err != nil {
			return nil, fmt.Errorf("listing collections in database %s: %w", db.Name(), err)
		}

		if !slices.Contains(collections, res.Collection) {
			return nil, fmt.Errorf("could not find collection %s in database %s", res.Collection, db.Name())
		}

		// Ensure a change stream can be initialized on each collection. This verifies that
		// ReplicaSet is enabled on the database.
		for _, name := range collections {
			cursor, err := db.Collection(name).Watch(ctx, mongo.Pipeline{})
			if err != nil {
				if e, ok := err.(mongo.ServerError); ok {
					if e.HasErrorMessage("The $changeStream stage is only supported on replica sets") {
						return nil, cerrors.NewUserError(err, fmt.Sprintf("unable to verify that a change stream can be created for collection %s: ReplicaSet is not enabled on your database", name))
					}
				}

				return nil, fmt.Errorf("creating change stream for collection %s: %w", name, err)
			}

			cursor.Close(ctx)
		}

		bindings = append(bindings, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Database, res.Collection},
		})
	}

	return &pc.Response_Validated{Bindings: bindings}, nil
}

// ApplyUpsert applies a new or updated capture to the store.
func (d *driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

func main() {
	boilerplate.RunMain(new(driver))
}
