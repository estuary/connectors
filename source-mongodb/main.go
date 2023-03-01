package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)


type resource struct {
	Database   string `json:"database" jsonschema=title=Database name"`
	Collection string `json:"collection" jsonschema:"title=Collection name"`
}

func (r resource) Validate() error {
	if r.Collection == "" {
		return fmt.Errorf("collection is required")
	}
	return nil
}

// config represents the endpoint configuration for postgres.
type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database." jsonschema_extras:"order=0"`
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
		return fmt.Errorf("`mongodb+srv://` addresses do not support specifying the port.")
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

	// Ping the primary
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	return client, nil
}

func (driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var endpointSchema, err = schemagen.GenerateSchema("MongoDB", &config{}).MarshalJSON()
	if err != nil {
		fmt.Println(fmt.Errorf("generating endpoint schema: %w", err))
	}
	resourceSchema, err := schemagen.GenerateSchema("MongoDB Resource Spec", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pc.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://go.estuary.dev/source-mongodb",
	}, nil
}


// Validate that store resources and proposed collection bindings are compatible.
func (d *driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.EndpointSpecJson, &cfg); err != nil {
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

	var bindings = []*pc.ValidateResponse_Binding{}

	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		var db = client.Database(res.Database)

		collections, err := db.ListCollectionNames(ctx, bson.D{{"name", res.Collection}})
		if err != nil {
			return nil, fmt.Errorf("listing collections in database %s: %w", db.Name(), err)
		}

		if !SliceContains(res.Collection, collections) {
			return nil, fmt.Errorf("could not find collection %s in database %s", res.Collection, db.Name())
		}

		bindings = append(bindings, &pc.ValidateResponse_Binding{
			ResourcePath: []string{res.Database, res.Collection},
		})
	}

	return &pc.ValidateResponse{Bindings: bindings}, nil
}

// ApplyUpsert applies a new or updated capture to the store.
func (d *driver) ApplyUpsert(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{ActionDescription: ""}, nil
}

// ApplyDelete deletes an existing capture from the store.
func (driver) ApplyDelete(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{ActionDescription: ""}, nil
}

func main() {
	boilerplate.RunMain(new(driver))
}

func SliceContains(expected string, actual []string) bool {
	for _, ty := range actual {
		if ty == expected {
			return true
		}
	}
	return false
}
