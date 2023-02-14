package main

import (
	"context"
	"encoding/json"
	"fmt"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	log "github.com/sirupsen/logrus"
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

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

// config represents the endpoint configuration for postgres.
type config struct {
	Address  string `json:"address" jsonschema:"title=Address,description=Host and port of the database." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database,omitempty" jsonschema:"title=Database,description=Name of the database to capture from." jsonschema_extras:"order=3"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"address", c.Address},
		{"user", c.User},
		{"password", c.Password},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	return nil
}

type driver struct{}

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

// ApplyUpsert applies a new or updated capture to the store.
func (d *driver) ApplyUpsert(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{ActionDescription: ""}, nil
}

// ApplyDelete deletes an existing capture from the store.
func (driver) ApplyDelete(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{ActionDescription: ""}, nil
}

// Discover returns the set of resources available from this Driver.
func (d *driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {

	return &pc.DiscoverResponse{Bindings: []*pc.DiscoverResponse_Binding{}}, nil
}

// Validate that store resources and proposed collection bindings are compatible.
func (d *driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	return &pc.ValidateResponse{Bindings: []*pc.ValidateResponse_Binding{}}, nil
}

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *driver) Pull(stream pc.Driver_PullServer) error {
	log.Debug("connector started")
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("error reading PullRequest: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected PullRequest.Open, got %#v", open)
	}

	//var c = Capture{
		//Bindings: bindings,
		//State:    state,
		//Output:   &boilerplate.PullOutput{Stream: stream},
		//Database: db,
	//}
	//return c.Run(ctx)
  return nil
}

func main() {
	boilerplate.RunMain(new(driver))
}
