package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
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

const (
	// Minimum oplog time difference: see the comment on OplogTimeDifference in
	// oplog.go
	minOplogTimediffHours   = 24
	minOplogTimediffSeconds = minOplogTimediffHours * 60 * 60 // 24 hours, in seconds
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

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

// config represents the endpoint configuration for mongodb
type config struct {
	Address  string `json:"address" jsonschema:"title=Address" jsonschema_description:"The connection URI for your database without the username and password. For example mongodb://my-mongo.test?authSource=admin." jsonschema_extras:"order=0"`
	User     string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database string `json:"database,omitempty" jsonschema:"title=Database,description=Optional comma-separated list of the databases to discover. If not provided will discover all available databases in the instance." jsonschema_extras:"order=3"`

	// We still don't have any exposed advanced configurations
	Advanced      advancedConfig `json:"advanced" jsonschema:"-"`
	NetworkTunnel *tunnelConfig  `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`
}

type advancedConfig struct {
	// The default value of -5m is useful for production use cases, but when
	// running our test suite we don't want to wait 5 minutes, so we use this
	// configuration in our test suite to disable oplog safety buffer. Note that
	// this is not exposed to users using the `jsonschema:"-"` stanza.
	OplogSafetyBuffer string `json:"oplogSafetyBuffer" jsonschema:"-"`
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
	uri.Path = "/"

	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:27017 to address
	// through the bastion server, so we use the tunnel's address.
	if c.NetworkTunnel != nil && c.NetworkTunnel.SSHForwarding != nil && c.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		uri.Host = "localhost:27017"
	}

	return uri.String()
}

type driver struct{}

func (d *driver) Connect(ctx context.Context, cfg config) (*mongo.Client, error) {
	// Mongodb atlas offers an "online-archive" service, which does not work
	// with this connector because it doesn't support change streams. Their UI
	// makes it really easy to grab the wrong URL, so this check exists to provide
	// a more helpful error message in the event that someone tries to use the
	// online-archive url instead of the regular mongodb url.
	if strings.Contains(cfg.Address, "atlas-online-archive") {
		return nil, fmt.Errorf("The provided URL appears to be for 'Atlas online archive', which is not supported by this connector. Please use the regular cluster URL instead")
	}

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
	var opts = options.Client().ApplyURI(cfg.ToURI()).SetCompressors([]string{"zstd", "zlib", "snappy"})
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}

	// Any error other than an authentication error will result in the call to Ping hanging until it
	// times out due to the way the mongo client handles retries. The flow control plane will pingCancel
	// any RPC after ~30 seconds, so we'll timeout ahead of that in order to produce a more useful
	// error message.
	pingCtx, pingCancel := context.WithTimeout(ctx, 15*time.Second)
	defer pingCancel()

	// Ping the primary
	if err := client.Ping(pingCtx, readpref.Primary()); err != nil {
		var mongoErr mongoDriver.Error
		client.Disconnect(ctx) // ignore error result

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
		ResourcePathPointers:     []string{"/database", "/collection"},
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

	if _, err = checkOplog(ctx, client); err != nil {
		return nil, err
	}

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

		// Ensure a change stream can be initialized on databases. This verifies that
		// ReplicaSet is enabled on the database.
		cursor, err := db.Watch(ctx, mongo.Pipeline{})
		if err != nil {
			if e, ok := err.(mongo.ServerError); ok {
				if e.HasErrorMessage("The $changeStream stage is only supported on replica sets") {
					return nil, cerrors.NewUserError(err, fmt.Sprintf("unable to verify that a change stream can be created for collection %s: ReplicaSet is not enabled on your database", res.Database))
				}
			}
			return nil, fmt.Errorf("creating change stream for database %s: %w", res.Database, err)
		}
		cursor.Close(ctx)

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
