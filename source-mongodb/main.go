package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/schedule"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	mongoDriver "go.mongodb.org/mongo-driver/x/mongo/driver"
)

// Downloads TLS certificates for connecting to AWS DocumentDB. This is used in
// `documentDBTLSConfig()` if the configured address is for AWS DocumentDB.
//go:generate wget -O global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

//go:embed global-bundle.pem
var documentDBTLSCerts embed.FS

type captureMode string

const (
	captureModeChangeStream captureMode = "Change Stream Incremental"
	captureModeSnapshot     captureMode = "Batch Snapshot"
	captureModeIncremental  captureMode = "Batch Incremental"
)

func (m captureMode) validate() error {
	switch m {
	case captureModeChangeStream, captureModeSnapshot, captureModeIncremental:
		return nil
	default:
		return fmt.Errorf("invalid capture mode: %s", m)
	}
}

type resource struct {
	Database     string      `json:"database" jsonschema:"title=Database name" jsonschema_extras:"order=0"`
	Collection   string      `json:"collection" jsonschema:"title=Collection name" jsonschema_extras:"order=1"`
	Mode         captureMode `json:"captureMode,omitempty" jsonschema:"title=Capture Mode,enum=Change Stream Incremental,enum=Batch Snapshot,enum=Batch Incremental" jsonschema_extras:"order=2"`
	Cursor       string      `json:"cursorField,omitempty" jsonschema:"title=Cursor Field,description=The name of the field to use as a cursor for batch-mode bindings. For best performance this field should be indexed. When used with 'Batch Incremental' mode documents added to the collection are expected to always have the cursor field and for it to be strictly increasing." jsonschema_extras:"order=3"`
	PollSchedule string      `json:"pollSchedule,omitempty" jsonschema:"title=Polling Schedule,description=When and how often to poll batch collections (overrides the connector default setting). Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$,order=4"`
}

func (r resource) Validate() error {
	if r.Collection == "" {
		return fmt.Errorf("collection is required")
	}
	if r.Database == "" {
		return fmt.Errorf("database is required")
	}

	if r.PollSchedule != "" {
		if err := schedule.Validate(r.PollSchedule); err != nil {
			return fmt.Errorf("invalid polling schedule '%s' for '%s.%s': %w", r.PollSchedule, r.Database, r.Collection, err)
		}
	}

	if r.Mode != "" {
		if err := r.Mode.validate(); err != nil {
			return fmt.Errorf("validating capture mode for '%s.%s': %w", r.Database, r.Collection, err)
		}
		if r.Mode == captureModeIncremental || r.Mode == captureModeSnapshot {
			if r.Cursor == "" {
				return fmt.Errorf("cursor field is required for '%s.%s' with mode '%s'", r.Database, r.Collection, r.Mode)
			}
		}
	}

	return nil
}

func (r resource) getMode() captureMode {
	if r.Mode != "" {
		return r.Mode
	}
	return captureModeChangeStream
}

func (r resource) getCursorField() string {
	if r.Cursor != "" {
		return r.Cursor
	}
	return idProperty
}

type sshForwarding struct {
	SSHEndpoint string `json:"sshEndpoint" jsonschema:"title=SSH Endpoint,description=Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])" jsonschema_extras:"pattern=^ssh://.+@.+$"`
	PrivateKey  string `json:"privateKey" jsonschema:"title=SSH Private Key,description=Private key to connect to the remote SSH server." jsonschema_extras:"secret=true,multiline=true"`
}

type tunnelConfig struct {
	SSHForwarding *sshForwarding `json:"sshForwarding,omitempty" jsonschema:"title=SSH Forwarding"`
}

type config struct {
	Address              string `json:"address" jsonschema:"title=Address" jsonschema_description:"The connection URI for your database without the username and password. For example mongodb://my-mongo.test?authSource=admin." jsonschema_extras:"order=0"`
	User                 string `json:"user" jsonschema:"title=User,description=Database user to connect as." jsonschema_extras:"order=1"`
	Password             string `json:"password" jsonschema:"title=Password,description=Password for the specified database user." jsonschema_extras:"secret=true,order=2"`
	Database             string `json:"database,omitempty" jsonschema:"title=Database,description=Optional comma-separated list of the databases to discover. If not provided will discover all available databases in the deployment." jsonschema_extras:"order=3"`
	BatchAndChangeStream bool   `json:"batchAndChangeStream,omitempty" jsonschema:"title=Capture Batch Collections in Addition to Change Stream Collections,description=Discover collections that can only be batch captured if the deployment supports change streams. Check this box to capture views and time series collections as well as change streams. All collections will be captured in batch mode if the server does not support change streams regardless of this setting." jsonschema_extras:"order=4"`
	PollSchedule         string `json:"pollSchedule,omitempty" jsonschema:"title=Default Batch Collection Polling Schedule,description=When and how often to poll batch collections. Accepts a Go duration string like '5m' or '6h' for frequency-based polling or a string like 'daily at 12:34Z' to poll at a specific time (specified in UTC) every day. Defaults to '24h' if unset." jsonschema_extras:"pattern=^([-+]?([0-9]+([.][0-9]+)?(h|m|s|ms))+|daily at [0-9][0-9]?:[0-9]{2}Z)$,order=5"`

	NetworkTunnel *tunnelConfig  `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network." jsonschema_extras:"order=6"`
	Advanced      advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true,order=7"`
}

type advancedConfig struct {
	MaxAwaitTime              string `json:"maxAwaitTime,omitempty" jsonschema:"title=Max Await Time,description=Maximum time to wait for new change stream events before returning an empty batch. Defaults to 1 second. Accepts a Go duration string like '10s'."`
	DisablePreImages          bool   `json:"disablePreImages,omitempty" jsonschema:"title=Disable Pre-Images,description=Disable requesting pre-images even if the MongoDB deployment supports them and they are enabled for collections."`
	ExclusiveCollectionFilter bool   `json:"exclusiveCollectionFilter,omitempty" jsonschema:"title=Change Stream Exclusive Collection Filter,description=Add a MongoDB pipeline filter to database change streams to exclusively match events having enabled capture bindings. Should only be used if a small number of bindings are enabled."`
	ExcludeCollections        string `json:"excludeCollections,omitempty" jsonschema:"title=Exclude Collections,description=Comma-separated list of collections to exclude from database change streams. Each one should be formatted as 'database_name:collection'. Cannot be set if exclusiveCollectionFilter is enabled."`
}

func parseExcludeCollections(excludeCollections string) (map[string][]string, error) {
	out := make(map[string][]string)

	if excludeCollections == "" {
		return out, nil
	}

	for _, collection := range strings.Split(excludeCollections, ",") {
		parts := strings.Split(collection, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid exclude collection: %s", collection)
		}
		out[parts[0]] = append(out[parts[0]], parts[1])
	}

	log.WithField("excludeCollections", excludeCollections).Info("parsed excludeCollections")

	return out, nil
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

	if c.PollSchedule != "" {
		if err := schedule.Validate(c.PollSchedule); err != nil {
			return fmt.Errorf("invalid default polling schedule %q: %w", c.PollSchedule, err)
		}
	}

	if c.Advanced.ExcludeCollections != "" && c.Advanced.ExclusiveCollectionFilter {
		return fmt.Errorf("cannot set both excludeCollections and exclusiveCollectionFilter")
	}

	if c.Advanced.MaxAwaitTime != "" {
		if parsed, err := time.ParseDuration(c.Advanced.MaxAwaitTime); err != nil {
			return fmt.Errorf("invalid maxAwaitTime: %w", err)
		} else if parsed <= 0 {
			return fmt.Errorf("maxAwaitTime must be greater than 0, got %s", parsed.String())
		}
	}

	if _, err := parseExcludeCollections(c.Advanced.ExcludeCollections); err != nil {
		return err
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

func isDocumentDB(address string) (bool, error) {
	uri, err := url.Parse(address)
	if err != nil {
		return false, fmt.Errorf("parsing address: %w", err)
	}

	return strings.HasSuffix(uri.Hostname(), ".docdb.amazonaws.com"), nil
}

func (d *driver) Connect(ctx context.Context, cfg config) (*mongo.Client, error) {
	// Mongodb atlas offers an "online-archive" service, which does not work
	// with this connector because it doesn't support change streams. Their UI
	// makes it really easy to grab the wrong URL, so this check exists to provide
	// a more helpful error message in the event that someone tries to use the
	// online-archive url instead of the regular mongodb url.
	if strings.Contains(cfg.Address, "atlas-online-archive") {
		return nil, fmt.Errorf("the provided URL appears to be for 'Atlas online archive', which is not supported by this connector. Please use the regular cluster URL instead")
	}

	// Similarly, MongoDB "Atlas SQL" does not support change streams, so our
	// connector doesn't work with that either.
	if strings.HasPrefix(cfg.Address, "mongodb://atlas-sql") {
		return nil, fmt.Errorf("the provided URL appears to be for 'Atlas SQL Interface', which is not supported by this connector. Please use the regular cluster URL instead")
	}

	isDocDB, err := isDocumentDB(cfg.Address)
	if err != nil {
		return nil, err
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
			return nil, err
		}
	} else if isDocDB {
		return nil, fmt.Errorf("the provided address %q appears to be for Amazon DocumentDB, which requires an SSH tunnel configuration", cfg.Address)
	}

	poolMonitor := &event.PoolMonitor{
		Event: func(evt *event.PoolEvent) {
			switch evt.Type {
			case event.GetStarted, event.GetSucceeded, event.ConnectionReturned:
				// These are for connection checkout start, connection checked
				// out, and connection checked in, which will happen frequently
				// during the course of normal connector operation as
				// connections are checked out and returned to the pool.
				return
			}

			log.WithField("event", evt).Debug("mongodb connection pool event")
		},
	}

	// Create a new client and connect to the server. "Majority" read concern is set to avoid
	// reading data during backfills that may be rolled back in uncommon situations. This matches
	// the behavior of change streams, which only represent data that has been majority committed.
	// This read concern will overwrite any that is set in the connection string parameter
	// "readConcernLevel".
	//
	// Snappy compression is used since it is widely supported and quite a bit faster than zlib.
	// zstd would be even better, but as of right now it causes the connector to use an excessive
	// amount of memory.
	var opts = options.Client().ApplyURI(cfg.ToURI()).SetCompressors([]string{"snappy"}).SetReadConcern(readconcern.Majority()).SetPoolMonitor(poolMonitor)
	if isDocDB {
		tlsConfig, err := documentDBTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("tlsConfig for documentDB: %w", err)
		}
		// In addition to the TLS configuration, DocumentDB doesn't support
		// connecting in replica set mode through an SSH tunnel, so the
		// directConnection query parameter is required.
		opts = opts.SetTLSConfig(tlsConfig).SetDirect(true)
	}

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

func documentDBTLSConfig() (*tls.Config, error) {
	var tlsConfig = new(tls.Config)
	tlsConfig.InsecureSkipVerify = true // for connecting to `localhost` via SSH tunnel
	tlsConfig.RootCAs = x509.NewCertPool()

	if certs, err := documentDBTLSCerts.ReadFile("global-bundle.pem"); err != nil {
		return nil, fmt.Errorf("failed to open global-bundle.pem: %w", err)
	} else if ok := tlsConfig.RootCAs.AppendCertsFromPEM(certs); !ok {
		return nil, fmt.Errorf("failed parsing pem file")
	}

	return tlsConfig, nil
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

	existingDatabases, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("getting list of databases: %w", err)
	}

	var lastResources []resource
	var lastBackfillCounters []uint32
	if req.LastCapture != nil {
		for _, lastBinding := range req.LastCapture.Bindings {
			var res resource
			if err := pf.UnmarshalStrict(lastBinding.ResourceConfigJson, &res); err != nil {
				return nil, fmt.Errorf("error parsing resource config for lastBinding: %w", err)
			}
			lastResources = append(lastResources, res)
			lastBackfillCounters = append(lastBackfillCounters, lastBinding.Backfill)
		}
	}

	var dbCollectionSpecs = make(map[string][]*mongo.CollectionSpecification)
	var bindings = []*pc.Response_Validated_Binding{}
	var servInf *serverInfo

	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		if servInf == nil {
			servInf, err = getServerInfo(ctx, cfg, client, res.Database)
			if err != nil {
				return nil, fmt.Errorf("getting server info: %w", err)
			}
		}

		var resourcePath = []string{res.Database, res.Collection}

		// Validate changes to the resource spec.
		for idx, lastResource := range lastResources {
			if lastResource.Database == res.Database && lastResource.Collection == res.Collection {
				// Changing the capture mode is not allowed, although
				// theoretically going from change stream to a snapshot could
				// work. It's just easier to not allow it for now unless the
				// binding is being reset (backfill counter incremented) as
				// well.
				if lastResource.getMode() != res.getMode() && binding.Backfill == lastBackfillCounters[idx] {
					return nil, fmt.Errorf("cannot change mode from %s to %s for binding %q without backfilling the binding", lastResource.getMode(), res.getMode(), resourcePath)
				}

				// Change the cursor field is not allowed for batch bindings.
				if lastResource.getMode() != captureModeChangeStream {
					if lastResource.getCursorField() != res.getCursorField() && binding.Backfill == lastBackfillCounters[idx] {
						return nil, fmt.Errorf("cannot change cursor field from %s to %s for binding %q without backfilling the binding", lastResource.getCursorField(), res.getCursorField(), resourcePath)
					}
				}
			}
		}

		if !slices.Contains(existingDatabases, res.Database) {
			return nil, fmt.Errorf("database %s does not exist", res.Database)
		}

		var db = res.Database
		if _, ok := dbCollectionSpecs[db]; !ok {
			specs, err := client.Database(db).ListCollectionSpecifications(ctx, bson.D{})
			if err != nil {
				return nil, fmt.Errorf("listing collections in database %s: %w", db, err)
			}
			dbCollectionSpecs[db] = specs
		}
		specs := dbCollectionSpecs[db]

		specIdx := slices.IndexFunc(specs, func(spec *mongo.CollectionSpecification) bool {
			return spec.Name == res.Collection
		})
		if specIdx == -1 {
			return nil, fmt.Errorf("could not find collection %s in database %s", res.Collection, db)
		}
		collection := specs[specIdx]
		collectionType := mongoCollectionType(collection.Type)
		if err := collectionType.validate(); err != nil {
			return nil, fmt.Errorf("unsupported collection type: %w", err)
		}

		if !servInf.supportsChangeStreams && res.getMode() == captureModeChangeStream {
			return nil, fmt.Errorf("binding %q is configured with mode '%s', but the server does not support change streams", resourcePath, res.getMode())
		} else if res.getMode() == captureModeChangeStream && !collectionType.canChangeStream() {
			return nil, fmt.Errorf("binding %q is configured with mode '%s', but the collection type '%s' does not support change streams", resourcePath, res.getMode(), collectionType)
		}

		bindings = append(bindings, &pc.Response_Validated_Binding{
			ResourcePath: resourcePath,
		})
	}

	excludeCollections, err := parseExcludeCollections(cfg.Advanced.ExcludeCollections)
	if err != nil {
		return nil, fmt.Errorf("invalid excludeCollections: %w", err)
	}

	for db, excludeColls := range excludeCollections {
		for _, coll := range excludeColls {
			rp := []string{db, coll}
			if slices.ContainsFunc(bindings, func(b *pc.Response_Validated_Binding) bool { return slices.Equal(rp, b.ResourcePath) }) {
				return nil, fmt.Errorf("excludeCollections collection %s of database %s is an enabled binding", coll, db)
			}
		}
	}

	return &pc.Response_Validated{Bindings: bindings}, nil
}

func (d *driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

func main() {
	boilerplate.RunMain(new(driver))
}
