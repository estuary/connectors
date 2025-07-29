package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

var featureFlagDefaults = map[string]bool{}

type sshForwarding struct {
	SshEndpoint string `json:"sshEndpoint"`
	PrivateKey  string `json:"privateKey"`
}

type tunnelConfig struct {
	SshForwarding *sshForwarding `json:"sshForwarding,omitempty"`
}

// credentials is a union representing either an api key or a username/password.
// It's allowed for all credentials to be missing, which is used for connecting to servers
// without authentication enabled.
type credentials struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	ApiKey   string `json:"apiKey,omitempty"`
}

type config struct {
	Credentials credentials `json:"credentials"`
	Endpoint    string      `json:"endpoint"`
	HardDelete  bool        `json:"hardDelete,omitempty"`

	Advanced advancedConfig `json:"advanced,omitempty"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty"`
}

type advancedConfig struct {
	Replicas     *int   `json:"number_of_replicas,omitempty"`
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

// The `go-schema-gen` package doesn't have a good way of dealing with oneOf, and I couldn't get it
// to output a schema that works with Flow's UI. So the endpoint config schema is written out manually
// instead of being generated from the structs.
func configSchema() json.RawMessage {
	var schemaStr = `{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"$id": "https://github.com/estuary/connectors/materialize-elasticsearch/config",
		"properties": {
		  "endpoint": {
			"type": "string",
			"title": "Endpoint",
			"description": "Endpoint host or URL. Must start with http:// or https://. If using Elastic Cloud this follows the format https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT",
			"pattern": "^(http://|https://).+$",
			"order": 0
		  },
		  "hardDelete": {
		    "type": "boolean",
		    "title": "Hard Delete",
		    "description": "If this option is enabled items deleted in the source will also be deleted from the destination. By default is disabled and _meta/op in the destination will signify whether rows have been deleted (soft-delete).",
		    "default": false,
		    "order": 1
		  },
		  "credentials": {
			"type": "object",
			"order": 1,
			"oneOf": [
			  {
				"type": "object",
				"title": "Username and Password",
				"properties": {
				  "username": {
					"type": "string",
					"title": "Username",
					"description": "Username to use with the Elasticsearch API."
				  },
				  "password": {
					"type": "string",
					"secret": true,
					"title": "Password",
					"description": "Password for the user."
				  }
				},
				"required": [
				  "username",
				  "password"
				]
			  },
			  {
				"type": "object",
				"title": "API Key",
				"properties": {
				  "apiKey": {
					"type": "string",
					"secret": true,
					"title": "API Key",
					"description": "API key for authenticating with the Elasticsearch API. Must be the 'encoded' API key credentials, which is the Base64-encoding of the UTF-8 representation of the id and api_key joined by a colon (:)."
				  }
				},
				"required": [
				  "apiKey"
				]
			  }
			]
		  },
		  "advanced": {
			"properties": {
			  "number_of_replicas": {
				"type": "integer",
				"title": "Index Replicas",
				"description": "The number of replicas to create new indexes with. Leave blank to use the cluster default."
			  },
			  "feature_flags": {
				"type": "string",
				"title": "Feature Flags",
				"description": "This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."
			  }
			},
			"type": "object",
			"title": "Advanced Options",
			"description": "Options for advanced users. You should not typically need to modify these.",
			"advanced": true
		  },
		  "networkTunnel": {
			"properties": {
			  "sshForwarding": {
				"properties": {
				  "sshEndpoint": {
					"type": "string",
					"title": "SSH Endpoint",
					"description": "Endpoint of the remote SSH server that supports tunneling (in the form of ssh://user@hostname[:port])",
					"pattern": "^ssh://.+@.+$"
				  },
				  "privateKey": {
					"type": "string",
					"title": "SSH Private Key",
					"description": "Private key to connect to the remote SSH server.",
					"multiline": true,
					"secret": true
				  }
				},
				"additionalProperties": false,
				"type": "object",
				"required": [
				  "sshEndpoint",
				  "privateKey"
				],
				"title": "SSH Forwarding"
			  }
			},
			"additionalProperties": false,
			"type": "object",
			"title": "Network Tunnel",
			"description": "Connect to your system through an SSH server that acts as a bastion host for your network."
		  }
		},
		"type": "object",
		"required": [
		  "endpoint",
		  "credentials"
		],
		"title": "Elasticsearch Connection"
	  }`
	return json.RawMessage([]byte(schemaStr))
}

func (c *credentials) Validate() error {
	if c.ApiKey == "" && c.Username == "" && c.Password == "" {
		return fmt.Errorf("missing credentials: must provide an API Key or a username/password")
	}

	if c.ApiKey != "" {
		if c.Username != "" || c.Password != "" {
			return fmt.Errorf("cannot set both API key and username/password")
		}

		dec, err := base64.StdEncoding.DecodeString(c.ApiKey)
		if err != nil {
			return fmt.Errorf("API key must be base64 encoded: %w", err)
		}

		if len(strings.Split(string(dec), ":")) != 2 {
			return fmt.Errorf("invalid API key: encoded value must be in the form of 'id:api_key'")
		}

		return nil
	}

	if c.Username == "" {
		return fmt.Errorf("missing username")
	} else if c.Password == "" {
		return fmt.Errorf("missing password")
	}

	return nil
}

func (c config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("missing Endpoint")
	} else if !strings.HasPrefix(c.Endpoint, "http://") && !strings.HasPrefix(c.Endpoint, "https://") {
		return fmt.Errorf("endpoint '%s' is invalid: must start with either http:// or https://", c.Endpoint)
	} else if c.Advanced.Replicas != nil && *c.Advanced.Replicas < 0 {
		return fmt.Errorf("number_of_replicas cannot be negative")
	}

	return c.Credentials.Validate()
}

func (c config) FeatureFlags() (string, map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

// toClient initializes a client for connecting to Elasticsearch, starting the network tunnel if
// configured.
func (c config) toClient(disableRetry bool) (*client, error) {
	endpoint := c.Endpoint

	// If SSH Endpoint is configured, then try to start a tunnel before establishing connections
	if c.NetworkTunnel != nil && c.NetworkTunnel.SshForwarding != nil && c.NetworkTunnel.SshForwarding.SshEndpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil {
			return nil, fmt.Errorf("parsing endpoint URL: %w", err)
		}

		var sshConfig = &networkTunnel.SshConfig{
			SshEndpoint: c.NetworkTunnel.SshForwarding.SshEndpoint,
			PrivateKey:  []byte(c.NetworkTunnel.SshForwarding.PrivateKey),
			ForwardHost: u.Hostname(),
			ForwardPort: u.Port(),
			LocalPort:   "9200",
		}
		var tunnel = sshConfig.CreateTunnel()

		if err := tunnel.Start(); err != nil {
			return nil, err
		}

		// If SSH Tunnel is configured, we are going to create a tunnel from localhost:9200 to
		// address through the bastion server, so we use the tunnel's address.
		endpoint = "http://localhost:9200"
	}

	es, err := elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses:           []string{endpoint},
			CompressRequestBody: true,
			Username:            c.Credentials.Username,
			Password:            c.Credentials.Password,
			APIKey:              c.Credentials.ApiKey,
			RetryOnStatus:       []int{429, 502, 503, 504},
			RetryBackoff: func(i int) time.Duration {
				d := min(time.Duration(1<<i)*time.Second, 5*time.Second)
				log.WithFields(log.Fields{
					"attempt": i,
					"delay":   d.String(),
				}).Info("waiting to retry request on retryable error")
				return d
			},
			MaxRetries:   10,
			DisableRetry: disableRetry,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &client{es: es}, nil
}

type resource struct {
	Index        string `json:"index" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates bool   `json:"delta_updates" jsonschema:"default=false" jsonschema_extras:"x-delta-updates=true"`
	Shards       *int   `json:"number_of_shards,omitempty"`
}

func (r resource) Validate() error {
	if r.Index == "" {
		return fmt.Errorf("missing Index")
	} else if r.Shards != nil && *r.Shards < 1 {
		return fmt.Errorf("number_of_shards must be greater than 0")
	}

	return nil
}

// GetFieldDocString implements the jsonschema.customSchemaGetFieldDocString interface.
func (resource) GetFieldDocString(fieldName string) string {
	switch fieldName {
	case "Index":
		return "Name of the Elasticsearch index to store the materialization results."
	case "DeltaUpdates":
		return "Should updates to this table be done via delta updates. Default is false."
	case "Shards":
		return "The number of shards to create the index with. Leave blank to use the cluster default."
	default:
		return ""
	}
}

func (r resource) WithDefaults(cfg config) resource {
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	indexName := normalizeIndexName(r.Index, maxByteLength)
	if len(indexName) == 0 {
		return nil, false, fmt.Errorf("index name '%s' is invalid: must contain at least 1 character that is not '.', '-', or '-'", r.Index)
	}

	return []string{indexName}, r.DeltaUpdates, nil
}

const (
	maxByteLength = 255
)

// For index naming requirements, see
// https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
func normalizeIndexName(index string, truncateLimit int) string {
	afterPrefix := false
	var b strings.Builder
	for _, r := range index {
		// Replace disallowed characters with underscore. Most of the characters in this list are
		// named in the docs, but ES will also truncate index names that contain a '#' character to
		// drop everything including & after that '#' character, so we'll normalize those too.
		if slices.Contains([]rune{'*', '<', '"', ' ', '\\', '/', ',', '|', '>', '?', ':', '#'}, r) {
			r = '_'
		}

		// Strip disallowed prefixes. The prefix may now be an underscore from the replacement
		// above, and we'll strip that too.
		if !afterPrefix && slices.Contains([]rune{'_', '-', '+', '.'}, r) {
			continue
		}

		afterPrefix = true

		// Index names must be lowercase.
		char := strings.ToLower(string(r))
		if b.Len()+len(char) > truncateLimit {
			// Truncate extremely long names. These must be less than 255 bytes.
			break
		}
		b.WriteString(char)
	}

	return b.String()
}

type driver struct{}

var _ boilerplate.Connector = &driver{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	resourceSchema, err := schemagen.GenerateSchema("Elasticsearch Index", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-elasticsearch", configSchema(), resourceSchema)
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	return boilerplate.RunValidate(ctx, req, newMaterialization)
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	return boilerplate.RunApply(ctx, req, newMaterialization)
}

func (driver) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
	return boilerplate.RunNewTransactor(ctx, req, be, newMaterialization)
}

type materialization struct {
	cfg        config
	metaClient *client
	dataClient *client
}

var _ boilerplate.Materializer[config, fieldConfig, resource, property] = &materialization{}

func newMaterialization(ctx context.Context, materializationName string, cfg config, featureFlags map[string]bool) (boilerplate.Materializer[config, fieldConfig, resource, property], error) {
	metaClient, err := cfg.toClient(true)
	if err != nil {
		return nil, fmt.Errorf("creating metadata client: %w", err)
	}

	dataClient, err := cfg.toClient(false)
	if err != nil {
		return nil, fmt.Errorf("creating data client: %w", err)
	}

	return &materialization{
		cfg:        cfg,
		metaClient: metaClient,
		dataClient: dataClient,
	}, nil
}

func (d *materialization) Config() boilerplate.MaterializeCfg {
	return boilerplate.MaterializeCfg{
		Translate:           translateField,
		ConcurrentApply:     true,
		NoCreateNamespaces:  true,
		NoTruncateResources: true,
	}
}

func (d *materialization) PopulateInfoSchema(ctx context.Context, resourcePaths [][]string, is *boilerplate.InfoSchema) error {
	return d.metaClient.populateInfoSchema(ctx, is)
}

func (d *materialization) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	errs := &cerrors.PrereqErr{}

	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if p, err := d.metaClient.es.Ping(d.metaClient.es.Ping.WithContext(pingCtx)); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			errs.Err(fmt.Errorf("cannot connect to endpoint '%s': double check your configuration, and make sure Estuary's IP is allowed to connect to your cluster", d.cfg.Endpoint))
		} else {
			errs.Err(fmt.Errorf("pinging Elasticsearch: %w", err))
		}
	} else if p.StatusCode == http.StatusUnauthorized {
		errs.Err(fmt.Errorf("invalid credentials: received an unauthorized response (401) when trying to connect"))
	} else if p.StatusCode == http.StatusNotFound {
		errs.Err(fmt.Errorf("could not connect to endpoint: endpoint URL not found"))
	} else if p.StatusCode == http.StatusForbidden {
		errs.Err(fmt.Errorf("could not check cluster status: credentials must have 'monitor' privilege for cluster"))
	} else if p.StatusCode != http.StatusOK {
		errs.Err(fmt.Errorf("could not connect to endpoint: received status code %d", p.StatusCode))
	}

	return errs
}

func (d *materialization) NewConstraint(p pf.Projection, deltaUpdates bool, fc fieldConfig) pm.Response_Validated_Constraint {
	_, isNumeric := m.AsFormattedNumeric(&p)

	var constraint = pm.Response_Validated_Constraint{}
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection() && deltaUpdates:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The root document must be materialized"
	case len(p.Inference.Types) == 0:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields are able to be materialized"
	case p.Inference.IsSingleScalarType() || isNumeric:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The projection has a single scalar type"
	case slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field where the only possible type is 'null'"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "object"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Object fields may be materialized"
	default:
		// Any other case is one where the field is an array or has multiple types.
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}

func (d *materialization) MapType(p boilerplate.Projection, fc fieldConfig) (property, boilerplate.ElementConverter) {
	return propForProjection(&p.Projection, p.Inference.Types, fc), nil
}

func (d *materialization) Setup(ctx context.Context, is *boilerplate.InfoSchema) (string, error) {
	return "", nil
}

func (d *materialization) CreateNamespace(ctx context.Context, ns string) (string, error) {
	return "", nil
}

func (d *materialization) CreateResource(ctx context.Context, res boilerplate.MappedBinding[config, resource, property]) (string, boilerplate.ActionApplyFn, error) {
	indexName := res.ResourcePath[0]
	props := buildIndexProperties(res.Keys, res.Values, res.Document)

	return fmt.Sprintf("create index %q", indexName), func(ctx context.Context) error {
		return d.metaClient.createIndex(ctx, indexName, res.Config.Shards, d.cfg.Advanced.Replicas, props)
	}, nil
}

func (d *materialization) DeleteResource(ctx context.Context, resourcePath []string) (string, boilerplate.ActionApplyFn, error) {
	indexName := resourcePath[0]

	return fmt.Sprintf("delete index %q", indexName), func(ctx context.Context) error {
		return d.metaClient.deleteIndex(ctx, indexName)
	}, nil
}

func (d *materialization) TruncateResource(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	panic("not supported")
}

func (d *materialization) UpdateResource(
	ctx context.Context,
	resourcePath []string,
	existing boilerplate.ExistingResource,
	update boilerplate.BindingUpdate[config, resource, property],
) (string, boilerplate.ActionApplyFn, error) {
	// ElasticSearch only considers new projections, since index mappings are always nullable.
	if len(update.NewProjections) == 0 {
		return "", nil, nil
	}

	indexName := resourcePath[0]

	var actions []string
	for _, np := range update.NewProjections {
		actions = append(actions, fmt.Sprintf(
			"add mapping %q to index %q with type %q",
			translateField(np.Field),
			indexName,
			np.Mapped.Type,
		))
	}

	return strings.Join(actions, "\n"), func(ctx context.Context) error {
		for _, np := range update.NewProjections {
			if err := d.metaClient.addMappingToIndex(ctx, indexName, translateField(np.Field), np.Mapped); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (d *materialization) NewMaterializerTransactor(
	ctx context.Context,
	req pm.Request_Open,
	is boilerplate.InfoSchema,
	mappedBindings []boilerplate.MappedBinding[config, resource, property],
	be *boilerplate.BindingEvents,
) (boilerplate.MaterializerTransactor, error) {
	isServerless, err := d.dataClient.isServerless(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting serverless status: %w", err)
	}
	if isServerless {
		log.Info("connected to a serverless elasticsearch cluster")
	}

	indexToBinding := make(map[string]int)
	var bindings []binding
	for idx, b := range mappedBindings {
		ps := append(b.Keys, b.Values...)
		fields := make([]string, 0, len(ps))
		floatFields := make([]bool, len(ps))
		wrapFields := make([]bool, len(ps))

		for idx, p := range ps {
			fields = append(fields, translateField(p.Field))
			if p.Mapped.Type == elasticTypeDouble {
				floatFields[idx] = true
			} else if mustWrapAndFlatten(&p.Projection.Projection) {
				wrapFields[idx] = true
			}
		}

		indexToBinding[b.ResourcePath[0]] = idx
		bindings = append(bindings, binding{
			index:        b.ResourcePath[0],
			deltaUpdates: b.DeltaUpdates,
			fields:       fields,
			floatFields:  floatFields,
			wrapFields:   wrapFields,
			docField:     b.FieldSelection.Document,
		})
	}

	var transactor = &transactor{
		cfg:            d.cfg,
		client:         d.dataClient,
		bindings:       bindings,
		isServerless:   isServerless,
		indexToBinding: indexToBinding,
	}
	return transactor, nil

}

func (d *materialization) Close(ctx context.Context) {}

var (
	// ElasticSearch has a number of pre-defined "Metadata" fields (see
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-fields.html) that
	// exist for every index, and trying to create an index with a separate mapping with any of
	// these names will fail with a "field is defined more than once" sort of error. All we can
	// really do is differentiate our Flow field by appending "_flow" to the name of the field to
	// allow the field to be created.
	elasticReservedFields []string = []string{
		"_index",
		"_id",
		"_source",
		"_size",
		"_doc_count",
		"_field_names",
		"_ignored",
		"_routing",
		"_tier",
		// _meta is itself a metadata field, and is for custom application-specific metadata.
		// Notably, you can explicitly create a mapping for the _meta field, so it seems reasonable
		// to allow Flow collection data to be materialized as this field if it exists and is in the
		// field selection.
	}

	reservedFieldSuffix string = "_flow"
)

func translateField(f string) string {
	// ElasticSearch allows dots in field names, but it then treats these fields as an object
	// hierarchy with nested fields. This isn't what we want, so dots are substituted with
	// underscores.
	f = strings.ReplaceAll(f, ".", "_")

	if slices.Contains(elasticReservedFields, f) {
		f = f + reservedFieldSuffix
	}

	return f
}

func main() { boilerplate.RunMain(new(driver)) }
