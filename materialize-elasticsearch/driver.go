package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

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
	Credentials credentials    `json:"credentials"`
	Endpoint    string         `json:"endpoint"`
	Advanced    advancedConfig `json:"advanced,omitempty"`

	NetworkTunnel *tunnelConfig `json:"networkTunnel,omitempty"`
}

type advancedConfig struct {
	Replicas *int `json:"number_of_replicas,omitempty"`
}

// The `go-schema-gen` package doesn't have a good way of dealing with oneOf, and I couldn't get it
// to output a schema that works with Flow's UI. So the endpoint config schema is written out manually
// instead of being generated from the structs.
func configSchema() json.RawMessage {
	var schemaStr = `{
		"$schema": "http://json-schema.org/draft/2020-12/schema",
		"$id": "https://github.com/estuary/connectors/materialize-elasticsearch/config",
		"properties": {
		  "endpoint": {
			"type": "string",
			"title": "Endpoint",
			"description": "Endpoint host or URL. Must start with http:// or https://. If using Elastic Cloud this follows the format https://CLUSTER_ID.REGION.CLOUD_PLATFORM.DOMAIN:PORT",
			"pattern": "^(http://|https://).+$",
			"order": 0
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
		  "endpoint"
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

// toClient initializes a client for connecting to Elasticsearch, starting the network tunnel if
// configured.
func (c config) toClient() (*client, error) {
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
			return nil, fmt.Errorf("error starting network tunnel: %w", err)
		}

		// If SSH Tunnel is configured, we are going to create a tunnel from localhost:9200 to
		// address through the bastion server, so we use the tunnel's address.
		endpoint = "http://localhost:9200"
	}

	es, err := elasticsearch.NewClient(
		elasticsearch.Config{
			Addresses: []string{endpoint},
			Username:  c.Credentials.Username,
			Password:  c.Credentials.Password,
			APIKey:    c.Credentials.ApiKey,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	return &client{es: es}, nil
}

type resource struct {
	Index        string `json:"index" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates bool   `json:"delta_updates" jsonschema:"default=false"`
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

const (
	maxByteLength = 255
)

// For index naming requirements, see
// https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
func normalizeIndexName(index string, truncateLimit int) string {
	afterPrefix := false
	var b strings.Builder
	for _, r := range index {
		// Replace disallowed characters with underscore. Of the various problematic characters in
		// an Elasticsearch index name, a "." is the only one that Flow allows in a collection name.
		if r == '.' {
			r = '_'
		}

		// Strip disallowed prefixes that may be present in a Flow collection name. A '.' is also a
		// bad prefix, but those were already replaced with '_' above.
		if !afterPrefix && (r == '-' || r == '_') {
			continue
		}
		afterPrefix = true

		char := strings.ToLower(string(r))
		if b.Len()+len(char) > truncateLimit {
			// Truncate extremely long names. These must be less than 255 bytes.
			break
		}
		b.WriteString(char)
	}

	return b.String()
}

// driver implements the DriverServer interface.
type driver struct{}

func (driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	var endpointSchema = configSchema()

	resourceSchema, err := schemagen.GenerateSchema("Elasticsearch Index", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         endpointSchema,
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-elasticsearch",
	}, nil
}

func (driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.toClient()
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if p, err := client.es.Ping(client.es.Ping.WithContext(pingCtx)); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, cerrors.NewUserError(err, fmt.Sprintf("cannot connect to endpoint '%s': double check your configuration, and make sure Estuary's IP is allowed to connect to your cluster", cfg.Endpoint))
		}

		return nil, fmt.Errorf("pinging Elasticsearch: %w", err)
	} else if p.StatusCode == http.StatusUnauthorized {
		return nil, cerrors.NewUserError(nil, "invalid credentials: received an unauthorized response (401) when trying to connect")
	} else if p.StatusCode == http.StatusNotFound {
		return nil, cerrors.NewUserError(nil, "could not connect to endpoint: endpoint URL not found")
	} else if p.StatusCode == http.StatusForbidden {
		return nil, cerrors.NewUserError(nil, "could not check cluster status: credentials must have 'monitor' privilege for cluster")
	} else if p.StatusCode != http.StatusOK {
		return nil, cerrors.NewUserError(nil, fmt.Sprintf("could not connect to endpoint: received status code %d", p.StatusCode))
	}

	storedSpec, err := client.getSpec(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("getting spec: %w", err)
	}

	var out []*pm.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		indexName := normalizeIndexName(res.Index, maxByteLength)
		if len(indexName) == 0 {
			return nil, fmt.Errorf("index name '%s' is invalid: must contain at least 1 character that is not '.', '-', or '-'", res.Index)
		}

		constraints, err := validateBinding(res, []string{indexName}, binding.Collection, storedSpec)
		if err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.DeltaUpdates,
			ResourcePath: []string{indexName},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.toClient()
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	actions := []string{}
	doAction := func(desc string, fn func() error) error {
		if !req.DryRun {
			actions = append(actions, "- "+desc)
			return fn()
		}
		actions = append(actions, "- "+desc+" (skipping due to dry-run)")
		return nil
	}

	if err := doAction(fmt.Sprintf("create index '%s'", defaultFlowMaterializations), func() error {
		return client.createMetaIndex(ctx, cfg.Advanced.Replicas)
	},
	); err != nil {
		return nil, fmt.Errorf("creating metadata index")
	}

	storedSpec, err := client.getSpec(ctx, req.Materialization.Name)
	if err != nil {
		return nil, fmt.Errorf("getting spec: %w", err)
	}

	for _, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		if err := validateSelectedFields(res, binding, storedSpec); err != nil {
			return nil, fmt.Errorf("validating selected fields: %w", err)
		}

		found, err := findExistingBinding(binding.ResourcePath, binding.Collection.Name, storedSpec)
		if err != nil {
			return nil, err
		}

		if found == nil {
			if err := doAction(fmt.Sprintf("create index '%s'", binding.ResourcePath[0]), func() error {
				return client.createIndex(ctx, binding.ResourcePath[0], res.Shards, cfg.Advanced.Replicas, buildIndexProperties(binding))
			}); err != nil {
				return nil, fmt.Errorf("creating index '%s': %w", binding.ResourcePath[0], err)
			}
		} else {
			// Map any new fields in the index.
			existingFields := found.FieldSelection.AllFields()
			for _, newField := range binding.FieldSelection.AllFields() {
				if slices.Contains(existingFields, newField) {
					continue
				}

				prop := propForField(newField, binding)
				if err := doAction(fmt.Sprintf("add mapping '%s' to index '%s' with type '%s'", newField, binding.ResourcePath[0], prop.Type), func() error {
					return client.addMappingToIndex(ctx, binding.ResourcePath[0], newField, prop)
				}); err != nil {
					return nil, fmt.Errorf("adding mapping for field %q to index %q: %w", newField, binding.ResourcePath[0], err)
				}
			}
		}
	}

	if err := doAction(fmt.Sprintf("update stored materialization spec and set version = %s", req.Version), func() error {
		return client.putSpec(ctx, req.Materialization, req.Version)
	}); err != nil {
		return nil, fmt.Errorf("updating stored materialization spec: %w", err)
	}

	return &pm.Response_Applied{ActionDescription: strings.Join(actions, "\n")}, nil
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	client, err := cfg.toClient()
	if err != nil {
		return nil, nil, fmt.Errorf("creating client: %w", err)
	}

	indexToBinding := make(map[string]int)
	var bindings []binding
	for idx, b := range open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}

		indexToBinding[b.ResourcePath[0]] = idx
		bindings = append(bindings, binding{
			index:        b.ResourcePath[0],
			deltaUpdates: res.DeltaUpdates,
			fields:       append(b.FieldSelection.Keys, b.FieldSelection.Values...),
			docField:     b.FieldSelection.Document,
		})
	}

	var transactor = &transactor{
		client:         client,
		bindings:       bindings,
		indexToBinding: indexToBinding,
	}
	return transactor, &pm.Response_Opened{}, nil
}

func main() { boilerplate.RunMain(new(driver)) }
