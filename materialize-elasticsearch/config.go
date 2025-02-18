package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	log "github.com/sirupsen/logrus"
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
	Credentials credentials `json:"credentials"`
	Endpoint    string      `json:"endpoint"`
	HardDelete  bool        `json:"hardDelete,omitempty"`

	Advanced advancedConfig `json:"advanced,omitempty"`

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
				d := time.Duration(1<<i) * time.Second
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

const maxByteLength = 255

func (r resource) Parameters() ([]string, bool, error) {
	indexName := normalizeIndexName(r.Index, maxByteLength)
	if len(indexName) == 0 {
		return nil, false, fmt.Errorf("index name '%s' is invalid: must contain at least 1 character that is not '.', '-', or '-'", r.Index)
	}

	return []string{indexName}, r.DeltaUpdates, nil
}

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
