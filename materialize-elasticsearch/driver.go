package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	"github.com/estuary/connectors/go/pkg/slices"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

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
      "order": 0
    },
    "credentials": {
      "type": "object",
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
	}

	return c.Credentials.Validate()
}

type resource struct {
	Index        string `json:"index" jsonschema_extras:"x-collection-name=true"`
	DeltaUpdates bool   `json:"delta_updates" jsonschema:"default=false"`
	Shards       *int   `json:"number_of_shards,omitempty"`
	Replicas     *int   `json:"number_of_replicas,omitempty"`
}

func (r resource) Validate() error {
	if r.Index == "" {
		return fmt.Errorf("missing Index")
	} else if r.Shards != nil && *r.Shards < 1 {
		return fmt.Errorf("number_of_shards must be greater than 0")
	} else if r.Replicas != nil && *r.Replicas < 0 {
		return fmt.Errorf("number_of_replicas cannot be negative")
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
	case "Replicas":
		return "The number of replicas to create the index with. Leave blank to use the cluster default."
	default:
		return ""
	}
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

		constraints, err := validateBinding(res, binding.Collection, storedSpec)
		if err != nil {
			return nil, fmt.Errorf("validating binding: %w", err)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: res.DeltaUpdates,
			ResourcePath: []string{res.Index},
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
		return client.createMetaIndex(ctx)
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

		found, err := findExistingBinding(res.Index, binding.Collection.Name, storedSpec)
		if err != nil {
			return nil, err
		}

		if found == nil {
			if err := doAction(fmt.Sprintf("create index '%s'", res.Index), func() error {
				return client.createIndex(ctx, res.Index, res.Shards, res.Replicas, buildIndexProperties(binding))
			}); err != nil {
				return nil, fmt.Errorf("creating index '%s': %w", res.Index, err)
			}
		} else {
			// Map any new fields in the index.
			existingFields := found.FieldSelection.AllFields()
			for _, newField := range binding.FieldSelection.AllFields() {
				if slices.Contains(existingFields, newField) {
					continue
				}

				prop := propForField(newField, binding)
				if err := doAction(fmt.Sprintf("add mapping '%s' to index '%s' with type '%s'", newField, res.Index, prop.Type), func() error {
					return client.addMappingToIndex(ctx, res.Index, newField, prop)
				}); err != nil {
					return nil, fmt.Errorf("adding mapping for field %q to index %q: %w", newField, res.Index, err)
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

		indexToBinding[res.Index] = idx
		bindings = append(bindings, binding{
			index:        res.Index,
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
