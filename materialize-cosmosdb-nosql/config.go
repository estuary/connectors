package connector

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"unicode"

	"github.com/estuary/connectors/go/common"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/invopop/jsonschema"
)

const accountKey = "Account Key"

type accountKeyCredentials struct {
	Key string `json:"key" jsonschema:"title=Account Key,description=A read-write account key for your Azure Cosmos DB account.,minLength=1" jsonschema_extras:"secret=true"`
}

type credentialConfig struct {
	CredentialsTitle string `json:"credentials_title"`
	accountKeyCredentials
}

func (credentialConfig) JSONSchema() *jsonschema.Schema {
	schema := schemagen.OneOfSchema("Authentication", "Authenticate with a read-write account key.", "credentials_title", accountKey,
		schemagen.OneOfSubSchema(accountKey, accountKeyCredentials{}, accountKey))
	method, _ := schema.OneOf[0].Properties.Get("credentials_title")
	method.Title = "Authentication Method"
	method.Description = "Authenticate with an account key."
	schema.OneOf[0].Required = append(schema.OneOf[0].Required, "credentials_title")
	return schema
}

type config struct {
	Endpoint    string           `json:"endpoint" jsonschema:"title=Endpoint,description=The account URI of your Azure Cosmos DB for NoSQL account. For example https://myaccount.documents.azure.com:443/." jsonschema_extras:"order=0"`
	Credentials credentialConfig `json:"credentials" jsonschema:"title=Authentication,description=Authenticate with a read-write account key." jsonschema_extras:"order=1"`
	Database    string           `json:"database" jsonschema:"title=Database,description=Name of the database to materialize to. The database is created if it does not exist." jsonschema_extras:"order=2"`
	HardDelete  bool             `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=If enabled documents deleted in the source are also deleted from the destination. Otherwise the destination retains documents with deletion metadata.,default=false" jsonschema_extras:"order=3,nonsensitive=true"`

	Advanced advancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Config,description=Advanced settings for the connector." jsonschema_extras:"advanced=true"`
}

type advancedConfig struct {
	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support." jsonschema_extras:"nonsensitive=true"`
}

var _ boilerplate.EndpointConfiger = config{}

func (c config) Validate() error {
	var requiredProperties = [][]string{
		{"endpoint", c.Endpoint},
		{"credentials.credentials_title", c.Credentials.CredentialsTitle},
		{"credentials.key", c.Credentials.Key},
		{"database", c.Database},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if c.Credentials.CredentialsTitle != accountKey {
		return fmt.Errorf("credentials.credentials_title must be %q", accountKey)
	}

	uri, err := url.Parse(c.Endpoint)
	if err != nil || (uri.Scheme != "https" && uri.Scheme != "http") || uri.Hostname() == "" {
		return fmt.Errorf("endpoint must be an absolute http or https account URI")
	}
	if uri.User != nil || uri.RawQuery != "" || uri.Fragment != "" || (uri.Path != "" && uri.Path != "/") {
		return fmt.Errorf("endpoint must be an account URI without credentials, a query, a fragment, or a resource path")
	}

	return nil
}

func (c config) DefaultNamespace() string {
	return c.Database
}

func (c config) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.Advanced.FeatureFlags, nil
}

type resource struct {
	Container    string `json:"container" jsonschema:"title=Container Name,description=Name of the container to materialize to." jsonschema_extras:"x-collection-name=true"`
	PartitionKey string `json:"partition_key,omitempty" jsonschema:"title=Partition Key,description=A collection key JSON pointer to use as the container partition key. Defaults to the first collection key pointer."`
	DeltaUpdates bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Updates,description=Skip loading existing documents and replace each item with the latest document for its key.,default=false" jsonschema_extras:"x-delta-updates=true,nonsensitive=true"`

	database string // Used for constructing the resource path.
}

var _ boilerplate.Resourcer[resource, config] = resource{}

func (r resource) Validate() error {
	if r.Container == "" {
		return fmt.Errorf("missing 'container'")
	}
	if r.PartitionKey != "" && !strings.HasPrefix(r.PartitionKey, "/") {
		return fmt.Errorf("partition_key must be a collection key JSON pointer")
	}
	return nil
}

func (r resource) WithDefaults(cfg config) resource {
	r.database = cfg.Database
	return r
}

func (r resource) Parameters() ([]string, bool, error) {
	return []string{r.database, r.Container}, r.DeltaUpdates, nil
}

func (r resource) partitionKey(keys []string) (string, error) {
	if len(keys) == 0 {
		return "", fmt.Errorf("collection must have at least one key for the container partition key")
	}
	pk := r.PartitionKey
	if pk == "" {
		pk = keys[0]
	} else if !slices.Contains(keys, pk) {
		return "", fmt.Errorf("partition_key %q must be one of the collection key pointers %v", r.PartitionKey, keys)
	}
	if strings.HasPrefix(pk, "/id/") {
		return "", fmt.Errorf("partition_key %q is beneath /id, but storage replaces id with an encoded string; choose /id itself or another collection key", pk)
	}
	if _, err := cosmosPartitionPath(pk); err != nil {
		return "", err
	}
	return pk, nil
}

func cosmosPartitionPath(ptr string) (string, error) {
	if !strings.HasPrefix(ptr, "/") {
		return "", fmt.Errorf("partition_key %q must be a non-root JSON pointer", ptr)
	}
	parts := strings.Split(ptr[1:], "/")
	for i, part := range parts {
		for j := 0; j < len(part); j++ {
			if part[j] == '~' {
				if j+1 == len(part) || (part[j+1] != '0' && part[j+1] != '1') {
					return "", fmt.Errorf("partition_key %q has an invalid JSON pointer escape", ptr)
				}
				j++
			}
		}
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		if part == "" || strings.ContainsAny(part, `"\`) || strings.ContainsFunc(part, unicode.IsControl) {
			return "", fmt.Errorf("unsupported partition key %q: property names must be nonempty and cannot contain quotes, backslashes, or control characters", ptr)
		}
		// Cosmos paths use quoted property names rather than JSON pointer escapes.
		if strings.ContainsFunc(part, func(r rune) bool {
			return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_')
		}) {
			part = `"` + part + `"`
		}
		parts[i] = part
	}
	return "/" + strings.Join(parts, "/"), nil
}

func normalizeCosmosPartitionPath(path string) (string, error) {
	var ptr strings.Builder
	for path != "" {
		if !strings.HasPrefix(path, "/") || len(path) == 1 {
			return "", fmt.Errorf("invalid Cosmos partition path")
		}
		path = path[1:]
		var part string
		if strings.HasPrefix(path, `"`) {
			var ok bool
			if part, path, ok = strings.Cut(path[1:], `"`); !ok {
				return "", fmt.Errorf("unterminated Cosmos partition path segment")
			}
		} else if next := strings.IndexByte(path, '/'); next != -1 {
			part, path = path[:next], path[next:]
		} else {
			part, path = path, ""
		}
		ptr.WriteByte('/')
		ptr.WriteString(strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1"))
	}
	return cosmosPartitionPath(ptr.String())
}
