package hubspot

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	schemagen "github.com/estuary/connectors/go/schema-gen"
	"github.com/invopop/jsonschema"
	orderedmap "github.com/wk8/go-ordered-map/v2"
	"golang.org/x/time/rate"
)

var featureFlagDefaults = map[string]bool{}

type AuthType string

const (
	OAuth2AuthType     AuthType = "OAuth2"
	ServiceKeyAuthType AuthType = "ServiceKey"
)

// Secret is a string that makes you work to print.
type Secret string

func (s Secret) Expose() string {
	return string(s)
}

func (s Secret) String() string {
	return "***"
}

func (s Secret) MarshalJSON() ([]byte, error) {
	return []byte(`"***"`), nil
}

func (s *Secret) UnmarshalJSON(b []byte) error {
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	*s = Secret(str)
	return nil
}

type OAuth2Credentials struct {
	ClientID             string    `json:"client_id" jsonschema_extras:"secret=true"`
	ClientSecret         Secret    `json:"client_secret" jsonschema_extras:"secret=true"`
	RefreshToken         Secret    `json:"refresh_token" jsonschema_extras:"secret=true"`
	AccessToken          Secret    `json:"access_token" jsonschema_extras:"secret=true"`
	AccessTokenExpiresAt time.Time `json:"access_token_expires_at"`
}

// ServiceKeyCredentials allows authentication with a service key.  You would
// only want to use this method for debugging and development.
type ServiceKeyCredentials struct {
	ServiceKey Secret `json:"service_key" jsonschema:"title=Service Key,description=HubSpot Service Key with required scope grants." jsonschema_extras:"secret=true"`
}

type Credentials struct {
	AuthType AuthType `json:"auth_type"`

	OAuth2Credentials
	ServiceKeyCredentials
}

func (c *Credentials) Validate() error {
	switch c.AuthType {
	case OAuth2AuthType:
		if c.RefreshToken == "" {
			return errors.New("missing 'refresh_token'")
		}
		return nil
	case ServiceKeyAuthType:
		if c.ServiceKey == "" {
			return errors.New("missing 'service_key'")
		}
		return nil
	default:
		return fmt.Errorf("unknown 'auth_type'")
	}
}

func (Credentials) JSONSchema() *jsonschema.Schema {
	subSchemas := []schemagen.OneOfSubSchemaT{
		schemagen.OneOfSubSchema("OAuth2", OAuth2Credentials{}, string(OAuth2AuthType)).WithOAuth2Provider(Oauth2Provider),
		schemagen.OneOfSubSchema("Service Key", ServiceKeyCredentials{}, string(ServiceKeyAuthType)),
	}
	return schemagen.OneOfSchema("Authentication", "", "auth_type", string(OAuth2AuthType), subSchemas...)
}

type AdvancedConfig struct {
	Limit       float64 `json:"limit" jsonschema:"title=Request Limit,description=Maximum requests per second across all bindings excluding search.,default=10,exclusiveMinimum=0,maximum=250"`
	Burst       int     `json:"burst" jsonschema:"title=Request Burst Count,description=Burst requests across all bindings excluding search.,default=100,minimum=1,maximum=1000"`
	SearchLimit float64 `json:"search_limit" jsonschema:"title=Search Request Limit,description=Maximum search requests per second across all bindings.,default=5,exclusiveMinimum=0,maximum=250"`
	SearchBurst int     `json:"search_burst" jsonschema:"title=Search Request Burst Count,description=Burst search requests across all bindings.,default=5,minimum=1,maximum=1000"`

	FeatureFlags string `json:"feature_flags,omitempty" jsonschema:"title=Feature Flags,description=This property is intended for Estuary internal use. You should only modify this field as directed by Estuary support."`
}

type Config struct {
	Credentials Credentials    `json:"credentials" jsonschema:"title=Authentication"`
	Advanced    AdvancedConfig `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Options for advanced users. You should not typically need to modify these." jsonschema_extras:"advanced=true"`
}

func (c *Config) Validate() error {
	return c.Credentials.Validate()
}

// DefaultNamepace is required to implement EndpointConfiger.
func (c *Config) DefaultNamespace() string {
	return ""
}

func (c *Config) FeatureFlags() (raw string, defaults map[string]bool) {
	return c.Advanced.FeatureFlags, featureFlagDefaults
}

func (c *Config) Limiter() *rate.Limiter {
	return NewLimiter(c.Advanced.Limit, c.Advanced.Burst)
}

func (c *Config) SearchLimiter() *rate.Limiter {
	return NewSearchLimiter(c.Advanced.SearchLimit, c.Advanced.SearchBurst)
}

type Resource struct {
	// Name is the collection name.  It is required that there is a
	// x-collection-name but is otherwise unused.
	Name string `json:"name"`
	// This connector always uses delta updates, we include this read-only
	// option to draw attention to the behavior and be consistent with other
	// materializations.
	DeltaUpdates bool `json:"delta_updates"`
	// Object is the title case representation of the CRMObject.
	Object string `json:"object"`
}

// JSONSchema defines the jsonschema for the Resource.
//
// This method is used instead of struct tags so that the object type enum can
// be generated.
func (Resource) JSONSchema() *jsonschema.Schema {
	objectEnum := []any{}
	for _, object := range allCRMObjects {
		objectEnum = append(objectEnum, object.ToTitle())
	}

	return &jsonschema.Schema{
		Type: "object",
		Properties: orderedmap.New[string, *jsonschema.Schema](orderedmap.WithInitialData[string, *jsonschema.Schema](
			orderedmap.Pair[string, *jsonschema.Schema]{
				Key: "name",
				Value: &jsonschema.Schema{
					Type: "string",
					Extras: map[string]any{
						"x-collection-name": true,
						"x-hidden-field":    true,
					},
				},
			},
			orderedmap.Pair[string, *jsonschema.Schema]{
				Key: "delta_updates",
				Value: &jsonschema.Schema{
					Type:        "boolean",
					Title:       "Delta Update",
					Description: "Should updates to this table be done via delta updates. This connector always uses delta updates.",
					ReadOnly:    true,
					Default:     true,
					Extras: map[string]any{
						"order": 2,
					},
				},
			},
			orderedmap.Pair[string, *jsonschema.Schema]{
				Key: "object",
				Value: &jsonschema.Schema{
					Type:        "string",
					Title:       "Object type",
					Description: "Object type.",
					Enum:        objectEnum,
					Extras: map[string]any{
						"order": 1,
					},
				},
			},
		)),
		Required: []string{
			"name",
			"object",
		},
	}
}

func (r Resource) CRMObject() (CRMObject, error) {
	return CRMObjectFromTitle(r.Object)
}

func (r Resource) Validate() error {
	_, err := r.CRMObject()
	return err
}

func (r Resource) Path() ([]string, error) {
	object, err := r.CRMObject()
	if err != nil {
		return nil, err
	}
	return []string{"crm", object.String()}, nil
}

type FieldConfig struct{}

func (c FieldConfig) Validate() error {
	return nil
}
