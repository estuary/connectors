package protocol

import (
	"encoding/json"
)

type RequestCommand string

const (
	Spec        RequestCommand = "spec"
	Validate    RequestCommand = "validate"
	Apply       RequestCommand = "apply"
	Open        RequestCommand = "open"
	Acknowledge RequestCommand = "acknowledge"
	Load        RequestCommand = "load"
	Flush       RequestCommand = "flush"
	Store       RequestCommand = "store"
	StartCommit RequestCommand = "startCommit"
)

type ResponseCommand string

const (
	Speced        ResponseCommand = "spec"
	Validated     ResponseCommand = "validated"
	Applied       ResponseCommand = "applied"
	Opened        ResponseCommand = "opened"
	Acknowledged  ResponseCommand = "acknowledged"
	Loaded        ResponseCommand = "loaded"
	Flushed       ResponseCommand = "flushed"
	StartedCommit ResponseCommand = "startedCommit"
)

type SpecRequest struct{}

type SpecResponse struct {
	// URL for connector's documentation.
	DocumentationUrl string `json:"documentationUrl"`
	// JSON schema of the connector's endpoint configuration.
	ConfigSchema json.RawMessage `json:"configSchema"`
	// JSON schema of a binding's resource specification.
	ResourceConfigSchema json.RawMessage `json:"resourceConfigSchema"`
	// Optional OAuth2 configuration.
	Oauth2 *OAuth2Spec `json:"oauth2,omitempty"`
}

// OAuth2Spec describes an OAuth2 provider
type OAuth2Spec struct {
	// Name of the OAuth2 provider. This is a machine-readable key and must stay consistent. One
	// example use case is to map providers to their respective style of buttons in the UI
	Provider string `json:"provider,omitempty"`
	// Template for authorization URL, this is the first step of the OAuth2 flow where the user is
	// redirected to the OAuth2 provider to authorize access to their account
	AuthUrlTemplate string `json:"authUrlTemplate,omitempty"`
	// Template for access token URL, this is the second step of the OAuth2 flow, where we request
	// an access token from the provider
	AccessTokenUrlTemplate string `json:"accessTokenUrlTemplate,omitempty"`
	// The method used to send access_token request. POST by default.
	AccessTokenMethod string `json:"accessTokenMethod,omitempty"`
	// The POST body of the access_token request
	AccessTokenBody string `json:"accessTokenBody,omitempty"`
	// Headers for the access_token request
	AccessTokenHeadersJson json.RawMessage `json:"accessTokenHeadersJson,omitempty"`
	// A json map that maps the response from the OAuth provider for Access Token request to keys in
	// the connector endpoint configuration. If the connector supports refresh tokens, must include
	// `refresh_token` and `expires_in`. If this mapping is not provided, the keys from the response
	// are passed as-is to the connector config.
	AccessTokenResponseMapJson json.RawMessage `json:"accessTokenResponseMapJson,omitempty"`
	// Template for refresh token URL, some providers require that the access token be refreshed.
	RefreshTokenUrlTemplate string `json:"refreshTokenUrlTemplate,omitempty"`
	// The method used to send refresh_token request. POST by default.
	RefreshTokenMethod string `json:"refreshTokenMethod,omitempty"`
	// The POST body of the refresh_token request
	RefreshTokenBody string `json:"refreshTokenBody,omitempty"`
	// Headers for the refresh_token request
	RefreshTokenHeadersJson json.RawMessage `json:"refreshTokenHeadersJson,omitempty"`
	// A json map that maps the response from the OAuth provider for Refresh Token request to keys
	// in the connector endpoint configuration. If the connector supports refresh tokens, must
	// include `refresh_token` and `expires_in`. If this mapping is not provided, the keys from the
	// response are passed as-is to the connector config.
	RefreshTokenResponseMapJson json.RawMessage `json:"refreshTokenResponseMapJson,omitempty"`
}

type ValidateRequest struct {
	// Name of the materialization being validated.
	Name string `json:"name"`
	// Connector endpoint configuration.
	Config json.RawMessage `json:"config"`
	// Proposed bindings of the validated materialization.
	Bindings []ValidateBinding `json:"bindings"`
}

type ValidateBinding struct {
	// Collection of the proposed binding.
	Collection CollectionSpec
	// Resource configuration of the proposed binding.
	ResourceConfig json.RawMessage
	// Field configuration of the proposed binding.
	FieldConfig map[string]json.RawMessage
}

type CollectionSpec struct {
	// Name of this collection.
	Name string `json:"name"`
	// Composite key of the collection.
	// Keys are specified as an ordered sequence of JSON-Pointers.
	Key []string `json:"key"`
	// Logically-partitioned fields of this collection.
	PartitionFields []string `json:"partitionFields"`
	// Projections of this collection.
	Projections []Projection `json:"projections"`
	// JSON Schema against which collection documents are validated.
	// If set, then writeSchema and readSchema are not.
	Schema json.RawMessage `json:"schema,omitempty"` //optional
	// JSON Schema against which written collection documents are validated.
	// If set, then readSchema is also and schema is not.
	WriteSchema json.RawMessage `json:"writeSchema,omitempty"` //optional
	// JSON Schema against which read collection documents are validated.
	// If set, then writeSchema is also and schema is not.
	ReadSchema json.RawMessage `json:"readSchema,omitempty"` // optional
}

type Projection struct {
	// Document location of this projection, as a JSON-Pointer.
	Ptr string `json:"ptr"`
	// Flattened, tabular alias of this projection.
	// A field may correspond to a SQL table column, for example.
	Field string `json:"field"`
	// Was this projection explicitly provided ?
	// (As opposed to implicitly created through static analysis of the schema).
	Explicit bool `json:"explicit"`
	// Does this projection constitute a logical partitioning of the collection?
	IsPartitionKey bool `json:"isPartitionKey"`
	// Does this location form (part of) the collection key?
	IsPrimaryKey bool `json:"isPrimaryKey"`
	// Inference of this projection.
	Inference Inference `json:"inference"`
}

type Inference struct {
	// The possible types for this location. Subset of:
	// ["null", "boolean", "object", "array", "integer", "numeric", "string"].
	Types []string `json:"types"`
	// String type-specific inferences, or null iff types
	// doesn't include "string".
	String_ *StringInference `json:"string,omitempty"` // optional
	// The title from the schema, if provided.
	Title string `json:"title"`
	// The description from the schema, if provided.
	Description string `json:"description"`
	// The default value from the schema, or "null" if there is no default.
	Default_ json.RawMessage `json:"default"`
	// Whether this location is marked as a secret, like a credential or password.
	Secret bool `json:"secret"`
	// Existence of this document location.
	Exists Exists `json:"exists"`
}

type StringInference struct {
	// Annotated Content-Type when the projection is of "string" type.
	ContentType string `json:"contentType"`
	// Annotated format when the projection is of "string" type.
	Format string `json:"format"`
	// Annotated Content-Encoding when the projection is of "string" type.
	ContentEncoding string `json:"contentEncoding"`
	// Is the Content-Encoding "base64" (case-invariant)?
	IsBase64 bool `json:"isBase64"`
	// Maximum length when the projection is of "string" type.
	// Zero for no limit.
	MaxLength int `json:"maxLength"`
}

type Exists string

const (
	// The location must exist.
	MustExist Exists = "Must"
	// The location may exist or be undefined.
	// Its schema has explicit keywords which allow it to exist
	// and which may constrain its shape, such as additionalProperties,
	// items, unevaluatedProperties, or unevaluatedItems.
	MayExist Exists = "May"
	// The location may exist or be undefined.
	// Its schema omits any associated keywords, but the specification's
	// default behavior allows the location to exist.
	ImplicitExist Exists = "Implicit"
	// The location cannot exist. For example, it's outside of permitted
	// array bounds, or is a disallowed property, or has an impossible type.
	CannotExist Exists = "Cannot"
)

type ValidateResponse struct {
	// Validated bindings of the endpoint.
	Bindings []ValidatedBinding `json:"bindings"`
}

type ValidatedBinding struct {
	// Resource path which fully qualifies the endpoint resource identified by this binding.
	ResourcePath []string `json:"resourcePath"`
	// Mapping of fields to their connector-imposed constraints.
	// The Flow runtime resolves a final set of fields from the user's specification
	// and the set of constraints returned by the connector.
	Constraints map[string]Constraint `json:"constraints"`
	// Should delta-updates be used for this binding?
	DeltaUpdates bool `json:"deltaUpdates"`
}

// A Constraint constrains the use of a collection projection within a materialization binding.
type Constraint struct {
	// The type of this constraint.
	Type ConstraintType `json:"type"`
	// A user-facing reason for the constraint on this field.
	Reason string `json:"reason"`
}

type ConstraintType string

const (
	// This specific projection must be present.
	FieldRequired ConstraintType = "FieldRequired"
	// At least one projection with this location pointer must be present.
	LocationRequired ConstraintType = "LocationRequired"
	// A projection with this location is recommended, and should be included by
	// default.
	LocationRecommended ConstraintType = "LocationRecommended"
	// This projection may be included, but should be omitted by default.
	FieldOptional ConstraintType = "FieldOptional"
	// This projection must not be present in the materialization.
	FieldForbidden ConstraintType = "FieldForbidden"
	// This specific projection is required but is also unacceptable (e.x.,
	// because it uses an incompatible type with a previous applied version).
	Unsatisfiable ConstraintType = "Unsatisfiable"
)

type ApplyRequest struct {
	// Name of the materialization being applied.
	Name string `json:"name"`
	// Connector endpoint configuration.
	Config json.RawMessage `json:"config"`
	// Binding specifications of the applied materialization.
	Bindings []ApplyBinding `json:"bindings"`
	// Opaque, unique version of this materialization application.
	Version string `json:"version"`
	// Is this application a dry run?
	// Dry-run applications take no action.
	DryRun bool `json:"dryRun"`
}

type ApplyBinding struct {
	// Collection of this binding.
	Collection CollectionSpec `json:"collection"`
	// Resource configuration of this binding.
	ResourceConfig json.RawMessage `json:"resourceConfig"`
	// Resource path which fully qualifies the endpoint resource identified by this binding.
	// For an RDBMS, this might be ["my-schema", "my-table"].
	// For Kafka, this might be ["my-topic-name"].
	// For Redis or DynamoDB, this might be ["/my/key/prefix"].
	ResourcePath []string `json:"resourcePath"`
	// Does this binding use delta-updates instead of standard materialization?
	DeltaUpdates bool `json:"deltaUpdates"`
	// Fields which have been selected for materialization.
	FieldSelection FieldSelection `json:"fieldSelection"`
}

type FieldSelection struct {
	// Selected fields which are collection key components.
	Keys []string `json:"keys"`
	// Selected fields which are values.
	Values []string `json:"values"`
	// Field which represents the Flow document, or null if the document isn't materialized.
	Document *string `json:"document,omitempty"`
	// Custom field configuration of the binding, keyed by field.
	FieldConfig map[string]json.RawMessage `json:"fieldConfig"`
}

type ApplyResponse struct {
	// User-facing description of the action taken by this application.
	// If the apply was a dry-run, then this is a description of actions
	// that would have been taken.
	ActionDescription string `json:"actionDescription"`
}

type OpenRequest struct {
	// Name of the materialization being opened.
	Name string `json:"name"`
	// Connector endpoint configuration.
	Config json.RawMessage `json:"config"`
	// Binding specifications of the opened materialization.
	Bindings []ApplyBinding `json:"bindings"`
	// Opaque, unique version of this materialization.
	Version string `json:"version"`
	// Beginning key-range which this connector invocation will materialize.
	// [keyBegin, keyEnd] are the inclusive range of keys processed by this
	// connector invocation. Ranges reflect the disjoint chunks of ownership
	// specific to each instance of a scale-out materialization.
	//
	// The Flow runtime manages the routing of document keys to distinct
	// connector invocations, and each invocation will receive only disjoint
	// subsets of possible keys. Thus, this key range is merely advisory.
	KeyBegin int `json:"keyBegin"`
	// Ending key-range which this connector invocation will materialize.
	KeyEnd int `json:"keyEnd"`
	// Last-persisted driver checkpoint from a previous materialization invocation.
	// Or empty, if the driver has cleared or never set its checkpoint.
	DriverCheckpoint json.RawMessage `json:"driverCheckpoint"`
}

type OpenResponse struct {
	// Flow runtime checkpoint to begin processing from. Optional.
	// If empty, the most recent checkpoint of the Flow recovery log is used.
	//
	// Or, a driver may send the value []byte{0xf8, 0xff, 0xff, 0xff, 0xf, 0x1}
	// to explicitly begin processing from a zero-valued checkpoint, effectively
	// rebuilding the materialization from scratch.
	RuntimeCheckpoint string `json:"runtimeCheckpoint"`
}

// Acknowledge to the connector that the previous transaction has committed
// to the Flow runtime's recovery log.
type AcknowledgeRequest struct{}

// Acknowledged follows Open and also StartedCommit, and tells the Flow Runtime
// that a previously started commit has completed.
//
// Acknowledged is _not_ a direct response to Request "acknowledge" command,
// and Acknowledge vs Acknowledged may be written in either order.
type AcknowledgeResponse struct{}

type LoadRequest struct {
	// Index of the Open binding for which this document is loaded.
	Binding int `json:"binding"`
	// Packed hexadecimal encoding of the key to load.
	// The packed encoding is order-preserving: given two instances of a
	// composite key K1 and K2, if K2 > K1 then K2's packed key is lexicographically
	// greater than K1's packed key.
	KeyPacked string `json:"keyPacked"`
	// Composite key to load.
	Key []interface{} `json:"key"`
}

type LoadResponse struct {
	// Index of the Open binding for which this document is loaded.
	Binding int `json:"binding"`
	// Loaded document.
	Doc json.RawMessage `json:"doc"`
}

type FlushRequest struct{}

type FlushResponse struct{}

type StoreRequest struct {
	// Index of the Open binding for which this document is stored.
	Binding int `json:"binding"`
	// Packed hexadecimal encoding of the key to store.
	KeyPacked string `json:"keyPacked"`
	// Composite key to store.
	Key []interface{} `json:"key"`
	// Array of selected, projected document values to store.
	Values []interface{} `json:"values"`
	// Complete JSON document to store.
	Doc json.RawMessage `json:"doc"`
	// Does this key exist in the endpoint?
	// True if this document was previously loaded or stored.
	// A SQL materialization might toggle between INSERT vs UPDATE behavior
	// depending on this value.
	Exists bool `json:"exists"`
}

type StartCommitRequest struct {
	// Opaque, base64-encoded Flow runtime checkpoint.
	// If the endpoint is authoritative, the connector must store this checkpoint
	// for a retrieval upon a future Open.
	RuntimeCheckpoint string `json:"runtimeCheckpoint"`
}

type StartCommitResponse struct {
	// Updated driver checkpoint to commit with this checkpoint.
	DriverCheckpoint json.RawMessage `json:"driverCheckpoint"`
	// Is this a partial update of the driver's checkpoint?
	// If true, then treat the driver checkpoint as a partial state update
	// which is incorporated into the full checkpoint as a RFC7396 Merge patch.
	// Otherwise the checkpoint is completely replaced.
	MergePatch bool `json:"mergePatch"`
}
