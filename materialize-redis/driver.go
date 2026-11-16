package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	networkTunnel "github.com/estuary/connectors/go/network-tunnel"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/redis/go-redis/v9"
)

type config struct {
	Address    string `json:"address" jsonschema:"title=Address,description=Address of your Redis server or initial Sentinel endpoint in host:port format.,minLength=1" jsonschema_extras:"order=0"`
	Username   string `json:"username,omitempty" jsonschema:"title=Username,description=Username for authenticating to your Redis server." jsonschema_extras:"order=1"`
	Password   string `json:"password,omitempty" jsonschema:"title=Password,description=Password for authenticating to your Redis server." jsonschema_extras:"secret=true,order=2"`
	Database   int    `json:"database,omitempty" jsonschema:"title=Database,description=Redis database number to materialize into.,default=0,minimum=0" jsonschema_extras:"order=3"`
	TLS        bool   `json:"tls,omitempty" jsonschema:"title=TLS,description=Connect to your Redis server using TLS.,default=false" jsonschema_extras:"order=4"`
	HardDelete bool   `json:"hardDelete,omitempty" jsonschema:"title=Hard Delete,description=Delete Redis keys when source documents are deleted. Otherwise store deletion documents.,default=false" jsonschema_extras:"order=5"`

	Advanced      advancedConfig              `json:"advanced,omitempty" jsonschema:"title=Advanced Options,description=Custom TLS settings. Requires TLS to be enabled." jsonschema_extras:"advanced=true"`
	Sentinel      *sentinelConfig             `json:"sentinel,omitempty" jsonschema:"title=Sentinel,description=Discover the current primary using Redis Sentinel. Address is used as the initial Sentinel endpoint."`
	NetworkTunnel *networkTunnel.TunnelConfig `json:"networkTunnel,omitempty" jsonschema:"title=Network Tunnel,description=Connect to your system through an SSH server that acts as a bastion host for your network."`

	startTunnel func(context.Context, string) (string, error)
}

func (c config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("missing 'address'")
	}
	if err := validateAddress(c.Address); err != nil {
		return err
	}
	if c.Database < 0 {
		return fmt.Errorf("database must be non-negative")
	}
	return c.validateConnection()
}

type resource struct {
	KeyPrefix    string `json:"keyPrefix" jsonschema:"title=Key Prefix,description=Prefix for Redis keys. Collection keys are appended using the selected key encoding.,minLength=1" jsonschema_extras:"x-collection-name=true"`
	TTLSeconds   int64  `json:"ttlSeconds,omitempty" jsonschema:"title=TTL Seconds,description=Lifetime of each Redis key in seconds after it is written. Zero disables expiry.,default=0,minimum=0,maximum=9223372036"`
	DeltaUpdates bool   `json:"delta_updates,omitempty" jsonschema:"title=Delta Updates,description=Write documents without first loading their existing values.,default=false" jsonschema_extras:"x-delta-updates=true"`
	Format       string `json:"format,omitempty" jsonschema:"title=Document Format,description=Store documents as Redis strings or RedisJSON values. JSON requires RedisJSON support.,enum=string,enum=json,default=string"`
	KeyEncoding  string `json:"keyEncoding,omitempty" jsonschema:"title=Key Encoding,description=Legacy joins key values with colons. JSON encodes the complete key tuple as a JSON array to prevent collisions.,enum=legacy,enum=json,default=legacy"`
}

func (r resource) Validate() error {
	if r.KeyPrefix == "" {
		return fmt.Errorf("missing 'keyPrefix'")
	}
	if r.TTLSeconds < 0 || r.TTLSeconds > math.MaxInt64/int64(time.Second) {
		return fmt.Errorf("ttlSeconds must be between 0 and %d", math.MaxInt64/int64(time.Second))
	}
	if r.Format != "" && r.Format != "string" && r.Format != "json" {
		return fmt.Errorf("format must be string or json")
	}
	if r.KeyEncoding != "" && r.KeyEncoding != "legacy" && r.KeyEncoding != "json" {
		return fmt.Errorf("keyEncoding must be legacy or json")
	}
	return nil
}

type driver struct {
	newClient func(config) (*redis.Client, error)
}

var _ boilerplate.Connector = &driver{}

// NewDriver builds the connector, and is its entry point.
func NewDriver() boilerplate.Connector { return driver{newClient: config.client} }

func (d driver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	endpointSchema, err := schemagen.GenerateSchema("Materialize Redis Spec", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("Redis Keys", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}
	return boilerplate.RunSpec(ctx, req, "https://go.estuary.dev/materialize-redis", endpointSchema, resourceSchema)
}

func (d driver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	resources := make([]resource, len(req.Bindings))
	current := make(map[string]bool)
	for i, b := range req.Bindings {
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &resources[i]); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		current[resources[i].KeyPrefix] = true
	}

	var out []*pm.Response_Validated_Binding
	prefixes := reservedPrefixes(req.LastMaterialization)
	for i, b := range req.Bindings {
		res := resources[i]
		if err := validateKeyPrefix(res.KeyPrefix, prefixes, current); err != nil {
			return nil, err
		}
		prefixes[res.KeyPrefix] = true

		last := boilerplate.FindLastBinding([]string{res.KeyPrefix}, req.LastMaterialization)
		if last != nil && last.DeltaUpdates && !res.DeltaUpdates && b.Backfill <= last.Backfill {
			return nil, fmt.Errorf("changing from delta updates to standard updates is not allowed without a backfill")
		}
		if last != nil && len(last.ResourceConfigJson) != 0 {
			var previous resource
			if err := pf.UnmarshalStrict(last.ResourceConfigJson, &previous); err != nil {
				return nil, fmt.Errorf("parsing previous resource config: %w", err)
			}
			if (previous.Format == "json") != (res.Format == "json") || (previous.KeyEncoding == "json") != (res.KeyEncoding == "json") {
				if res.DeltaUpdates || b.Backfill <= last.Backfill {
					return nil, fmt.Errorf("changing format or keyEncoding requires a new keyPrefix or a standard-update backfill")
				}
			}
		}

		collection := req.BindingCollection(b)
		if collection == nil {
			return nil, fmt.Errorf("missing collection for keyPrefix %q", res.KeyPrefix)
		}
		var constraints []*pm.Response_Validated_ProjectionConstraint
		for _, projection := range collection.Projections {
			constraint := projectionConstraint(projection)
			constraints = append(constraints, &pm.Response_Validated_ProjectionConstraint{
				Field:      projection.Field,
				Constraint: &constraint,
			})
		}
		out = append(out, &pm.Response_Validated_Binding{
			ProjectionConstraints: constraints,
			ResourcePath:          []string{res.KeyPrefix},
			DeltaUpdates:          res.DeltaUpdates,
		})
	}

	client, err := d.newClient(cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to Redis: %w", err)
	}
	return &pm.Response_Validated{Bindings: out}, nil
}

func reservedPrefixes(specs ...*pf.MaterializationSpec) map[string]bool {
	prefixes := make(map[string]bool)
	for _, spec := range specs {
		if spec == nil {
			continue
		}
		// Disabled and removed bindings retain their keys, including in this publication.
		for _, bindings := range [][]*pf.MaterializationSpec_Binding{spec.Bindings, spec.InactiveBindings} {
			for _, b := range bindings {
				if len(b.ResourcePath) > 0 {
					prefixes[b.ResourcePath[0]] = false // Allow exact-prefix reuse.
				}
			}
		}
	}
	return prefixes
}

func validateKeyPrefix(prefix string, prefixes, current map[string]bool) error {
	if prefixes[prefix] {
		return fmt.Errorf("duplicate keyPrefix %q", prefix)
	}
	for reserved := range prefixes {
		if strings.HasPrefix(prefix, reserved+":") || strings.HasPrefix(reserved, prefix+":") {
			err := fmt.Errorf("overlapping keyPrefixes %q and %q", reserved, prefix)
			if !current[reserved] {
				return fmt.Errorf("%w: prefixes of disabled or previously removed bindings remain reserved to protect retained keys; choose a non-nested keyPrefix", err)
			}
			return err
		}
	}
	return nil
}

func projectionConstraint(p pf.Projection) pm.Response_Validated_Constraint {
	var constraint pm.Response_Validated_Constraint
	switch {
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "The full document must be materialized"
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "Collection keys are required to construct Redis keys"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Redis stores the full document"
	}
	return constraint
}

// appliedState lets runtime-next resume without the previous specification.
// Legacy flowctl-v1 previews do not persist or supply Apply state.
type appliedState struct {
	Name     pf.Materialization `json:"name"`
	Bindings map[string]uint32  `json:"bindings"`
}

func (d driver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	var cfg config
	if err := pf.UnmarshalStrict(req.Materialization.ConfigJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing endpoint config: %w", err)
	}
	var previous appliedState
	if len(req.StateJson) > 0 {
		if err := json.Unmarshal(req.StateJson, &previous); err != nil {
			return nil, fmt.Errorf("parsing applied state: %w", err)
		}
	}
	next := appliedState{Name: req.Materialization.Name, Bindings: make(map[string]uint32)}
	if previous.Name == next.Name {
		for _, b := range req.Materialization.InactiveBindings {
			if len(b.ResourcePath) > 0 {
				if backfill, ok := previous.Bindings[b.ResourcePath[0]]; ok {
					next.Bindings[b.ResourcePath[0]] = backfill
				}
			}
		}
	}
	current := make(map[string]bool)
	var prefixes []string
	for _, b := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}
		current[res.KeyPrefix] = true
		if res.DeltaUpdates {
			continue
		}
		next.Bindings[res.KeyPrefix] = b.Backfill
		// Matching saved state preserves keys even without the last specification.
		if backfill, ok := previous.Bindings[res.KeyPrefix]; ok && previous.Name == next.Name && backfill == b.Backfill {
			continue
		}
		last := boilerplate.FindLastBinding(b.ResourcePath, req.LastMaterialization)
		collection := req.Materialization.BindingCollection(b)
		if collection == nil {
			return nil, fmt.Errorf("missing collection for keyPrefix %q", res.KeyPrefix)
		}
		if last == nil && b.Backfill == 0 && canRetainPrefix(collection.GetReadSchemaJson()) {
			continue
		}
		if last == nil || b.Backfill > last.Backfill {
			prefixes = append(prefixes, res.KeyPrefix)
		}
	}
	var response pm.Response_Applied
	if previous.Name != next.Name || !maps.Equal(previous.Bindings, next.Bindings) {
		state, err := json.Marshal(next)
		if err != nil {
			return nil, fmt.Errorf("encoding applied state: %w", err)
		}
		response.State = &pf.ConnectorState{UpdatedJson: state}
	}
	if len(prefixes) == 0 {
		return &response, nil
	}

	reserved := reservedPrefixes(req.Materialization, req.LastMaterialization)
	for _, prefix := range prefixes {
		if err := validateKeyPrefix(prefix, reserved, current); err != nil {
			return nil, err
		}
	}

	client, err := d.newClient(cfg)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	var actions []string
	for _, prefix := range prefixes {
		// Replayed reductions must not load values from an earlier materialization or backfill.
		if err := clearPrefix(ctx, client, prefix); err != nil {
			return nil, err
		}
		actions = append(actions, fmt.Sprintf("Cleared Redis keys for keyPrefix %q", prefix))
	}
	response.ActionDescription = strings.Join(actions, "\n")
	return &response, nil
}

func canRetainPrefix(raw json.RawMessage) bool {
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil || schema == nil {
		return false
	}
	// Without other reductions, Flow replaces whole documents. Be conservative about references.
	var onlyReplacements func(any) bool
	onlyReplacements = func(node any) bool {
		switch node := node.(type) {
		case map[string]any:
			for name, child := range node {
				switch name {
				case "$ref", "$dynamicRef", "$recursiveRef":
					return false
				case "reduce":
					reduction, ok := child.(map[string]any)
					if !ok || reduction["strategy"] != "lastWriteWins" {
						return false
					}
				case "properties", "patternProperties", "$defs", "definitions", "dependentSchemas", "dependencies":
					schemas, ok := child.(map[string]any)
					if !ok {
						return false
					}
					for _, schema := range schemas {
						if !onlyReplacements(schema) {
							return false
						}
					}
				case "items", "prefixItems", "additionalItems", "additionalProperties", "not", "if", "then", "else",
					"allOf", "anyOf", "oneOf", "contains", "propertyNames", "unevaluatedItems", "unevaluatedProperties", "contentSchema":
					if !onlyReplacements(child) {
						return false
					}
				}
			}
		case []any:
			for _, child := range node {
				if !onlyReplacements(child) {
					return false
				}
			}
		}
		return true
	}
	return onlyReplacements(schema)
}

func clearPrefix(ctx context.Context, client *redis.Client, prefix string) error {
	const batchSize = 1000
	pattern := strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`).Replace(prefix) + ":*"
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, pattern, batchSize).Result()
		if err != nil {
			return fmt.Errorf("scanning Redis keys for keyPrefix %q: %w", prefix, err)
		}
		// SCAN's count is a hint, so limit UNLINK batches independently.
		for len(keys) > 0 {
			n := min(len(keys), batchSize)
			if err := client.Unlink(ctx, keys[:n]...).Err(); err != nil {
				return fmt.Errorf("clearing Redis keys for keyPrefix %q: %w", prefix, err)
			}
			keys = keys[n:]
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func (d driver) NewTransactor(ctx context.Context, open pm.Request_Open, be *m.BindingEvents) (m.Transactor, *pm.Response_Opened, *m.MaterializeOptions, error) {
	if err := boilerplate.RequireSingleShard(open); err != nil {
		return nil, nil, nil, err
	}
	var cfg config
	if err := pf.UnmarshalStrict(open.Materialization.ConfigJson, &cfg); err != nil {
		return nil, nil, nil, fmt.Errorf("parsing endpoint config: %w", err)
	}

	var bindings []binding
	for _, b := range open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, nil, fmt.Errorf("parsing resource config: %w", err)
		}
		bindings = append(bindings, binding{resource: res, path: b.ResourcePath})
	}

	client, err := d.newClient(cfg)
	if err != nil {
		return nil, nil, nil, err
	}
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, nil, nil, fmt.Errorf("connecting to Redis: %w", err)
	}
	return &transactor{
		client:     client,
		bindings:   bindings,
		hardDelete: cfg.HardDelete,
		be:         be,
	}, &pm.Response_Opened{DisableLoadOptimization: true}, &m.MaterializeOptions{ExtendedLogging: true}, nil
}
