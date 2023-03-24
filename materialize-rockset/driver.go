package materialize_rockset

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	// importing tzdata is required so that time.LoadLocation can be used to validate timezones
	// without requiring timezone packages to be installed on the system.
	_ "time/tzdata"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	rockset "github.com/rockset/rockset-go-client"
	"github.com/rockset/rockset-go-client/option"
	log "github.com/sirupsen/logrus"
)

type config struct {
	RegionBaseUrl string `json:"region_base_url" jsonschema:"title=Region Base URL,description=The base URL to connect to your Rockset deployment. Example: api.usw2a1.rockset.com (do not include the protocol).,enum=api.usw2a1.rockset.com,enum=api.use1a1.rockset.com,enum=api.euc1a1.rockset.com" jsonschema_extras:"multiline=true,order=0"`
	ApiKey        string `json:"api_key" jsonschema:"title=Rockset API Key,description=The key used to authenticate to the Rockset API. Must have role of admin or member." jsonschema_extras:"secret=true,multiline=true,order=1"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"api_key", c.ApiKey},
		{"region_base_url", c.RegionBaseUrl},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	return nil
}

func (c *config) client() (*rockset.RockClient, error) {
	return rockset.NewClient(rockset.WithAPIKey(c.ApiKey), rockset.WithAPIServer(c.RegionBaseUrl))
}

// fieldPartition was copied from rtypes.FieldPartition and modified both to customize the json
// schema and to remove unnecessary fields. Turns out that all the fields except for `FieldName` are
// seemingly unnecessary, beucause the only supported `type` is `AUTO`, and the
// [docs](https://rockset.com/docs/rest-api/#createcollection) say that the `keys` are not needed if
// type is `AUTO`.
type fieldPartition struct {
	FieldName string `json:"field_name" jsonschema:"title=Field Name,description=The name of a field\u002C parsed as a SQL qualified name"`
}

func (f *fieldPartition) toOpt() option.CollectionOption {
	return option.WithCollectionClusteringKey(
		f.FieldName,
		"AUTO",
		[]string{},
	)
}

// collectionSettings exposes a subset of the "advanced" options on rtypes.CreateCollectionRequest
type collectionSettings struct {
	RetentionSecs *int64           `json:"retention_secs,omitempty" jsonschema:"title=Retention Period,description=Number of seconds after which data is purged based on event time"`
	ClusteringKey []fieldPartition `json:"clustering_key,omitempty" jsonschema:"title=Clustering Key,description=List of clustering fields"`
}

func (s *collectionSettings) Validate() error {
	if s.RetentionSecs != nil && *s.RetentionSecs < 0 {
		return fmt.Errorf("retention period cannot be negative")
	}
	// nothing to validate on ClusteringKey
	return nil
}

type resource struct {
	Workspace string `json:"workspace" jsonschema:"title=Workspace,description=The name of the Rockset workspace (will be created if it does not exist)"`
	// The name of the Rockset collection (will be created if it does not exist)
	Collection string `json:"collection" jsonschema:"title=Rockset Collection,description=The name of the Rockset collection (will be created if it does not exist)" jsonschema_extras:"x-collection-name=true"`
	// Additional settings for creating the Rockset collection, which are likely to be rarely used.
	AdvancedCollectionSettings *collectionSettings `json:"advancedCollectionSettings,omitempty" jsonschema:"title=Advanced Collection Settings" jsonschema_extras:"advanced=true"`
}

func (r *resource) Validate() error {
	var requiredProperties = [][]string{
		{"workspace", r.Workspace},
		{"collection", r.Collection},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if err := validateRocksetName("workspace", r.Workspace); err != nil {
		return err
	}
	if err := validateRocksetName("collection", r.Collection); err != nil {
		return err
	}

	return nil
}

func validateRocksetName(field string, value string) error {
	// Alphanumeric or dash
	if match, err := regexp.MatchString("\\A[[:alnum:]_-]+\\z", value); err != nil {
		return fmt.Errorf("malformed regexp: %v", err)
	} else if !match {
		return fmt.Errorf("%s must be alphanumeric. got: %s", field, value)
	}
	return nil

}

type rocksetDriver struct{}

func NewRocksetDriver() *rocksetDriver {
	return new(rocksetDriver)
}

// pm.DriverServer interface.
func (d *rocksetDriver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := schemagen.GenerateSchema("Rockset Endpoint", &config{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := schemagen.GenerateSchema("Rockset Collection", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         "https://go.estuary.dev/materialize-rockset",
	}, nil
}

// pm.DriverServer interface.
func (d *rocksetDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var bindings = []*pm.Response_Validated_Binding{}
	for i, binding := range req.Bindings {
		res, err := ResolveResourceConfig(binding.ResourceConfigJson)
		if err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}

		var constraints = make(map[string]*pm.Response_Validated_Constraint)
		for _, projection := range binding.Collection.Projections {
			var constraint = &pm.Response_Validated_Constraint{}
			if projection.Inference.IsSingleScalarType() {
				constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "The projection has a single scalar type."
			} else {
				constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
				constraint.Reason = "The projection may materialize this field."
			}
			constraints[projection.Field] = constraint
		}

		bindings = append(bindings, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			ResourcePath: []string{res.Workspace, res.Collection},
			DeltaUpdates: true,
		})
	}
	var response = &pm.Response_Validated{Bindings: bindings}

	return response, nil
}

func (d *rocksetDriver) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
	var cfg, err = ResolveEndpointConfig(req.Materialization.ConfigJson)
	if err != nil {
		return nil, err
	}

	client, err := cfg.client()
	if err != nil {
		return nil, err
	}

	actionLog := []string{}
	for i, binding := range req.Materialization.Bindings {
		var res resource
		if res, err = ResolveResourceConfig(binding.ResourceConfigJson); err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}

		if createdWorkspace, err := ensureWorkspaceExists(ctx, client, res.Workspace); err != nil {
			return nil, err
		} else if createdWorkspace != nil {
			actionLog = append(actionLog, fmt.Sprintf("created %s workspace", *createdWorkspace.Name))
		}

		if createdCollection, err := ensureCollectionExists(ctx, client, &res); err != nil {
			return nil, err
		} else if createdCollection {
			actionLog = append(actionLog, fmt.Sprintf("created %s collection", res.Collection))
		}
	}

	response := &pm.Response_Applied{
		ActionDescription: strings.Join(actionLog, ", "),
	}

	return response, nil
}

func (d *rocksetDriver) NewTransactor(ctx context.Context, open pm.Request_Open) (pm.Transactor, *pm.Response_Opened, error) {
	cfg, err := ResolveEndpointConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	client, err := cfg.client()
	if err != nil {
		return nil, nil, err
	}

	var bindings = make([]*binding, 0, len(open.Materialization.Bindings))
	for i, spec := range open.Materialization.Bindings {
		if res, err := ResolveResourceConfig(spec.ResourceConfigJson); err != nil {
			return nil, nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		} else {
			bindings = append(bindings, NewBinding(spec, &res))
		}
	}

	// Ensure that all the collections are ready to accept writes before returning the opened
	// response. The error that's returned when attempting to write to a non-ready collection is
	// not considered retryable by the client library.
	log.Info("Waiting for Rockset collections to be ready to accept writes")
	for _, b := range bindings {
		if err := client.WaitUntilCollectionReady(ctx, b.rocksetWorkspace(), b.rocksetCollection()); err != nil {
			return nil, nil, fmt.Errorf("waiting for rockset collection %q to be ready: %w", b.rocksetCollection(), err)
		}
		log.WithFields(log.Fields{
			"workspace":  b.rocksetWorkspace(),
			"collection": b.rocksetCollection(),
		}).Info("Rockset collection is ready to accept writes")
	}
	log.Info("All Rockset collections are ready to accept writes")

	return &transactor{
		config:   &cfg,
		client:   client,
		bindings: bindings,
	}, &pm.Response_Opened{}, nil
}

func ResolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing Rockset config: %w", err)
	}
	return cfg, cfg.Validate()
}

func ResolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var cfg = resource{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing Rockset config: %w", err)
	}
	return cfg, cfg.Validate()
}
