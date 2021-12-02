package materialize_rockset

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/alecthomas/jsonschema"
	pf "github.com/estuary/protocols/flow"
	pm "github.com/estuary/protocols/materialize"
	log "github.com/sirupsen/logrus"
)

type config struct {
	// Credentials used to authenticate with the Rockset API.
	ApiKey string `json:"api_key"`
	// Enable verbose logging of the HTTP calls to the Rockset API.
	HttpLogging bool `json:"http_logging"`
	// Field name which contains CDC change types are located. Values should be one of `["Insert", "Update", "Delete"]`
	ChangeIndicator string `json:"change_indicator"`
	// The upper limit on how many concurrent requests will be sent to Rockset.
	MaxConcurrentRequests int `json:"max_concurrent_requests" jsonschema:"default=1"`
}

func (c *config) Validate() error {
	var requiredProperties = [][]string{
		{"api_key", c.ApiKey},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}

	if c.MaxConcurrentRequests < 1 {
		return fmt.Errorf("max_concurrent_requests must be a positive integer. got: %v", c.MaxConcurrentRequests)
	}

	return nil
}

func configFromJson(json json.RawMessage) (*config, error) {
	var config = new(config)
	if err := pf.UnmarshalStrict(json, config); err != nil {
		return nil, fmt.Errorf("parsing Rockset configuration: %w", err)
	} else if err = config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid Rockset configuration: %w", err)
	}

	return config, nil
}

type resource struct {
	Workspace    string `json:"workspace,omitempty"`
	Collection   string `json:"collection,omitempty"`
	MaxBatchSize int    `json:"maxBatchSize,omitempty"`
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
	if r.MaxBatchSize == 0 {
		r.MaxBatchSize = 1000
	}

	return nil
}

func resourceFromJson(serialized *json.RawMessage) (*resource, error) {
	var res *resource

	if err := json.Unmarshal(*serialized, &res); err != nil {
		return nil, fmt.Errorf("parsing resource config: %w -- %s", err, *serialized)
	} else if err = res.Validate(); err != nil {
		return nil, fmt.Errorf("resource invalid: %w", err)
	}

	return res, nil
}

func validateRocksetName(field string, value string) error {
	// Alphanumeric or dash
	if match, err := regexp.MatchString("\\A[[:alnum:]-]+\\z", value); err != nil {
		return fmt.Errorf("malformed regexp: %v", err)
	} else if !match {
		return fmt.Errorf("%s must be alphanumeric. got: %s", field, value)
	}
	return nil

}

type rocksetDriver struct{}

func NewRocksetDriver() pm.DriverServer {
	return new(rocksetDriver)
}

// pm.DriverServer interface.
func (d *rocksetDriver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := jsonschema.Reflect(new(config)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}
	resourceSchema, err := jsonschema.Reflect(new(resource)).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.SpecResponse{
		EndpointSpecSchemaJson: json.RawMessage(endpointSchema),
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       "https://docs.estuary.dev",
	}, nil
}

// pm.DriverServer interface.
func (d *rocksetDriver) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	var bindings = []*pm.ValidateResponse_Binding{}
	for _, binding := range req.Bindings {
		res, err := resourceFromJson(&binding.ResourceSpecJson)
		if err != nil {
			return nil, err
		}

		var constraints = make(map[string]*pm.Constraint)
		for _, projection := range binding.Collection.Projections {
			var constraint = &pm.Constraint{}
			if projection.Inference.IsSingleScalarType() {
				constraint.Type = pm.Constraint_LOCATION_RECOMMENDED
				constraint.Reason = "The projection has a single scalar type."
			} else {
				constraint.Type = pm.Constraint_FIELD_OPTIONAL
				constraint.Reason = "The projection may materialize this field."
			}
			constraints[projection.Field] = constraint
		}

		bindings = append(bindings, &pm.ValidateResponse_Binding{
			Constraints:  constraints,
			ResourcePath: []string{res.Workspace, res.Collection, fmt.Sprintf("%v", res.MaxBatchSize)},
			DeltaUpdates: true,
		})
	}
	var response = &pm.ValidateResponse{Bindings: bindings}

	return response, nil
}

// pm.DriverServer interface.
func (d *rocksetDriver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	config, err := configFromJson(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	client, err := NewClient(config.ApiKey, config.HttpLogging)
	if err != nil {
		return nil, err
	}

	actionLog := []string{}
	for i, binding := range req.Materialization.Bindings {
		res, err := resourceFromJson(&binding.ResourceSpecJson)
		if err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}

		if createdWorkspace, err := createNewWorkspace(ctx, client, res.Workspace); err != nil {
			return nil, err
		} else if createdWorkspace != nil {
			actionLog = append(actionLog, fmt.Sprintf("created %s workspace", createdWorkspace.Name))
		}

		if createdCollection, err := createNewCollection(ctx, client, res.Workspace, res.Collection); err != nil {
			return nil, err
		} else if createdCollection != nil {
			actionLog = append(actionLog, fmt.Sprintf("created %s collection", createdCollection.Name))
		}
	}

	response := &pm.ApplyResponse{
		ActionDescription: strings.Join(actionLog, ", "),
	}

	return response, nil
}

func (d *rocksetDriver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	// TODO: delete Rockset resources now that we can clean this up as part of the real protocol.
	return nil, nil
}

// pm.DriverServer interface.
func (d *rocksetDriver) Transactions(stream pm.Driver_TransactionsServer) error {
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected Open, got %#v", open)
	}

	config, err := configFromJson(open.Open.Materialization.EndpointSpecJson)
	if err != nil {
		return err
	}

	client, err := NewClient(config.ApiKey, config.HttpLogging)
	if err != nil {
		return err
	}

	var bindings = make([]*binding, 0, len(open.Open.Materialization.Bindings))
	for i, spec := range open.Open.Materialization.Bindings {
		res, err := resourceFromJson(&spec.ResourceSpecJson)
		if err != nil {
			return fmt.Errorf("building resource for binding %v: %w", i, err)
		}
		bindings = append(bindings, NewBinding(spec, res))
	}

	transactor := transactor{
		ctx:      stream.Context(),
		config:   config,
		client:   client,
		bindings: bindings,
	}

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: nil},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	log := log.NewEntry(log.StandardLogger())

	return pm.RunTransactions(stream, &transactor, log)
}

func RandString(len int) string {
	var buffer = make([]byte, len)
	if _, err := rand.Read(buffer); err != nil {
		panic("failed to generate random string")
	}
	return hex.EncodeToString(buffer)
}
