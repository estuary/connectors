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
	// JSON pointer to the field in the document where CDC change types are located.
	ChangeIndicator string `json:"change_indicator"`
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
	Workspace  string `json:"workspace,omitempty"`
	Collection string `json:"collection,omitempty"`
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
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		} else if err = res.Validate(); err != nil {
			return nil, fmt.Errorf("resource invalid: %w", err)
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
			ResourcePath: []string{res.Workspace, res.Collection},
			DeltaUpdates: true,
		})
	}
	var response = &pm.ValidateResponse{Bindings: bindings}

	return response, nil
}

// pm.DriverServer interface.
func (d *rocksetDriver) Apply(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	config, err := configFromJson(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	client, err := NewClient(config.ApiKey, config.HttpLogging)
	if err != nil {
		return nil, err
	}

	actionLog := []string{}
	for _, binding := range req.Materialization.Bindings {
		workspace := binding.ResourcePath[0]
		collection := binding.ResourcePath[1]

		if createdWorkspace, err := createNewWorkspace(ctx, client, workspace); err != nil {
			return nil, err
		} else if createdWorkspace != nil {
			actionLog = append(actionLog, fmt.Sprintf("created %s workspace", createdWorkspace.Name))
		}

		if createdCollection, err := createNewCollection(ctx, client, workspace, collection); err != nil {
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
	for _, spec := range open.Open.Materialization.Bindings {
		bindings = append(bindings, &binding{
			spec:       spec,
			operations: make(map[int][]json.RawMessage),
		})

	}

	transactor := transactor{
		ctx:      stream.Context(),
		config:   config,
		client:   client,
		bindings: bindings,
	}

	if err = stream.Send(&pm.TransactionResponse{
		// TODO: should I be doing anything with this FlowCheckpoint? I don't *think* that's appropriate for Rockset...
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
