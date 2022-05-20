package materialize_rockset

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/connectors/materialize-s3-parquet/checkpoint"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	rockset "github.com/rockset/rockset-go-client"
	log "github.com/sirupsen/logrus"
)

type config struct {
	// Credentials used to authenticate with the Rockset API.
	ApiKey string `json:"api_key"`
	// TODO: remove HttpLogging
	// Enable verbose logging of the HTTP calls to the Rockset API.
	HttpLogging bool `json:"http_logging"`
	// TODO: remove MaxConcurrentRequests
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

type resource struct {
	Workspace  string `json:"workspace,omitempty"`
	Collection string `json:"collection,omitempty"`
	// Configures the rockset collection to bulk load an initial data set from an S3 bucket, before transitioning to
	// using the write API for ongoing data.
	// If a previous version of this materialization wrote files into S3 in order to more quickly backfill historical
	// data, then this value should contain configuration about the integration.  See: https://go.estuary.dev/rock-bulk
	// If a bulk loading integration is not being used, then this should be undefined.
	InitializeFromS3 *cloudStorageIntegration `json:"initializeFromS3,omitempty"`
	MaxBatchSize     int                      `json:"maxBatchSize,omitempty"`
}

// Configuration for bulk loading data into the new Rockset collection from a cloud storage bucket.
type cloudStorageIntegration struct {
	// The name of the integration in Rockset, which would have been created in the Rockset UI.
	Integration string `json:"integration"`
	// The name of the S3 bucket to load data from.
	Bucket string `json:"bucket"`
	// The region of the S3 bucket. Optional.
	Region string `json:"region,omitempty"`
	// A regex that is used to match objects to be ingested, according to the rules specified in the [rockset
	// docs](https://rockset.com/docs/amazon-s3/#specifying-s3-path). Optional. Must not be set if 'prefix' is defined.
	Pattern string `json:"pattern,omitempty"`
	// Prefix of the data within the S3 bucket. All files under this prefix will be loaded. Optional. Must not be set if
	// 'pattern' is defined.
	Prefix string `json:"prefix,omitempty"`
}

func (c *cloudStorageIntegration) Validate() error {
	var requiredProperties = [][]string{
		{"integration", c.Integration},
		{"bucket", c.Bucket},
	}
	for _, req := range requiredProperties {
		if req[1] == "" {
			return fmt.Errorf("missing '%s'", req[0])
		}
	}
	if c.Pattern != "" && c.Prefix != "" {
		return fmt.Errorf("'pattern' and 'prefix' can both be used together")
	}
	return nil
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

	if r.InitializeFromS3 != nil {
		if err := r.InitializeFromS3.Validate(); err != nil {
			return fmt.Errorf("invalid 'initializeFromS3' value: %w", err)
		}
	}

	return nil
}

func (r *resource) SetDefaults() {
	if r.MaxBatchSize == 0 {
		r.MaxBatchSize = 1000
	}
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

func NewRocksetDriver() pm.DriverServer {
	return new(rocksetDriver)
}

// pm.DriverServer interface.
func (d *rocksetDriver) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
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
	cfg, err := ResolveEndpointConfig(req.EndpointSpecJson)
	if err != nil {
		return nil, err
	}
	client, err := rockset.NewClient(rockset.WithAPIKey(cfg.ApiKey))
	if err != nil {
		return nil, fmt.Errorf("creating Rockset client: %w", err)
	}

	var bindings = []*pm.ValidateResponse_Binding{}
	for i, binding := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}
		res.SetDefaults()
		rocksetCollection, err := getCollection(ctx, client, res.Workspace, res.Collection)
		if err != nil {
			return nil, fmt.Errorf("requesting rockset collection: %w", err)
		}
		// If the binding specifies a bulkLoadIntegration and the collection already exists,
		// then it must already have the integration, since the collection definition is immutable.
		if rocksetCollection != nil &&
			res.InitializeFromS3 != nil &&
			GetS3IntegrationSource(rocksetCollection, res.InitializeFromS3.Integration) == nil {
			return nil, fmt.Errorf("Rockset collection '%s' does not have an integration named '%s', which is required by the binding '%s'",
				res.Collection, res.InitializeFromS3.Integration, binding.Collection.Collection.String())
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
	var cfg config = config{}
	if err := pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, &cfg); err != nil {
		return nil, fmt.Errorf("parsing Rockset config: %w", err)
	}

	client, err := rockset.NewClient(rockset.WithAPIKey(cfg.ApiKey))
	if err != nil {
		return nil, err
	}

	actionLog := []string{}
	for i, binding := range req.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}
		res.SetDefaults()

		if createdWorkspace, err := createNewWorkspace(ctx, client, res.Workspace); err != nil {
			return nil, err
		} else if createdWorkspace != nil {
			actionLog = append(actionLog, fmt.Sprintf("created %s workspace", *createdWorkspace.Name))
		}

		if createdCollection, err := createNewCollection(ctx, client, res.Workspace, res.Collection, res.InitializeFromS3); err != nil {
			return nil, err
		} else if createdCollection {
			actionLog = append(actionLog, fmt.Sprintf("created %s collection", res.Collection))
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

	cfg, err := ResolveEndpointConfig(open.Open.Materialization.EndpointSpecJson)
	if err != nil {
		return err
	}

	client, err := rockset.NewClient(rockset.WithAPIKey(cfg.ApiKey))
	if err != nil {
		return err
	}

	// It's possible that there was a previous materialization with the same name as this one, and that it may have left
	// a driver checkpoint. This is expected in the case that a user wishes to use a cloud storage integration in
	// Rockset for bulk loading data for a large backfill as described here: https://go.estuary.dev/rock-bulk
	// If such a checkpoint is left over, then we'll return it here. The driver checkpoint will then be cleared out
	// on the next successful commit of a transaction.
	var flowCheckpoint []byte
	var s3DriverCheckpoint checkpoint.DriverCheckpoint
	if len(open.Open.DriverCheckpointJson) > 0 {
		if err = json.Unmarshal(open.Open.DriverCheckpointJson, &s3DriverCheckpoint); err != nil {
			return fmt.Errorf("unmarshaling S3 driver checkpoint: %w", err)
		}
		if s3DriverCheckpoint.B64EncodedFlowCheckpoint != "" {
			log.WithField("s3DriverCheckpoint", open.Open.DriverCheckpointJson).Info("Using driver checkpoint from a prior cloud storage materialization connector")
			if flowCheckpoint, err = base64.StdEncoding.DecodeString(s3DriverCheckpoint.B64EncodedFlowCheckpoint); err != nil {
				return fmt.Errorf("decoding base64 checkpoint: %w", err)
			}
		}
	}

	var bindings = make([]*binding, 0, len(open.Open.Materialization.Bindings))
	for i, spec := range open.Open.Materialization.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(spec.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("building resource for binding %v: %w", i, err)
		}
		res.SetDefaults()
		bindings = append(bindings, NewBinding(spec, &res))
	}

	transactor := transactor{
		config:   &cfg,
		client:   client,
		bindings: bindings,
	}
	// If any bindings have an integration named as part of their resource spec, then await them now, before returning
	// the opened response. It's important that we await _all_ bindings that are bulk loading before continuing, since
	// the flow checkpoint that's embedded in the driver checkpoint will be cleared on the first successful transaction.
	transactor.awaitAllIntegrationIngestCompletions(stream.Context())

	if err = stream.Send(&pm.TransactionResponse{
		Opened: &pm.TransactionResponse_Opened{FlowCheckpoint: flowCheckpoint},
	}); err != nil {
		return fmt.Errorf("sending Opened: %w", err)
	}

	log := log.NewEntry(log.StandardLogger())

	return pm.RunTransactions(stream, &transactor, log)
}

func ResolveEndpointConfig(specJson json.RawMessage) (config, error) {
	var cfg = config{}
	if err := pf.UnmarshalStrict(specJson, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing Rockset config: %w", err)
	}
	return cfg, cfg.Validate()
}

func RandString(len int) string {
	var buffer = make([]byte, len)
	if _, err := rand.Read(buffer); err != nil {
		panic("failed to generate random string")
	}
	return hex.EncodeToString(buffer)
}
