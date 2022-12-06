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
	"time"

	// importing tzdata is required so that time.LoadLocation can be used to validate timezones
	// without requiring timezone packages to be installed on the system.
	_ "time/tzdata"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	"github.com/estuary/connectors/materialize-s3-parquet/checkpoint"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	rockset "github.com/rockset/rockset-go-client"
	rtypes "github.com/rockset/rockset-go-client/openapi"
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

// eventTimeInfo is copied from rtypes.EventTimeInfo and modified to customize the JSON schema.
type eventTimeInfo struct {
	Field    string  `json:"field" jsonschema:"title=Field Name,description=Name of the field containing the event time"`
	Format   *string `json:"format,omitempty" jsonschema:"title=Format,description=Format of the time field,enum=milliseconds_since_epoch,enum=seconds_since_epoch"`
	TimeZone *string `json:"time_zone,omitempty" jsonschema:"title=Timezone,description=Default timezone, in IANA format"`
}

func (e *eventTimeInfo) Validate() error {
	if e.Field == "" {
		return fmt.Errorf("Event Time Info: Field Name is empty")
	}
	if e.TimeZone != nil {
		if _, err := time.LoadLocation(*e.TimeZone); err != nil {
			return fmt.Errorf("Event Time Info: invalid Timezone: %w", err)
		}
	}

	var validFormats = map[string]bool{
		"milliseconds_since_epoch": true,
		"seconds_since_epoch":      true,
	}
	if e.Format != nil {
		if !validFormats[*e.Format] {
			return fmt.Errorf("Event Time Info: Format is invalid")
		}
	}
	return nil
}

// fieldPartition was copied from rtypes.FieldPartition and modified both to customize the json
// schema and to remove unnecessary fields. Turns out that all the fields except for `FieldName` are
// seemingly unnecessary, beucause the only supported `type` is `AUTO`, and the
// [docs](https://rockset.com/docs/rest-api/#createcollection) say that the `keys` are not needed if
// type is `AUTO`.
type fieldPartition struct {
	FieldName *string `json:"field_name,omitempty" jsonschema:"title=Field Name,description=The name of a field\u002C parsed as a SQL qualified name"`
}

func (fp *fieldPartition) ToRocksetFieldPartition() rtypes.FieldPartition {
	// Go does not let you take the address of a constant directly :/
	// AUTO is listed in the docs as the only permissible value for Type, so we might as well set it
	// automatically.
	var partitionType = "AUTO"
	return rtypes.FieldPartition{
		FieldName: fp.FieldName,
		Type:      &partitionType,
	}
}

// collectionSettings exposes a subset of the "advanced" options on rtypes.CreateCollectionRequest
type collectionSettings struct {
	RetentionSecs *int64           `json:"retention_secs,omitempty" jsonschema:"title=Retention Period,description=Number of seconds after which data is purged based on event time"`
	EventTimeInfo *eventTimeInfo   `json:"event_time_info,omitempty" jsonschema:"title=Event Time Info"`
	ClusteringKey []fieldPartition `json:"clustering_key,omitempty" jsonschema:"title=Clustering Key,description=List of clustering fields"`
	InsertOnly    *bool            `json:"insert_only,omitempty" jsonschema:"title=Insert Only,description=If true disallows updates and deletes. The materialization will fail if there are documents with duplicate keys.,default=false"`
}

func (s *collectionSettings) Validate() error {
	if s.RetentionSecs != nil && *s.RetentionSecs < 0 {
		return fmt.Errorf("Retention Period cannot be negative")
	}
	if s.EventTimeInfo != nil {
		return s.EventTimeInfo.Validate()
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
	// Configures the rockset collection to bulk load an initial data set from an S3 bucket, before
	// transitioning to using the write API for ongoing data. If a previous version of this
	// materialization wrote files into S3 in order to more quickly backfill historical data, then
	// this value should contain configuration about the integration.  See:
	// https://go.estuary.dev/rock-bulk If a bulk loading integration is not being used, then this
	// should be undefined.
	InitializeFromS3 *cloudStorageIntegration `json:"initializeFromS3,omitempty" jsonschema:"title=Backfill from S3" jsonschema_extras:"advanced=true"`
}

// Configuration for bulk loading data into the new Rockset collection from a cloud storage bucket.
type cloudStorageIntegration struct {
	Integration string `json:"integration" jsonschema:"title=Integration Name,description=The name of the integration that was previously created in the Rockset UI"`
	Bucket      string `json:"bucket" jsonschema:"title=Bucket,description=The name of the S3 bucket to load data from."`
	Region      string `json:"region,omitempty" jsonschema:"title=Region,description=The AWS region in which the bucket resides. Optional."`
	Pattern     string `json:"pattern,omitempty" jsonschema:"title=Pattern,description=A regex that is used to match objects to be ingested, according to the rules specified in the rockset docs (https://rockset.com/docs/amazon-s3/#specifying-s3-path). Optional. Must not be set if 'prefix' is defined"`
	Prefix      string `json:"prefix,omitempty" jsonschema:"title=Prefix,description=Prefix of the data within the S3 bucket. All files under this prefix will be loaded. Optional. Must not be set if 'pattern' is defined."`
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
		DocumentationUrl:       "https://go.estuary.dev/materialize-rockset",
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
		if res, err = ResolveResourceConfig(binding.ResourceSpecJson); err != nil {
			return nil, fmt.Errorf("building resource for binding %v: %w", i, err)
		}
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
			ResourcePath: []string{res.Workspace, res.Collection},
			DeltaUpdates: true,
		})
	}
	var response = &pm.ValidateResponse{Bindings: bindings}

	return response, nil
}

// pm.DriverServer interface.
func (d *rocksetDriver) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var cfg, err = ResolveEndpointConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}

	client, err := rockset.NewClient(rockset.WithAPIKey(cfg.ApiKey))
	if err != nil {
		return nil, err
	}

	actionLog := []string{}
	for i, binding := range req.Materialization.Bindings {
		var res resource
		if res, err = ResolveResourceConfig(binding.ResourceSpecJson); err != nil {
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

	response := &pm.ApplyResponse{
		ActionDescription: strings.Join(actionLog, ", "),
	}

	return response, nil
}

// ApplyDelete implements pm.DriverServer and deletes all the rockset collections for all bindings.
// It does not attempt to delete any workspaces, even if they would be left empty. This is because
// deletion of collections is asynchronous and takes a while, and the workspace can't be deleted
// until all the deletions complete.
func (d *rocksetDriver) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var cfg, err = ResolveEndpointConfig(req.Materialization.EndpointSpecJson)
	if err != nil {
		return nil, err
	}
	client, err := rockset.NewClient(rockset.WithAPIKey(cfg.ApiKey))
	if err != nil {
		return nil, err
	}

	var actionDescription strings.Builder
	if req.DryRun {
		actionDescription.WriteString("Dry Run (skipping all actions):\n")
	}

	for _, binding := range req.Materialization.Bindings {
		var res resource
		// I think it's appropriate to ignore validation errors here, since we're in the process of
		// deleting this thing anyway. But we should only try to delete the rockset collection if
		// the resource validation is successful, since otherwise it's likely to result in a bad
		// request.
		if res, err = ResolveResourceConfig(binding.ResourceSpecJson); err != nil {
			log.WithFields(log.Fields{
				"resource": binding.ResourceSpecJson,
				"error":    err,
			}).Warn("Will skip deleting the Rockset collection for this binding because the resource spec failed validation")
			fmt.Fprintf(&actionDescription, "skipping deletion due to failed validation of resource: '%s', error: %s\n", string(binding.ResourceSpecJson), err)
			continue
		}

		fmt.Fprintf(&actionDescription, "Deleting Rockset Collection: '%s', Workspace: '%s'\n", res.Collection, res.Workspace)
		if req.DryRun {
			continue
		}
		var logEntry = log.WithFields(log.Fields{
			"rocksetCollection": res.Collection,
			"rocksetWorkspace":  res.Workspace,
		})
		var delErr = client.DeleteCollection(ctx, res.Workspace, res.Collection)
		if delErr != nil {
			if typedErr, ok := delErr.(rockset.Error); ok && typedErr.IsNotFoundError() {
				logEntry.Info("Did not delete the collection because it does not exist")
			} else {
				logEntry.WithField("error", delErr).Error("Failed to delete rockset collection")
				// We'll return the first deletion error we encounter, but only after deleting
				// the rest of the collections.
				if err == nil {
					err = delErr
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	return &pm.ApplyResponse{
		ActionDescription: actionDescription.String(),
	}, nil
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

	client, err := rockset.NewClient(rockset.WithAPIKey(cfg.ApiKey), rockset.WithAPIServer(cfg.RegionBaseUrl))
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
		if res, err := ResolveResourceConfig(spec.ResourceSpecJson); err != nil {
			return fmt.Errorf("building resource for binding %v: %w", i, err)
		} else {
			bindings = append(bindings, NewBinding(spec, &res))
		}
	}

	transactor := transactor{
		config:   &cfg,
		client:   client,
		bindings: bindings,
	}
	// Ensure that all the collections are ready to accept writes before returning the opened
	// response. It's important that we await _all_ bindings before continuing, since the flow
	// checkpoint that's embedded in the driver checkpoint (when there's an s3 integration) will be
	// cleared on the first successful transaction. Even collections without any integrations may
	// still take a while to become ready after they've been created, so this also ensures that
	// those are ready before we proceed (the error that's returned when attempting to write to a
	// non-ready collection is not considered retryable by the client library).
	transactor.awaitAllRocksetCollectionsReady(stream.Context())

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

func ResolveResourceConfig(specJson json.RawMessage) (resource, error) {
	var cfg = resource{}
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
