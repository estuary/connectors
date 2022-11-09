package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// Resource represents the capture configuration of a single table.
type Resource struct {
	Namespace  string   `json:"namespace,omitempty" jsonschema:"title=Namespace,description=The schema (namespace) in which the table resides."`
	Stream     string   `json:"stream" jsonschema:"title=Table Name,description=The name of the table to be captured."`
	PrimaryKey []string `json:"primary_key,omitempty" jsonschema:"title=Primary Key Columns,description=The columns which together form the primary key of the table."`
}

// Validate checks to make sure a resource appears usable.
func (r Resource) Validate() error {
	if r.Namespace == "" {
		return fmt.Errorf("table namespace unspecified")
	}
	if r.Stream == "" {
		return fmt.Errorf("table name unspecified")
	}
	return nil
}

// Binding represents a capture binding, and includes a Resource config and a binding index.
type Binding struct {
	Index    uint32
	Resource Resource
}

// Driver is an implementation of the pc.DriverServer interface which performs
// captures from a SQL database.
type Driver struct {
	ConfigSchema     json.RawMessage
	DocumentationURL string

	Connect func(ctx context.Context, cfg json.RawMessage) (Database, error)
}

// Spec returns the specification definition of this driver.
// Notably this includes its endpoint and resource configuration JSON schema.
func (d *Driver) Spec(ctx context.Context, req *pc.SpecRequest) (*pc.SpecResponse, error) {
	var resourceSchema, err = schemagen.GenerateSchema("SQL Database Resource Spec", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}
	return &pc.SpecResponse{
		EndpointSpecSchemaJson: d.ConfigSchema,
		ResourceSpecSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:       d.DocumentationURL,
	}, nil
}

// ApplyUpsert applies a new or updated capture to the store.
func (d *Driver) ApplyUpsert(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{ActionDescription: ""}, nil
}

// ApplyDelete deletes an existing capture from the store.
func (d *Driver) ApplyDelete(ctx context.Context, req *pc.ApplyRequest) (*pc.ApplyResponse, error) {
	return &pc.ApplyResponse{ActionDescription: ""}, nil
}

// Validate that store resources and proposed collection bindings are compatible.
func (d *Driver) Validate(ctx context.Context, req *pc.ValidateRequest) (*pc.ValidateResponse, error) {
	var db, err = d.Connect(ctx, req.EndpointSpecJson)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	defer db.Close(ctx)

	if _, err := DiscoverCatalog(ctx, db); err != nil {
		return nil, err
	}

	var out []*pc.ValidateResponse_Binding
	for _, binding := range req.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}
		out = append(out, &pc.ValidateResponse_Binding{
			ResourcePath: []string{res.Namespace, res.Stream},
		})
	}
	return &pc.ValidateResponse{Bindings: out}, nil
}

// Discover returns the set of resources available from this Driver.
func (d *Driver) Discover(ctx context.Context, req *pc.DiscoverRequest) (*pc.DiscoverResponse, error) {
	var db, err = d.Connect(ctx, req.EndpointSpecJson)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	defer db.Close(ctx)

	discoveredBindings, err := DiscoverCatalog(ctx, db)
	if err != nil {
		return nil, err
	}

	// Filter the watermarks table out of the discovered catalog before output
	// It's basically never useful to capture so we shouldn't suggest it.
	var watermarkStreamID = db.WatermarksTable()
	var filteredBindings = []*pc.DiscoverResponse_Binding{} // Empty discovery must result in `[]` rather than `null`
	for _, binding := range discoveredBindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}
		var streamID = JoinStreamID(res.Namespace, res.Stream)
		if streamID != watermarkStreamID {
			filteredBindings = append(filteredBindings, binding)
		} else {
			log.WithFields(log.Fields{
				"filtered":   streamID,
				"watermarks": watermarkStreamID,
			}).Debug("filtered watermarks table from discovery")
		}
	}

	return &pc.DiscoverResponse{Bindings: filteredBindings}, nil
}

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *Driver) Pull(stream pc.Driver_PullServer) error {
	log.Debug("connector started")
	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("error reading PullRequest: %w", err)
	} else if open.Open == nil {
		return fmt.Errorf("expected PullRequest.Open, got %#v", open)
	}

	var ctx = stream.Context()
	db, err := d.Connect(ctx, open.Open.Capture.EndpointSpecJson)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}

	var state = &PersistentState{Streams: make(map[string]TableState)}
	if open.Open.DriverCheckpointJson != nil {
		if err := pf.UnmarshalStrict(open.Open.DriverCheckpointJson, state); err != nil {
			return fmt.Errorf("unable to parse state checkpoint: %w", err)
		}
	}

	if err := db.Connect(ctx); err != nil {
		return err
	}
	defer db.Close(ctx)

	// Build a mapping from stream IDs to capture binding information
	var bindings = make(map[string]*Binding)
	for idx, binding := range open.Open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceSpecJson, &res); err != nil {
			return fmt.Errorf("error parsing resource config: %w", err)
		}
		var streamID = JoinStreamID(res.Namespace, res.Stream)
		bindings[streamID] = &Binding{
			Index:    uint32(idx),
			Resource: res,
		}
	}

	var c = Capture{
		Bindings: bindings,
		State:    state,
		Output:   &boilerplate.PullOutput{Stream: stream},
		Tail:     open.Open.Tail,
		Database: db,
	}
	return c.Run(ctx)
}
