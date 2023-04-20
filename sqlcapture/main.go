package sqlcapture

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	schemagen "github.com/estuary/connectors/go-schema-gen"
	boilerplate "github.com/estuary/connectors/source-boilerplate"
	pc "github.com/estuary/flow/go/protocols/capture"
	pf "github.com/estuary/flow/go/protocols/flow"
	log "github.com/sirupsen/logrus"
)

// Resource represents the capture configuration of a single table.
type Resource struct {
	Namespace string `json:"namespace" jsonschema:"title=Schema,description=The schema (namespace) in which the table resides."`
	Stream    string `json:"stream" jsonschema:"title=Table Name,description=The name of the table to be captured."`

	// PrimaryKey allows the user to override the "scan key" columns which will be used
	// to perform backfill queries and merge replicated changes. If left unset we default
	// to the collection's key, which is basically always what the user wants, so we omit
	// this property from the config schema to avoid confusion. However it is still supported,
	// if any captures set it.
	PrimaryKey []string `json:"primary_key,omitempty" jsonschema:"-"`

	DeprecatedSyncMode string `json:"syncMode,omitempty" jsonschema:"-"` // Unused, only supported to avoid breaking existing captures
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
	Index         uint32
	Resource      Resource
	CollectionKey []string // JSON pointers
}

// Driver is an implementation of the pc.DriverServer interface which performs
// captures from a SQL database.
type Driver struct {
	ConfigSchema     json.RawMessage
	DocumentationURL string

	Connect func(ctx context.Context, name string, cfg json.RawMessage) (Database, error)
}

type prerequisitesError struct {
	errs []error
}

func (e *prerequisitesError) Error() string {
	var b = new(strings.Builder)
	fmt.Fprintf(b, "the capture cannot run due to the following error(s):")
	for _, err := range e.errs {
		b.WriteString("\n - ")
		b.WriteString(err.Error())
	}
	return b.String()
}

func (e *prerequisitesError) Unwrap() []error {
	return e.errs
}

// docsUrlFromEnv looks for an environment variable set as DOCS_URL to use for the spec response
// documentation URL. It uses that instead of the default documentation URL from the connector if
// found.
func docsUrlFromEnv(providedURL string) string {
	fromEnv := os.Getenv("DOCS_URL")
	if fromEnv != "" {
		return fromEnv
	}

	return providedURL
}

// spec returns the specification definition of this driver.
// Notably this includes its endpoint and resource configuration JSON schema.
func (d *Driver) Spec(ctx context.Context, req *pc.Request_Spec) (*pc.Response_Spec, error) {
	var resourceSchema, err = schemagen.GenerateSchema("SQL Database Resource Spec", &Resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}
	return &pc.Response_Spec{
		Protocol:                 3032023,
		ConfigSchemaJson:         d.ConfigSchema,
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         docsUrlFromEnv(d.DocumentationURL),
	}, nil
}

// Apply applies a new or updated capture to the store.
func (d *Driver) Apply(ctx context.Context, req *pc.Request_Apply) (*pc.Response_Applied, error) {
	// If there are no bindings in the request, then it may represent the deletion of the capture,
	// or it may also be that the all of the bindings were deleted but the capture remains.
	if len(req.Capture.Bindings) > 0 {
		var db, err = d.Connect(ctx, string(req.Capture.Name), req.Capture.ConfigJson)
		if err != nil {
			return nil, fmt.Errorf("error connecting to database: %w", err)
		}
		defer db.Close(ctx)

		discoveredTables, err := db.DiscoverTables(ctx)
		if err != nil {
			return nil, err
		}

		for _, binding := range req.Capture.Bindings {
			var res Resource
			if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
				return nil, fmt.Errorf("error parsing resource config: %w", err)
			}

			var streamID = JoinStreamID(res.Namespace, res.Stream)

			if _, ok := discoveredTables[streamID]; !ok {
				return nil, fmt.Errorf("could not find or access table %s", res.Stream)
			}
		}
	}
	return &pc.Response_Applied{ActionDescription: ""}, nil
}

// Validate that store resources and proposed collection bindings are compatible.
func (d *Driver) Validate(ctx context.Context, req *pc.Request_Validate) (*pc.Response_Validated, error) {
	var db, err = d.Connect(ctx, string(req.Name), req.ConfigJson)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	defer db.Close(ctx)

	if _, err := DiscoverCatalog(ctx, db); err != nil {
		return nil, err
	}

	var errs = db.SetupPrerequisites(ctx)
	var out []*pc.Response_Validated_Binding
	for _, binding := range req.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("error parsing resource config: %w", err)
		}

		if err := db.SetupTablePrerequisites(ctx, res.Namespace, res.Stream); err != nil {
			errs = append(errs, err)
			continue
		}

		out = append(out, &pc.Response_Validated_Binding{
			ResourcePath: []string{res.Namespace, res.Stream},
		})
	}
	if len(errs) > 0 {
		return nil, &prerequisitesError{errs}
	}
	return &pc.Response_Validated{Bindings: out}, nil
}

// Discover returns the set of resources available from this Driver.
func (d *Driver) Discover(ctx context.Context, req *pc.Request_Discover) (*pc.Response_Discovered, error) {
	var db, err = d.Connect(ctx, "Flow Discovery", req.ConfigJson)
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
	var filteredBindings = []*pc.Response_Discovered_Binding{} // Empty discovery must result in `[]` rather than `null`
	for _, binding := range discoveredBindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
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

	return &pc.Response_Discovered{Bindings: filteredBindings}, nil
}

// Pull is a very long lived RPC through which the Flow runtime and a
// Driver cooperatively execute an unbounded number of transactions.
func (d *Driver) Pull(open *pc.Request_Open, stream *boilerplate.PullOutput) error {
	log.Debug("connector started")

	var state = &PersistentState{Streams: make(map[string]TableState)}
	if len(open.StateJson) > 0 {
		if err := pf.UnmarshalStrict(open.StateJson, state); err != nil {
			return fmt.Errorf("unable to parse state checkpoint: %w", err)
		}
	}

	var ctx = stream.Context()
	db, err := d.Connect(ctx, string(open.Capture.Name), open.Capture.ConfigJson)
	if err != nil {
		return fmt.Errorf("error connecting to database: %w", err)
	}
	defer db.Close(ctx)

	var errs = db.SetupPrerequisites(ctx)

	// Build a mapping from stream IDs to capture binding information
	var bindings = make(map[string]*Binding)
	for idx, binding := range open.Capture.Bindings {
		var res Resource
		if err := pf.UnmarshalStrict(binding.ResourceConfigJson, &res); err != nil {
			return fmt.Errorf("error parsing resource config: %w", err)
		}
		if err := db.SetupTablePrerequisites(ctx, res.Namespace, res.Stream); err != nil {
			errs = append(errs, err)
			continue
		}
		var streamID = JoinStreamID(res.Namespace, res.Stream)
		bindings[streamID] = &Binding{
			Index:         uint32(idx),
			Resource:      res,
			CollectionKey: binding.Collection.Key,
		}
	}

	if len(errs) > 0 {
		return &prerequisitesError{errs}
	}

	var c = Capture{
		Bindings: bindings,
		State:    state,
		Output:   &boilerplate.PullOutput{Connector_CaptureServer: stream},
		Database: db,
	}
	return c.Run(ctx)
}
