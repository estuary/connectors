package sql

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"slices"
// 	"strings"

// 	cerrors "github.com/estuary/connectors/go/connector-errors"
// 	m "github.com/estuary/connectors/go/protocols/materialize"
// 	schemagen "github.com/estuary/connectors/go/schema-gen"
// 	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
// 	pf "github.com/estuary/flow/go/protocols/flow"
// 	pm "github.com/estuary/flow/go/protocols/materialize"
// 	log "github.com/sirupsen/logrus"
// )

// type DriverV2 struct {
// 	// URL at which documentation for the driver may be found.
// 	DocumentationURL string
// 	// Instance of the type into which endpoint specifications are parsed.
// 	EndpointSpecType boilerplate.EndpointConfiger
// 	// Instance of the type into which resource specifications are parsed.
// 	ResourceSpecType Resource
// 	// StartTunnel starts up an SSH tunnel if one is configured prior to running
// 	// any operations that require connectivity to the database.
// 	StartTunnel func(ctx context.Context, endpointConfig boilerplate.EndpointConfiger) error
// 	// NewEndpoint returns an *Endpoint which will be used to handle interactions with the database.
// 	NewEndpoint func(_ context.Context, endpointConfig boilerplate.EndpointConfiger, tenant string) (*Endpoint, error)
// 	// PreReqs performs verification checks that the provided configuration can
// 	// be used to interact with the endpoint to the degree required by the
// 	// connector, to as much of an extent as possible. The returned PrereqErr
// 	// can include multiple separate errors if it possible to determine that
// 	// there is more than one issue that needs corrected.
// 	PreReqs func(ctx context.Context, endpointConfig boilerplate.EndpointConfiger, tenant string) *cerrors.PrereqErr
// }

// var _ boilerplate.Connector = &DriverV2{}

// func (d *DriverV2) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
// 	var endpoint, resource []byte

// 	if err := req.Validate(); err != nil {
// 		return nil, fmt.Errorf("validating request: %w", err)
// 	} else if endpoint, err = schemagen.GenerateSchema("SQL Connection", d.EndpointSpecType).MarshalJSON(); err != nil {
// 		return nil, fmt.Errorf("generating endpoint schema: %w", err)
// 	} else if resource, err = schemagen.GenerateSchema("SQL Table", d.ResourceSpecType).MarshalJSON(); err != nil {
// 		return nil, fmt.Errorf("generating resource schema: %w", err)
// 	}

// 	return &pm.Response_Spec{
// 		ConfigSchemaJson:         json.RawMessage(endpoint),
// 		ResourceConfigSchemaJson: json.RawMessage(resource),
// 		DocumentationUrl:         docsUrlFromEnv(d.DocumentationURL),
// 	}, nil
// }

// func (d *DriverV2) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
// 	return boilerplate.RunValidate(ctx, req, d.newMaterialization)
// }

// func (d *DriverV2) Apply(ctx context.Context, req *pm.Request_Apply) (*pm.Response_Applied, error) {
// 	return boilerplate.RunApply(ctx, req, d.newMaterialization)
// }

// func (d *DriverV2) NewTransactor(ctx context.Context, req pm.Request_Open, be *boilerplate.BindingEvents) (m.Transactor, *pm.Response_Opened, *boilerplate.MaterializeOptions, error) {
// 	return boilerplate.RunNewTransactor(ctx, req, be, d.newMaterialization)
// }
