package matsim

import (
	"context"
	"encoding/json"
	"fmt"

	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/gogo/protobuf/proto"
)

// flowSinkAdapter implements the pm.DriverServer interface for direct connection to adapters
// that are being called as if they were FlowSink connectors (for testing) instead of through
// the flowSink interface. The only difference is that EndpointSpecJson data must be unwrapped
// from the flowSink config and presented as if it was a native flow connector. If you do not
// do this the EndpointSpecJson will include the FLowSink image and config which will confuse
// the connector.
// Though flowSinkAdapter is a gRPC service stub, it's called in synchronous and
// in-process contexts to minimize ser/de & memory copies. As such it
// doesn't get to assume deep ownership of its requests, and must
// proto.Clone() shared state before mutating it.
// TODO: This logic should be combined with flow/go/materialize/driver/image/driver.go
type flowSinkAdapter struct {
	pm.DriverServer
}

// NewWrapper returns a new flowSinkWrapper.
func NewFlowSinkAdapter(server pm.DriverServer) pm.DriverServer { return &flowSinkAdapter{server} }

type specWithImage struct {
	Config json.RawMessage
	Image  string
}

func (h *specWithImage) Validate() error {
	return nil
}

// Spec returns the specification of the connector.
func (fsa flowSinkAdapter) Spec(ctx context.Context, req *pm.SpecRequest) (*pm.SpecResponse, error) {
	var source = new(specWithImage)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of flowSink configuration.
	req.EndpointSpecJson = source.Config
	return fsa.DriverServer.Spec(ctx, req)
}

// Validate validates the configuration.
func (fsa flowSinkAdapter) Validate(ctx context.Context, req *pm.ValidateRequest) (*pm.ValidateResponse, error) {
	var source = new(specWithImage)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of flowSink configuration.
	req.EndpointSpecJson = source.Config

	return fsa.DriverServer.Validate(ctx, req)
}

// Apply applies the configuration.
func (fsa flowSinkAdapter) ApplyUpsert(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	var source = new(specWithImage)
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(req.Materialization.EndpointSpecJson, source); err != nil {
		return nil, fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of flowSink configuration.
	req.Materialization = proto.Clone(req.Materialization).(*pf.MaterializationSpec)
	req.Materialization.EndpointSpecJson = source.Config

	return fsa.DriverServer.ApplyUpsert(ctx, req)
}

func (fsa flowSinkAdapter) ApplyDelete(ctx context.Context, req *pm.ApplyRequest) (*pm.ApplyResponse, error) {
	panic("ApplyDelete not adapted")
}

// Transactions implements the DriverServer interface.
func (fsa flowSinkAdapter) Transactions(stream pm.Driver_TransactionsServer) error {

	var open, err = stream.Recv()
	if err != nil {
		return fmt.Errorf("read Open: %w", err)
	}

	var source = new(specWithImage)
	if err := open.Validate(); err != nil {
		return fmt.Errorf("validating request: %w", err)
	} else if err = pf.UnmarshalStrict(open.Open.Materialization.EndpointSpecJson, source); err != nil {
		return fmt.Errorf("parsing connector configuration: %w", err)
	}
	// Unwrap layer of flowSink configuration.
	open.Open.Materialization = proto.Clone(open.Open.Materialization).(*pf.MaterializationSpec)
	open.Open.Materialization.EndpointSpecJson = source.Config

	return fsa.DriverServer.Transactions(&driverServerProxy{Driver_TransactionsServer: stream, open: open})

}

// driverServerProxy is a simple proxy interface that intercepts the first open message
// and provides the unwrapped flowSink configuration. Otherwise it passes the rest of the data
// through unmodified.
type driverServerProxy struct {
	pm.Driver_TransactionsServer
	open *pm.TransactionRequest
}

func (dsp *driverServerProxy) Send(resp *pm.TransactionResponse) error {
	return dsp.Driver_TransactionsServer.Send(resp)
}

func (dsp *driverServerProxy) Recv() (*pm.TransactionRequest, error) {
	if dsp.open != nil {
		var open = dsp.open
		dsp.open = nil
		return open, nil
	}
	return dsp.Driver_TransactionsServer.Recv()
}
