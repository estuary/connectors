package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strings"
	"text/template"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/sirupsen/logrus"
)

type Client interface {
	// FetchSpecAndVersion retrieves the materialization from Table `specs`,
	// or returns sql.ErrNoRows if no such spec exists.
	FetchSpecAndVersion(ctx context.Context, specs Table, materialization pf.Materialization) (specB64, version string, _ error)
	ExecStatements(ctx context.Context, statements []string) error
	InstallFence(ctx context.Context, checkpoints Table, fence Fence) (Fence, error)

	// Apply performs the driver-specific table creation or alteration actions to achieve
	// consistency with the proposed specification.
	Apply(ctx context.Context, ep *Endpoint, req *pm.Request_Apply, actions ApplyActions, updateSpec MetaSpecsUpdate) (string, error)

	// PreReqs performs verification checks that the provided configuration can be used to interact
	// with the endpoint to the degree required by the connector, to as much of an extent as
	// possible. The returned PrereqErr can include multiple separate errors if it possible to
	// determine that there is more than one issue that needs corrected.
	PreReqs(ctx context.Context, ep *Endpoint) *PrereqErr

	InfoSchema(ctx context.Context, ep *Endpoint, resourcePaths [][]string) (*boilerplate.InfoSchema, error)
}

// Resource is a driver-provided type which represents the SQL resource
// (for example, a table) bound to by a binding.
type Resource interface {
	// Validate returns an error if the Resource is malformed.
	Validate() error
	// Path returns the fully qualified name of the resource as a slice of strings.
	Path() TablePath
	// DeltaUpdates is true if the resource should be materialized using delta updates.
	DeltaUpdates() bool
}

// Fence is an installed barrier in a shared checkpoints table which prevents
// other sessions from committing transactions under the fenced ID,
// and prevents this Fence from committing where another session has in turn
// fenced this instance off.
type Fence struct {
	// TablePath of the checkpoints table in which this Fence lives.
	TablePath TablePath
	// Full name of the fenced Materialization.
	Materialization pf.Materialization
	// [KeyBegin, KeyEnd) identify the range of keys covered by this Fence.
	KeyBegin uint32
	KeyEnd   uint32
	// Fence is the current value of the monotonically increasing integer used
	// to order and isolate instances of the Transactions RPCs.
	// If fencing is not supported, Fence is zero.
	Fence int64
	// Checkpoint associated with this Fence.
	// A zero-length Checkpoint indicates that the Endpoint is unable to supply
	// a Checkpoint and that the recovery-log Checkpoint should be used instead.
	Checkpoint []byte
}

// Endpoint is a driver description of the SQL endpoint being driven.
type Endpoint struct {
	// Config is an implementation-specific type for the Endpoint configuration.
	Config interface{}
	// Dialect of the Endpoint.
	Dialect
	// MetaSpecs is the specification meta-table of the Endpoint.
	MetaSpecs *TableShape
	// MetaCheckpoints is the checkpoints meta-table of the Endpoint.
	// It's optional, and won't be created or used if it's nil.
	MetaCheckpoints *TableShape
	// Client provides Endpoint-specific methods for performing basic operations with the Endpoint
	// store.
	Client Client
	// CreateTableTemplate evaluates a Table into an endpoint statement which creates it.
	CreateTableTemplate *template.Template
	// ReplaceTableTemplate evaluates a Table into an endpoint statement which creates or replaces it.
	ReplaceTableTemplate *template.Template
	// NewResource returns an uninitialized or partially-initialized Resource
	// which will be parsed into and validated from a resource configuration.
	NewResource func(*Endpoint) Resource
	// NewTransactor returns a Transactor ready for pm.RunTransactions.
	NewTransactor func(ctx context.Context, _ *Endpoint, _ Fence, bindings []Table, open pm.Request_Open) (pm.Transactor, error)
	// Tenant owning this task, as determined from the task name.
	Tenant string
}

// PrereqErr is a wrapper for recording accumulated errors during prerequisite checking and
// formatting them for user presentation.
type PrereqErr struct {
	errs []error
}

// Err adds an error to the accumulated list of errors.
func (e *PrereqErr) Err(err error) {
	e.errs = append(e.errs, err)
}

func (e *PrereqErr) Len() int {
	return len(e.errs)
}

func (e *PrereqErr) Unwrap() []error {
	return e.errs
}

func (e *PrereqErr) Error() string {
	var b = new(strings.Builder)
	fmt.Fprintf(b, "the materialization cannot run due to the following error(s):")
	for _, err := range e.errs {
		b.WriteString("\n - ")
		b.WriteString(err.Error())
	}
	return b.String()
}

// loadSpec loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
func loadSpec(ctx context.Context, endpoint *Endpoint, materialization pf.Materialization) (*pf.MaterializationSpec, string, error) {
	var (
		err              error
		metaSpecs        Table
		spec             = new(pf.MaterializationSpec)
		specB64, version string
	)

	if endpoint.MetaSpecs == nil {
		return nil, "", nil
	}
	if metaSpecs, err = ResolveTable(*endpoint.MetaSpecs, endpoint.Dialect); err != nil {
		return nil, "", fmt.Errorf("resolving specifications table: %w", err)
	}
	specB64, version, err = endpoint.Client.FetchSpecAndVersion(ctx, metaSpecs, materialization)

	if err == sql.ErrNoRows {
		return nil, "", nil
	} else if err != nil {
		logrus.WithFields(logrus.Fields{
			"table": endpoint.MetaSpecs.Path,
			"err":   err,
		}).Info("failed to query materialization spec (the table may not be initialized?)")
		return nil, "", nil
	} else if specBytes, err := base64.StdEncoding.DecodeString(specB64); err != nil {
		return nil, version, fmt.Errorf("base64.Decode: %w", err)
	} else if err = spec.Unmarshal(specBytes); err != nil {
		return nil, version, fmt.Errorf("spec.Unmarshal: %w", err)
	} else if err = spec.Validate(); err != nil {
		return nil, version, fmt.Errorf("validating spec: %w", err)
	}

	return spec, version, nil
}
