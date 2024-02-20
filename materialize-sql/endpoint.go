package sql

import (
	"context"
	"fmt"
	"strings"
	"text/template"

	m "github.com/estuary/connectors/go/protocols/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
)

type Client interface {
	// FetchSpecAndVersion retrieves the materialization from Table `specs`,
	// or returns sql.ErrNoRows if no such spec exists.
	FetchSpecAndVersion(ctx context.Context, specs Table, materialization pf.Materialization) (specB64, version string, _ error)
	ExecStatements(ctx context.Context, statements []string) error
	InstallFence(ctx context.Context, checkpoints Table, fence Fence) (Fence, error)

	// PreReqs performs verification checks that the provided configuration can be used to interact
	// with the endpoint to the degree required by the connector, to as much of an extent as
	// possible. The returned PrereqErr can include multiple separate errors if it possible to
	// determine that there is more than one issue that needs corrected.
	PreReqs(ctx context.Context) *PrereqErr

	// InfoSchema returns a populated *boilerplate.InfoSchema containing the state of the actual
	// materialized tables in the schemas referenced by the resourcePaths. It doesn't necessarily
	// need to include all tables in the entire destination system, but must include all tables in
	// the relevant schemas.
	InfoSchema(ctx context.Context, resourcePaths [][]string) (*boilerplate.InfoSchema, error)

	// PutSpec executes the MetaSpecsUpdate to upsert the spec into the metadata table.
	PutSpec(ctx context.Context, spec MetaSpecsUpdate) error

	// CreateTable creates a table in the destination system.
	CreateTable(ctx context.Context, tc TableCreate) error

	// DeleteTable deletes a table in preparation for creating it anew as part of re-backfilling the
	// binding. It should return a string describing the action (the SQL statements it will run) and
	// a callback function to execute in order to complete the action.
	DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error)

	// AlterTable takes the actions needed per the TableAlter. Like DeleteTable, it should return a
	// description of the action in the form of SQL statements, and a callback to do the action.
	AlterTable(ctx context.Context, ta TableAlter) (string, boilerplate.ActionApplyFn, error)

	// Close is called to free up any resources held by the Client.
	Close()
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
	// NewClient creates a client, which provides Endpoint-specific methods for performing
	// operations with the Endpoint store.
	NewClient func(context.Context, *Endpoint) (Client, error)
	// CreateTableTemplate evaluates a Table into an endpoint statement which creates it.
	CreateTableTemplate *template.Template
	// NewResource returns an uninitialized or partially-initialized Resource
	// which will be parsed into and validated from a resource configuration.
	NewResource func(*Endpoint) Resource
	// NewTransactor returns a Transactor ready for pm.RunTransactions.
	NewTransactor func(ctx context.Context, _ *Endpoint, _ Fence, bindings []Table, open pm.Request_Open) (m.Transactor, error)
	// Tenant owning this task, as determined from the task name.
	Tenant string
	// ConcurrentApply of Apply actions, for system that may benefit from a scatter/gather strategy
	// for changing many tables in a single apply.
	ConcurrentApply bool
	// MaxFieldCharLen is the maximum length allowable by the destination for materialized column
	// names. A value of 0 means no maximum length is enforced.
	MaxFieldCharLen int
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
