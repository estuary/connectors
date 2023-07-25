package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"text/template"

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

	// Driver-specific method for adding a column to a table. Some databases support syntax like
	// "ADD COLUMN IF NOT EXISTS", and others need specific error handling for cases where a column
	// already exists in a table. This is important for situations where a materialized table was
	// initially created, had a field removed from the field selection, and then has the field added
	// back to the field selection.
	AddColumnToTable(ctx context.Context, dryRun bool, tableIdentifier, columnIdentifier, columnDDL string) (string, error)

	// Driver specific method for dropping a NOT NULL constraint from a table. Some databases treat
	// this as an idempotent action, where a column that is already NOT NULL returns a successful
	// outcome. Others report and error, and still others don't support doing this at all and treat
	// it as a no-op - all columns in the table must be created as NOT NULL for these cases.
	// Handling these different cases is important for removing a field from a materialization if
	// the table column is not nullable since we will always try to drop the NOT NULL constraint for
	// the table when a field is removed. We cannot rely on the field being required in the
	// collection schema because required fields added to a table after initial creation will be
	// created as nullable.
	DropNotNullForColumn(ctx context.Context, dryRun bool, tableIdentifier, columnIdentifier string) (string, error)
}

// Resource is a driver-provided type which represents the SQL resource
// (for example, a table) bound to by a binding.
type Resource interface {
	// Validate returns an error if the Resource is malformed.
	Validate() error
	// Path returns the fully qualified name of the resource, as '.'-separated components.
	Path() TablePath
	// Get any user-defined additional SQL to be executed transactionally with table creation.
	GetAdditionalSql() string
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
	// NewResource returns an uninitialized or partially-initialized Resource
	// which will be parsed into and validated from a resource configuration.
	NewResource func(*Endpoint) Resource
	// NewTransactor returns a Transactor ready for pm.RunTransactions.
	NewTransactor func(ctx context.Context, _ *Endpoint, _ Fence, bindings []Table) (pm.Transactor, error)
	// CheckPrerequisites validates that the proposed configuration is able to connect to the
	// endpoint and perform the required actions. It assumes that any required SSH tunneling is
	// setup prior to its call.
	CheckPrerequisites func(ctx context.Context, ep *Endpoint) *PrereqErr

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

// resolveResourceToExistingBinding identifies an existing binding of `loadedSpec`
// which matches the parsed `resourceSpec`. It further identifies field constraints
// of the resolved binding, and performs initial validation that the resource is
// well-formed and is a valid transition for an existing binding (if present).
func resolveResourceToExistingBinding(
	endpoint *Endpoint,
	resourceSpec json.RawMessage,
	collection *pf.CollectionSpec,
	loadedSpec *pf.MaterializationSpec,
) (
	constraints map[string]*pm.Response_Validated_Constraint,
	loadedBinding *pf.MaterializationSpec_Binding,
	resource Resource,
	err error,
) {
	resource = endpoint.NewResource(endpoint)

	if err = pf.UnmarshalStrict(resourceSpec, resource); err != nil {
		err = fmt.Errorf("resource binding for collection %q: %w", &collection.Name, err)
	} else if loadedBinding, err = findBinding(resource.Path(), collection.Name, loadedSpec); err != nil {
	} else if loadedBinding != nil && loadedBinding.DeltaUpdates && !resource.DeltaUpdates() {
		// We allow a binding to switch from standard => delta updates but not the other way.
		// This is because a standard materialization is trivially a valid delta-updates
		// materialization, but a delta-updates table may have multiple instances of a given
		// key and can no longer be loaded correctly by a standard materialization.
		err = fmt.Errorf("cannot disable delta-updates binding of collection %s", collection.Name)
	} else if loadedBinding != nil {
		constraints = ValidateMatchesExisting(resource, loadedBinding, collection)
	} else {
		constraints = ValidateNewSQLProjections(resource, collection)
	}

	return
}

func findBinding(path TablePath, collection pf.Collection, spec *pf.MaterializationSpec) (*pf.MaterializationSpec_Binding, error) {
	if spec == nil {
		return nil, nil // Binding is trivially not found.
	}
	for i := range spec.Bindings {
		var b = spec.Bindings[i]

		if b.Collection.Name == collection && path.equals(b.ResourcePath) {
			return b, nil
		} else if path.equals(b.ResourcePath) {
			return nil, fmt.Errorf("cannot materialize %s to table %s because the table is already materializing collection %s",
				path, &b.Collection.Name, collection)
		}
	}
	return nil, nil
}
