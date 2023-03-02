package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	MetaSpecs TableShape
	// MetaCheckpoints is the checkpoints meta-table of the Endpoint.
	// It's optional, and won't be created or used if it's nil.
	MetaCheckpoints *TableShape
	// Client provides Endpoint-specific methods for performing basic operations with the Endpoint
	// store.
	Client Client
	// CreateTableTemplate evaluates a Table into an endpoint statement which creates it.
	CreateTableTemplate *template.Template
	// AlterFieldNullable alters a column and marks it as nullable
	AlterColumnNullableTemplate *template.Template

	// NewResource returns an uninitialized or partially-initialized Resource
	// which will be parsed into and validated from a resource configuration.
	NewResource func(*Endpoint) Resource
	// NewTransactor returns a Transactor ready for pm.RunTransactions.
	NewTransactor func(ctx context.Context, _ *Endpoint, _ Fence, bindings []Table) (pm.Transactor, error)
}

// loadBindings loads the named MaterializationSpec and its version that's stored within the Endpoint, if any.
func loadSpec(ctx context.Context, endpoint *Endpoint, materialization pf.Materialization) (*pf.MaterializationSpec, string, error) {
	var (
		err              error
		metaSpecs        Table
		spec             = new(pf.MaterializationSpec)
		specB64, version string
	)

	if metaSpecs, err = ResolveTable(endpoint.MetaSpecs, endpoint.Dialect); err != nil {
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
	constraints map[string]*pm.Constraint,
	loadedBinding *pf.MaterializationSpec_Binding,
	resource Resource,
	err error,
) {
	resource = endpoint.NewResource(endpoint)

	if err = pf.UnmarshalStrict(resourceSpec, resource); err != nil {
		err = fmt.Errorf("resource binding for collection %q: %w", &collection.Collection, err)
	} else if loadedBinding, err = findBinding(resource.Path(), collection.Collection, loadedSpec); err != nil {
	} else if loadedBinding != nil && loadedBinding.DeltaUpdates && !resource.DeltaUpdates() {
		// We allow a binding to switch from standard => delta updates but not the other way.
		// This is because a standard materialization is trivially a valid delta-updates
		// materialization, but a delta-updates table may have multiple instances of a given
		// key and can no longer be loaded correctly by a standard materialization.
		err = fmt.Errorf("cannot disable delta-updates binding of collection %s", collection.Collection)
	} else if loadedBinding != nil {
		constraints = ValidateMatchesExisting(loadedBinding, collection)
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

		if b.Collection.Collection == collection && path.equals(b.ResourcePath) {
			return b, nil
		} else if path.equals(b.ResourcePath) {
			return nil, fmt.Errorf("cannot materialize %s to table %s because the table is already materializing collection %s",
				path, &b.Collection.Collection, collection)
		}
	}
	return nil, nil
}
