package sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"text/template"
	"time"

	"github.com/estuary/connectors/go/protocol"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/sirupsen/logrus"
)

type Client interface {
	// FetchSpecAndVersion retrieves the materialization from Table `specs`,
	// or returns sql.ErrNoRows if no such spec exists.
	FetchSpecAndVersion(ctx context.Context, specs Table, materialization string) (specB64, version string, _ error)
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
	Materialization string
	// [KeyBegin, KeyEnd) identify the range of keys covered by this Fence.
	KeyBegin uint32
	KeyEnd   uint32
	// Fence is the current value of the monotonically increasing integer used
	// to order and isolate instances of the Transactions RPCs.
	// If fencing is not supported, Fence is zero.
	Fence int64
	// Checkpoint associated with this Fence as a base64-encoded byte slice.
	// A zero-length Checkpoint indicates that the Endpoint is unable to supply
	// a Checkpoint and that the recovery-log Checkpoint should be used instead.
	Checkpoint string
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
	// NewTransactor returns a Transactor to handle Load and Store requests.
	NewTransactor func(ctx context.Context, _ *Endpoint, _ Fence, bindings []Table) (Transactor, error)
}

// loadSpec loads the named materialization's bindings and its version that's stored within the Endpoint, if any.
func loadBindings(ctx context.Context, endpoint *Endpoint, materialization string) ([]protocol.ApplyBinding, string, error) {
	var (
		err              error
		metaSpecs        Table
		loadedBindings   []protocol.ApplyBinding
		specB64, version string
	)

	if metaSpecs, err = ResolveTable(endpoint.MetaSpecs, endpoint.Dialect); err != nil {
		return nil, "", fmt.Errorf("resolving specifications table: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(10*time.Second))
	defer cancel()

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
		// TODO(whb): Replace unmarshalCompatible with json.Unmarshal once all sql materializations
		// have converted to JSON protocol and have re-applied all specs in the new form.
	} else if err = unmarshalCompatible(specBytes, &loadedBindings); err != nil {
		return nil, version, fmt.Errorf("spec.Unmarshal: %w", err)
	}

	return loadedBindings, version, nil
}

func unmarshalCompatible(specBytes []byte, bindings *[]protocol.ApplyBinding) error {
	// Try to unmarshal directly as a StoredSpec first.
	if err := json.Unmarshal(specBytes, bindings); err != nil {
		// We're probably dealing with a legacy protobuf spec if that didn't work.
		protobufSpec := &pf.MaterializationSpec{}
		if err := protobufSpec.Unmarshal(specBytes); err != nil {
			return fmt.Errorf("unmarshaling legacy protobufSpec: %w", err)
		}
		*bindings = protocol.MaterializationSpecPbToBindings(protobufSpec)
	}

	return nil
}

// resolveResourceToExistingBinding identifies an existing binding which matches the parsed
// `resourceSpec`. It further identifies field constraints of the resolved binding, and performs
// initial validation that the resource is well-formed and is a valid transition for an existing
// binding (if present).
func resolveResourceToExistingBinding(
	endpoint *Endpoint,
	resourceSpec json.RawMessage,
	collection protocol.CollectionSpec,
	loadedBindings []protocol.ApplyBinding,
) (
	constraints map[string]protocol.Constraint,
	loadedBinding *protocol.ApplyBinding,
	resource Resource,
	err error,
) {
	resource = endpoint.NewResource(endpoint)

	if err = pf.UnmarshalStrict(resourceSpec, resource); err != nil {
		err = fmt.Errorf("resource binding for collection %q: %w", collection.Name, err)
	} else if loadedBinding, err = findBinding(resource.Path(), collection.Name, loadedBindings); err != nil {
	} else if loadedBinding != nil && loadedBinding.DeltaUpdates && !resource.DeltaUpdates() {
		// We allow a binding to switch from standard => delta updates but not the other way.
		// This is because a standard materialization is trivially a valid delta-updates
		// materialization, but a delta-updates table may have multiple instances of a given
		// key and can no longer be loaded correctly by a standard materialization.
		err = fmt.Errorf("cannot disable delta-updates binding of collection %s", collection.Name)
	} else if loadedBinding != nil {
		constraints = ValidateMatchesExisting(*loadedBinding, collection)
	} else {
		constraints = ValidateNewSQLProjections(resource, collection)
	}

	return
}

func findBinding(path TablePath, collection string, bindings []protocol.ApplyBinding) (*protocol.ApplyBinding, error) {
	if bindings == nil {
		return nil, nil // Binding is trivially not found.
	}
	for i := range bindings {
		var b = bindings[i]

		if b.Collection.Name == collection && path.equals(b.ResourcePath) {
			return &b, nil
		} else if path.equals(b.ResourcePath) {
			return nil, fmt.Errorf("cannot materialize %s to table %s because the table is already materializing collection %s",
				path, b.Collection.Name, collection)
		}
	}
	return nil, nil
}
