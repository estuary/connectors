package filesink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"time"

	m "github.com/estuary/connectors/go/protocols/materialize"
	schemagen "github.com/estuary/connectors/go/schema-gen"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	log "github.com/sirupsen/logrus"
	"go.gazette.dev/core/consumer/protocol"
	"golang.org/x/sync/errgroup"
)

type Config interface {
	CommonConfig() CommonConfig
}

type CommonConfig struct {
	Prefix         string
	Extension      string
	UploadInterval time.Duration
	FileSizeLimit  int
}

// Store represents a file/object storage system capable of put'ing a stream of data to a binary
// object with a specified key.
type Store interface {
	PutStream(ctx context.Context, r io.Reader, key string) error
}

// StreamEncoder encodes rows of data to a particular format, writing the result to a stream.
type StreamEncoder interface {
	Encode(row []any) error
	Written() int
	Close() error
}

type resource struct {
	Path string `json:"path" jsonschema:"title=Path,description=The path that objects will be materialized to." jsonschema_extras:"x-collection-name=true"`
}

func (r resource) Validate() error {
	if r.Path == "" {
		return fmt.Errorf("missing 'path'")
	}

	return nil
}

var _ boilerplate.Connector = &FileDriver{}

// FileDriver contains the behaviors particular to a destination system and file format.
type FileDriver struct {
	NewConfig        func(raw json.RawMessage) (Config, error)
	NewStore         func(ctx context.Context, config Config) (Store, error)
	NewEncoder       func(config Config, b *pf.MaterializationSpec_Binding, w io.WriteCloser) StreamEncoder
	NewConstraints   func(p *pf.Projection) *pm.Response_Validated_Constraint
	DocumentationURL func() string
	ConfigSchema     func() ([]byte, error)
}

func (d FileDriver) Apply(context.Context, *pm.Request_Apply) (*pm.Response_Applied, error) {
	return &pm.Response_Applied{}, nil
}

func (d FileDriver) Spec(ctx context.Context, req *pm.Request_Spec) (*pm.Response_Spec, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("validating request: %w", err)
	}

	endpointSchema, err := d.ConfigSchema()
	if err != nil {
		return nil, fmt.Errorf("generating endpoint schema: %w", err)
	}

	resourceSchema, err := schemagen.GenerateSchema("ResourceConfig", &resource{}).MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("generating resource schema: %w", err)
	}

	return &pm.Response_Spec{
		ConfigSchemaJson:         json.RawMessage(endpointSchema),
		ResourceConfigSchemaJson: json.RawMessage(resourceSchema),
		DocumentationUrl:         d.DocumentationURL(),
	}, nil
}

func (d FileDriver) Validate(ctx context.Context, req *pm.Request_Validate) (*pm.Response_Validated, error) {
	var out []*pm.Response_Validated_Binding

	for _, b := range req.Bindings {
		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, fmt.Errorf("parsing resource config: %w", err)
		}

		constraints := make(map[string]*pm.Response_Validated_Constraint)
		for _, p := range b.Collection.Projections {
			constraints[p.Field] = d.NewConstraints(&p)
		}

		out = append(out, &pm.Response_Validated_Binding{
			Constraints:  constraints,
			DeltaUpdates: true,
			ResourcePath: []string{res.Path},
		})
	}

	return &pm.Response_Validated{Bindings: out}, nil
}

func (d FileDriver) NewTransactor(ctx context.Context, open pm.Request_Open) (m.Transactor, *pm.Response_Opened, error) {
	driverCfg, err := d.NewConfig(open.Materialization.ConfigJson)
	if err != nil {
		return nil, nil, err
	}

	store, err := d.NewStore(ctx, driverCfg)
	if err != nil {
		return nil, nil, err
	}

	bindings := make([]binding, 0, len(open.Materialization.Bindings))

	for _, b := range open.Materialization.Bindings {
		b := b // for the newEncoder closure

		var res resource
		if err := pf.UnmarshalStrict(b.ResourceConfigJson, &res); err != nil {
			return nil, nil, err
		}

		bindings = append(bindings, binding{
			stateKey:   b.StateKey,
			backfill:   b.Backfill,
			path:       res.Path,
			includeDoc: b.FieldSelection.Document != "",
			newEncoder: func(w io.WriteCloser) StreamEncoder {
				// Partial application of the driverCfg and b arguments to the FileDriver's NewEncoder
				// function, for convenience.
				return d.NewEncoder(driverCfg, b, w)
			},
		})
	}

	return &transactor{
		bindings: bindings,
		store:    store,
		common:   driverCfg.CommonConfig(),
	}, &pm.Response_Opened{}, nil
}

type connectorState struct {
	FileCounts map[string]uint64 `json:"fileCounts"`
}

func (cs connectorState) Validate() error { return nil }

type transactor struct {
	bindings []binding
	store    Store
	state    connectorState
	common   CommonConfig
}

type binding struct {
	stateKey   string
	backfill   uint32
	path       string
	includeDoc bool
	newEncoder func(w io.WriteCloser) StreamEncoder
}

// File keys are the full "path" to a file, usually applied as a key for an object in an object
// store. They consist of:
// 1) The optional prefix supplied from the connector config.
// 2) The resource path.
// 3) The backfill version: Incrementing the backfill counter will not delete any files, but it will
// start to materialize them using a different backfill version part of their keys.
// 4) A monotonic counter for the files. Each file gets the next highest counter value. There may be
// multiple files created within the same transaction. If the connector fails part-way through a
// transaction, some files may have been written without committing that transaction to the recovery
// log. In that case, when the connector restarts files with the same names will be uploaded to
// overwrite the previous files. In this way the connector is effectively-once, although the most
// recent files may have some churn. This will become less of a concern when/if Flow transactions
// are idempotent, since the re-uploaded files will at least have the same data.
// 5) The file extension from the specific driver implementation.
func (t *transactor) nextFileKey(b binding) string {
	sk := b.stateKey

	// If we haven't generated any file keys yet for this state key, start at 0. Otherwise use one
	// greater than the prior key.
	next := uint64(0)
	if _, ok := t.state.FileCounts[sk]; ok {
		next = t.state.FileCounts[sk] + 1
	}
	t.state.FileCounts[sk] = next

	return filepath.Join(
		t.common.Prefix,
		b.path,
		fmt.Sprintf("v%010d", b.backfill), // 10 digits to hold the largest possible uint32
		fmt.Sprintf("%020d%s", next, t.common.Extension), // 20 digits to hold the largest possible uint64
	)
}

// AckDelay implements the DelayedCommitter interface, which is used to upload files at the desired
// interval.
func (t *transactor) AckDelay() time.Duration {
	return t.common.UploadInterval
}

func (t *transactor) UnmarshalState(state json.RawMessage) error {
	if err := pf.UnmarshalStrict(state, &t.state); err != nil {
		return err
	}

	if t.state.FileCounts == nil {
		t.state.FileCounts = make(map[string]uint64)
	}

	return nil
}

func (t *transactor) Load(it *m.LoadIterator, _ func(int, json.RawMessage) error) error {
	for it.Next() {
		panic("driver only supports delta updates")
	}
	return nil
}

func (t *transactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	var ctx = it.Context()
	var encoder StreamEncoder
	var group errgroup.Group

	startFile := func(b binding) {
		// Start a new file upload. This may be called multiple times in a single transaction if
		// there is more data than can fit in a single file.
		r, w := io.Pipe()

		group.Go(func() error {
			k := t.nextFileKey(b)
			log.WithField("key", k).Info("started uploading file")
			if err := t.store.PutStream(ctx, r, k); err != nil {
				r.CloseWithError(err)
				return fmt.Errorf("uploading file: %w", err)
			}
			log.WithField("key", k).Info("finished uploading file")

			return nil
		})

		encoder = b.newEncoder(w)
	}

	finishFile := func() error {
		// Close out the current file and wait for its upload to complete.
		if encoder == nil {
			return nil
		} else if err := encoder.Close(); err != nil {
			return fmt.Errorf("closing encoder: %w", err)
		} else if err := group.Wait(); err != nil {
			return fmt.Errorf("group.Wait(): %w", err)
		}

		encoder = nil
		return nil
	}

	lastBinding := -1
	for it.Next() {
		b := t.bindings[it.Binding]

		if lastBinding != -1 && lastBinding != it.Binding {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile after binding change: %w", err)
			}
		}
		lastBinding = it.Binding

		if encoder == nil {
			startFile(b)
		}

		row := make([]any, 0, len(it.Key)+len(it.Values)+1)
		row = append(row, it.Key.ToInterface()...)
		row = append(row, it.Values.ToInterface()...)
		if b.includeDoc {
			row = append(row, it.RawJSON)
		}

		if err := encoder.Encode(row); err != nil {
			return nil, fmt.Errorf("encoding row: %w", err)
		}

		if t.common.FileSizeLimit != 0 && encoder.Written() >= t.common.FileSizeLimit {
			if err := finishFile(); err != nil {
				return nil, fmt.Errorf("finishFile on file size limit: %w", err)
			}
		}

	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("store iterator error: %w", err)
	}

	if err := finishFile(); err != nil {
		return nil, fmt.Errorf("final finishFile: %w", err)
	}

	return func(ctx context.Context, _ *protocol.Checkpoint) (*pf.ConnectorState, m.OpFuture) {
		checkpointJSON, err := json.Marshal(t.state)
		if err != nil {
			return nil, m.FinishedOperation(fmt.Errorf("creating checkpoint json: %w", err))
		}

		return &pf.ConnectorState{UpdatedJson: checkpointJSON}, nil
	}, nil
}

// Acknowledge is a no-op since the files for this transaction were already uploaded to their
// desired location in Store, and the driver checkpoint contains the last file counts used.
func (t *transactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) { return nil, nil }

func (t *transactor) Destroy() {}

// StdConstraints represents standard constraints for a projection when materializing columns of
// data to files.
func StdConstraints(p *pf.Projection) *pm.Response_Validated_Constraint {
	_, isNumeric := boilerplate.AsFormattedNumeric(p)

	var constraint = new(pm.Response_Validated_Constraint)
	switch {
	case p.IsPrimaryKey:
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_REQUIRED
		constraint.Reason = "All Locations that are part of the collections key are required"
	case p.IsRootDocumentProjection():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The root document should usually be materialized"
	case len(p.Inference.Types) == 0 || slices.Equal(p.Inference.Types, []string{"null"}):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_FORBIDDEN
		constraint.Reason = "Cannot materialize a field with no types"
	case p.Field == "_meta/op":
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "The operation type should usually be materialized"
	case strings.HasPrefix(p.Field, "_meta/"):
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "Metadata fields fields are able to be materialized"
	case p.Inference.IsSingleType() && slices.Contains(p.Inference.Types, "array") ||
		isNumeric ||
		p.Inference.IsSingleScalarType():
		constraint.Type = pm.Response_Validated_Constraint_LOCATION_RECOMMENDED
		constraint.Reason = "This field should usually be materialized"
	default:
		constraint.Type = pm.Response_Validated_Constraint_FIELD_OPTIONAL
		constraint.Reason = "This field is able to be materialized"
	}

	return constraint
}
