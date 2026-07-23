package boilerplate

import (
	"context"
	"embed"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/base.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/updated.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/incompatible-changes.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/truncate.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/field-addition.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/field-removal.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/backfill-migratable.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/backfill-key-migratable.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/backfill-key-change.flow.yaml
//go:generate ./testdata/generate-spec-proto.sh testdata/materializer/backfill-nullable.flow.yaml

//go:embed testdata/materializer/generated_specs
var materializerFS embed.FS

func loadMaterializerSpec(t *testing.T, path string) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := materializerFS.ReadFile(filepath.Join("testdata/materializer/generated_specs", path))
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	return &spec
}

func TestRunApply(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name         string
		originalSpec *pf.MaterializationSpec
		newSpec      *pf.MaterializationSpec
		want         testCalls
	}{
		{
			name:         "new materialization",
			originalSpec: nil,
			newSpec:      loadMaterializerSpec(t, "base.flow.proto"),
			want: testCalls{
				createResource: [][]string{{"key_value"}},
			},
		},
		{
			name:         "noop re-apply",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "base.flow.proto"),
			want: testCalls{
				updateResource: [][]string{{"key_value"}}, // called for every resource, even if no changes
			},
		},
		{
			name:         "update existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "updated.flow.proto"),
			want: testCalls{
				updateResource: [][]string{{"key_value"}},
			},
		},
		{
			name:         "backfill with incompatible changes drops existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "incompatible-changes.flow.proto"),
			want: testCalls{
				createResource: [][]string{{"key_value"}},
				deleteResource: [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with only backfill change truncates existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "truncate.flow.proto"),
			want: testCalls{
				truncateResource: [][]string{{"key_value"}},
				updateResource:   [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with backfill & field addition truncates existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "field-addition.flow.proto"),
			want: testCalls{
				truncateResource: [][]string{{"key_value"}},
				updateResource:   [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with backfill & field removal truncates existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "field-removal.flow.proto"),
			want: testCalls{
				truncateResource: [][]string{{"key_value"}},
				updateResource:   [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with backfill & migratable changes truncates existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "backfill-migratable.flow.proto"),
			want: testCalls{
				truncateResource: [][]string{{"key_value"}},
				updateResource:   [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with backfill & migratable change to key field drops existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "backfill-key-migratable.flow.proto"),
			want: testCalls{
				createResource: [][]string{{"key_value"}},
				deleteResource: [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with backfill & nullability removal truncates & updates existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "backfill-nullable.flow.proto"),
			want: testCalls{
				truncateResource: [][]string{{"key_value"}},
				updateResource:   [][]string{{"key_value"}},
			},
		},
		{
			name:         "binding with backfill & key change drops existing resource",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "backfill-key-change.flow.proto"),
			want: testCalls{
				createResource: [][]string{{"key_value"}},
				deleteResource: [][]string{{"key_value"}},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := &pm.Request_Apply{
				Materialization:     tt.newSpec,
				Version:             "thisone",
				LastMaterialization: tt.originalSpec,
				LastVersion:         "oldone",
			}

			is := testInfoSchemaFromSpec(t, tt.originalSpec, func(in string) string { return in })
			got := &testCalls{}

			_, err := RunApply(ctx, req, makeTestMaterializerFn(got, is))
			require.NoError(t, err)
			require.Equal(t, tt.want, *got)
		})
	}
}

func TestRunApplyRetainExistingDataOnBackfill(t *testing.T) {
	ctx := context.Background()

	for _, tt := range []struct {
		name         string
		originalSpec *pf.MaterializationSpec
		newSpec      *pf.MaterializationSpec
		want         testCalls
		expectError  bool
	}{
		{
			name:         "should not truncate resource when retain_existing_data_on_backfill is set",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "backfill-nullable.flow.proto"),
			want: testCalls{
				// No truncateResource call - it should be skipped
				updateResource: [][]string{{"key_value"}},
			},
			expectError: false,
		},
		{
			name:         "should error when drop/recreate is required and retain_existing_data_on_backfill is set",
			originalSpec: loadMaterializerSpec(t, "base.flow.proto"),
			newSpec:      loadMaterializerSpec(t, "backfill-key-change.flow.proto"),
			want:         testCalls{
				// No calls should be made because it errors before getting there
			},
			expectError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			req := &pm.Request_Apply{
				Materialization:     tt.newSpec,
				Version:             "thisone",
				LastMaterialization: tt.originalSpec,
				LastVersion:         "oldone",
			}

			// Parse the original spec's ConfigJson and set retain_existing_data_on_backfill feature flag and set back on the req
			var config testEndpointConfiger
			require.NoError(t, json.Unmarshal(req.Materialization.ConfigJson, &config))

			// Modify config to enable retain_existing_data_on_backfill
			config.Config = map[string]any{
				"advanced": map[string]any{
					"feature_flags": "retain_existing_data_on_backfill",
				},
			}

			modifiedConfigJson, err := json.Marshal(config)
			require.NoError(t, err)
			req.Materialization.ConfigJson = modifiedConfigJson

			is := testInfoSchemaFromSpec(t, tt.originalSpec, func(in string) string { return in })
			got := &testCalls{}

			_, err = RunApply(ctx, req, makeTestMaterializerFn(got, is))

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "retain_existing_data_on_backfill")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, *got)
			}
		})
	}
}

func TestRunApplyDrainsPendingState(t *testing.T) {
	ctx := context.Background()

	pendingState := json.RawMessage(`{"key_value":{"query":"MERGE INTO ..."}}`)
	clearingPatch := &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"key_value":null}`), MergePatch: true}

	for _, tt := range []struct {
		name         string
		newSpec      *pf.MaterializationSpec
		stateJson    json.RawMessage
		ackPatch     *pf.ConnectorState
		featureFlags string
		wantAckKeys  []string
		wantState    *pf.ConnectorState
		wantExecuted int
	}{
		{
			name:         "pending state is drained before in-place updates",
			newSpec:      loadMaterializerSpec(t, "updated.flow.proto"),
			stateJson:    pendingState,
			ackPatch:     clearingPatch,
			wantAckKeys:  []string{"key_value"},
			wantState:    clearingPatch,
			wantExecuted: 0, // the drain returns early: no actions may run
		},
		{
			name:         "no pending work proceeds to actions in the same invocation",
			newSpec:      loadMaterializerSpec(t, "updated.flow.proto"),
			stateJson:    pendingState,
			ackPatch:     nil,
			wantAckKeys:  []string{"key_value"},
			wantState:    nil,
			wantExecuted: 1,
		},
		{
			name:         "a state update changing nothing proceeds to actions",
			newSpec:      loadMaterializerSpec(t, "updated.flow.proto"),
			stateJson:    pendingState,
			ackPatch:     &pf.ConnectorState{UpdatedJson: json.RawMessage(`{"unrelated":null}`), MergePatch: true},
			wantAckKeys:  []string{"key_value"},
			wantState:    nil,
			wantExecuted: 1,
		},
		{
			name:         "no state means no drain",
			newSpec:      loadMaterializerSpec(t, "updated.flow.proto"),
			stateJson:    nil,
			ackPatch:     clearingPatch,
			wantAckKeys:  nil,
			wantState:    nil,
			wantExecuted: 1,
		},
		{
			name:         "truncated backfill discards pending state without draining",
			newSpec:      loadMaterializerSpec(t, "truncate.flow.proto"),
			stateJson:    pendingState,
			ackPatch:     clearingPatch,
			wantAckKeys:  nil,
			wantState:    nil,
			wantExecuted: 2, // truncation + update
		},
		{
			name:         "re-created backfill discards pending state without draining",
			newSpec:      loadMaterializerSpec(t, "incompatible-changes.flow.proto"),
			stateJson:    pendingState,
			ackPatch:     clearingPatch,
			wantAckKeys:  nil,
			wantState:    nil,
			wantExecuted: 2, // delete + create
		},
		{
			name:         "data-retaining backfill drains the pre-backfill state key",
			newSpec:      loadMaterializerSpec(t, "backfill-nullable.flow.proto"),
			stateJson:    pendingState,
			ackPatch:     clearingPatch,
			featureFlags: "retain_existing_data_on_backfill",
			wantAckKeys:  []string{"key_value"}, // NOT key_value.v1: pending work was staged under the pre-backfill key
			wantState:    clearingPatch,
			wantExecuted: 0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			originalSpec := loadMaterializerSpec(t, "base.flow.proto")

			req := &pm.Request_Apply{
				Materialization:     tt.newSpec,
				Version:             "thisone",
				LastMaterialization: originalSpec,
				LastVersion:         "oldone",
				StateJson:           tt.stateJson,
			}

			if tt.featureFlags != "" {
				var config testEndpointConfiger
				require.NoError(t, json.Unmarshal(req.Materialization.ConfigJson, &config))
				config.Config = map[string]any{
					"advanced": map[string]any{"feature_flags": tt.featureFlags},
				}
				modifiedConfigJson, err := json.Marshal(config)
				require.NoError(t, err)
				req.Materialization.ConfigJson = modifiedConfigJson
			}

			is := testInfoSchemaFromSpec(t, originalSpec, func(in string) string { return in })
			rec := &drainRecorder{ackPatch: tt.ackPatch}

			applied, err := RunApply(ctx, req, makeDrainTestMaterializerFn(rec, is))
			require.NoError(t, err)

			require.Equal(t, tt.wantAckKeys, rec.ackKeys)
			require.Equal(t, tt.wantState, applied.State)
			require.Equal(t, tt.wantExecuted, rec.executedActions)

			if tt.wantAckKeys != nil {
				// The transactor must have been built from the last-applied
				// specification and given the prior state, then destroyed.
				require.Same(t, originalSpec, rec.openedSpec)
				require.Equal(t, "oldone", rec.openedVersion)
				require.Equal(t, tt.stateJson, rec.unmarshaled)
				require.True(t, rec.destroyed)
			} else {
				require.Nil(t, rec.openedSpec) // no transactor was built
			}

			if tt.wantState != nil {
				require.Contains(t, applied.ActionDescription, "key_value")
			}
		})
	}
}

type testCalls struct {
	createResource   [][]string
	updateResource   [][]string
	deleteResource   [][]string
	truncateResource [][]string
}

var _ Materializer[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper] = (*testMaterializer)(nil)

type testMaterializer struct {
	calls *testCalls
	is    *InfoSchema
}

func makeTestMaterializerFn(got *testCalls, is *InfoSchema) NewMaterializerFn[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper] {
	return func(
		ctx context.Context,
		materializationName string,
		endpointConfig testEndpointConfiger,
		featureFlags map[string]bool,
	) (Materializer[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper], error) {
		return &testMaterializer{
			calls: got,
			is:    is,
		}, nil
	}
}

var testMaterializerFn NewMaterializerFn[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper] = func(
	_ context.Context,
	_ string,
	_ testEndpointConfiger,
	_ map[string]bool,
) (Materializer[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper], error) {
	return &testMaterializer{}, nil
}

func (m *testMaterializer) Config() MaterializeCfg {
	return MaterializeCfg{}
}

func (m *testMaterializer) PopulateInfoSchema(ctx context.Context, is *InfoSchema, paths [][]string) error {
	*is = *m.is
	return nil
}

func (m *testMaterializer) CheckPrerequisites(ctx context.Context) *cerrors.PrereqErr {
	return nil
}

func (m *testMaterializer) NewConstraint(p pf.Projection, deltaUpdates bool, fieldConfig testFieldConfiger) pm.Response_Validated_Constraint {
	return pm.Response_Validated_Constraint{}
}

func (m *testMaterializer) MapType(p Projection, fieldCfg testFieldConfiger) (testMappedTyper, ElementConverter) {
	return testMappedTyper{
		jsonTypes: p.Inference.Types,
	}, nil
}

func (m *testMaterializer) Setup(ctx context.Context, is *InfoSchema) (string, error) {
	return "", nil
}

func (m *testMaterializer) CreateNamespace(ctx context.Context, namespace string) (string, error) {
	panic("unimplemented")
}

func (m *testMaterializer) CreateResource(ctx context.Context, binding MappedBinding[testEndpointConfiger, testResourcer, testMappedTyper]) (string, ActionApplyFn, error) {
	m.calls.createResource = append(m.calls.createResource, binding.ResourcePath)
	return "", func(context.Context) error { return nil }, nil
}

func (m *testMaterializer) DeleteResource(ctx context.Context, path []string) (string, ActionApplyFn, error) {
	m.calls.deleteResource = append(m.calls.createResource, path)
	return "", func(context.Context) error { return nil }, nil
}

func (m *testMaterializer) UpdateResource(ctx context.Context, path []string, existing ExistingResource, update BindingUpdate[testEndpointConfiger, testResourcer, testMappedTyper]) (string, ActionApplyFn, error) {
	m.calls.updateResource = append(m.calls.updateResource, path)
	return "", func(context.Context) error { return nil }, nil
}

func (m *testMaterializer) TruncateResource(ctx context.Context, path []string) (string, ActionApplyFn, error) {
	m.calls.truncateResource = append(m.calls.truncateResource, path)
	return "", func(context.Context) error { return nil }, nil
}

func (m *testMaterializer) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}

type testEndpointConfiger struct {
	Image  string         `json:"image"`
	Config map[string]any `json:"config"`
}

func (c testEndpointConfiger) Validate() error {
	return nil
}

func (c testEndpointConfiger) DefaultNamespace() string {
	return ""
}

func (c testEndpointConfiger) FeatureFlags() (string, map[string]bool) {
	featureFlags := ""
	if advanced, ok := c.Config["advanced"].(map[string]any); ok {
		if ff, ok := advanced["feature_flags"].(string); ok {
			featureFlags = ff
		}
	}

	return featureFlags, map[string]bool{
		"retain_existing_data_on_backfill": false,
	}
}

type testFieldConfiger struct{}

func (c testFieldConfiger) Validate() error {
	return nil
}

func (c testFieldConfiger) CastToString() bool {
	return false
}

type testResourcer struct {
	Table string `json:"table"`
}

func (r testResourcer) Validate() error {
	return nil
}

func (r testResourcer) WithDefaults(ec testEndpointConfiger) testResourcer {
	return r
}

func (r testResourcer) Parameters() ([]string, bool, error) {
	return []string{"test", "resource"}, false, nil
}

type testMappedTyper struct {
	jsonTypes []string
}

func (t testMappedTyper) String() string {
	return strings.Join(t.jsonTypes, ", ")
}

func (t testMappedTyper) Compatible(e ExistingField) bool {
	if len(t.jsonTypes) == 1 && t.jsonTypes[0] == e.Type {
		return true
	}

	return false
}

func (t testMappedTyper) CanMigrate(e ExistingField) bool {
	return len(t.jsonTypes) > 1 || (e.Name == "key" && t.jsonTypes[0] == "boolean")
}

func (m *testMaterializer) NewTransactor(ctx context.Context, req pm.Request_Open, is InfoSchema, bindings []MappedBinding[testEndpointConfiger, testResourcer, testMappedTyper], be *m.BindingEvents) (m.Transactor, error) {
	return &testTransactor{}, nil
}

func (m *testMaterializer) ListTestTasks(context.Context) ([]string, error) {
	panic("unimplemented")
}

func (m *testMaterializer) CleanupTestTask(context.Context, string) error {
	panic("unimplemented")
}

func (m *testMaterializer) SnapshotTestResource(context.Context, []string) (columnNames []string, rows [][]any, _ error) {
	panic("unimplemented")
}

func (m *testMaterializer) Close(ctx context.Context) {}

type testTransactor struct{}

func (t *testTransactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (m.RuntimeCheckpoint, error) {
	panic("unimplemented")
}

func (t *testTransactor) UnmarshalState(state json.RawMessage) error {
	panic("unimplemented")
}

func (t *testTransactor) Load(it *m.LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	panic("unimplemented")
}

func (t *testTransactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	panic("unimplemented")
}

func (t *testTransactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	panic("unimplemented")
}

func (t *testTransactor) Destroy() {
	panic("unimplemented")
}

// drainRecorder records the interactions of RunApply's pending-state drain:
// which actions were actually executed, how the transactor was built, and the
// state keys its Acknowledge was restricted to.
type drainRecorder struct {
	executedActions int
	openedSpec      *pf.MaterializationSpec
	openedVersion   string
	unmarshaled     json.RawMessage
	ackKeys         []string
	ackPatch        *pf.ConnectorState // returned by Acknowledge
	destroyed       bool
}

type drainTestMaterializer struct {
	testMaterializer
	rec *drainRecorder
}

func makeDrainTestMaterializerFn(rec *drainRecorder, is *InfoSchema) NewMaterializerFn[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper] {
	return func(
		ctx context.Context,
		materializationName string,
		endpointConfig testEndpointConfiger,
		featureFlags map[string]bool,
	) (Materializer[testEndpointConfiger, testFieldConfiger, testResourcer, testMappedTyper], error) {
		return &drainTestMaterializer{
			testMaterializer: testMaterializer{calls: &testCalls{}, is: is},
			rec:              rec,
		}, nil
	}
}

func (d *drainTestMaterializer) countingAction() ActionApplyFn {
	return func(context.Context) error {
		d.rec.executedActions++
		return nil
	}
}

func (d *drainTestMaterializer) CreateResource(ctx context.Context, binding MappedBinding[testEndpointConfiger, testResourcer, testMappedTyper]) (string, ActionApplyFn, error) {
	return "", d.countingAction(), nil
}

func (d *drainTestMaterializer) DeleteResource(ctx context.Context, path []string) (string, ActionApplyFn, error) {
	return "", d.countingAction(), nil
}

func (d *drainTestMaterializer) UpdateResource(ctx context.Context, path []string, existing ExistingResource, update BindingUpdate[testEndpointConfiger, testResourcer, testMappedTyper]) (string, ActionApplyFn, error) {
	return "", d.countingAction(), nil
}

func (d *drainTestMaterializer) TruncateResource(ctx context.Context, path []string) (string, ActionApplyFn, error) {
	return "", d.countingAction(), nil
}

func (d *drainTestMaterializer) NewTransactor(ctx context.Context, req pm.Request_Open, is InfoSchema, bindings []MappedBinding[testEndpointConfiger, testResourcer, testMappedTyper], be *m.BindingEvents) (m.Transactor, error) {
	d.rec.openedSpec = req.Materialization
	d.rec.openedVersion = req.Version
	return &drainTestTransactor{rec: d.rec}, nil
}

type drainTestTransactor struct {
	testTransactor
	rec *drainRecorder
}

func (t *drainTestTransactor) UnmarshalState(state json.RawMessage) error {
	t.rec.unmarshaled = state
	return nil
}

func (t *drainTestTransactor) Acknowledge(ctx context.Context, statePatches []json.RawMessage, stateKeys []string) (*pf.ConnectorState, error) {
	t.rec.ackKeys = append([]string(nil), stateKeys...)
	return t.rec.ackPatch, nil
}

func (t *drainTestTransactor) Destroy() {
	t.rec.destroyed = true
}
