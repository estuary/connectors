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

func (m *testMaterializer) PopulateInfoSchema(ctx context.Context, paths [][]string, is *InfoSchema) error {
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

func (m *testMaterializer) NewMaterializerTransactor(ctx context.Context, req pm.Request_Open, is InfoSchema, bindings []MappedBinding[testEndpointConfiger, testResourcer, testMappedTyper], be *m.BindingEvents) (MaterializerTransactor, error) {
	return &testMaterializerTransactor{}, nil
}

func (m *testMaterializer) Close(ctx context.Context) {}

type testMaterializerTransactor struct{}

func (t *testMaterializerTransactor) RecoverCheckpoint(ctx context.Context, spec pf.MaterializationSpec, rangeSpec pf.RangeSpec) (RuntimeCheckpoint, error) {
	panic("unimplemented")
}

func (t *testMaterializerTransactor) UnmarshalState(state json.RawMessage) error {
	panic("unimplemented")
}

func (t *testMaterializerTransactor) Load(it *m.LoadIterator, loaded func(binding int, doc json.RawMessage) error) error {
	panic("unimplemented")
}

func (t *testMaterializerTransactor) Store(it *m.StoreIterator) (m.StartCommitFunc, error) {
	panic("unimplemented")
}

func (t *testMaterializerTransactor) Acknowledge(ctx context.Context) (*pf.ConnectorState, error) {
	panic("unimplemented")
}

func (t *testMaterializerTransactor) Destroy() {
	panic("unimplemented")
}
