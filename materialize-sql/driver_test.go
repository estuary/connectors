package sql

import (
	"context"
	"testing"
	"text/template"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

type stateKeyTestConfig struct{}

func (stateKeyTestConfig) Validate() error                         { return nil }
func (stateKeyTestConfig) DefaultNamespace() string                { return "public" }
func (stateKeyTestConfig) FeatureFlags() (string, map[string]bool) { return "", nil }

type stateKeyTestResource struct{}

func (stateKeyTestResource) Validate() error { return nil }
func (stateKeyTestResource) Parameters() ([]string, bool, error) {
	return []string{"a_database", "public", "a_table"}, true, nil
}
func (r stateKeyTestResource) WithDefaults(boilerplate.EndpointConfiger) Resource { return r }

// recordingClient captures the TableCreate it is given, so a test can inspect
// the table the Apply path resolved.
type recordingClient struct {
	created TableCreate
}

func (c *recordingClient) CreateTable(ctx context.Context, tc TableCreate) error {
	c.created = tc
	return nil
}

func (c *recordingClient) ExecStatements(ctx context.Context, statements []string) error { return nil }
func (c *recordingClient) InstallFence(ctx context.Context, checkpoints Table, fence Fence) (Fence, error) {
	return fence, nil
}
func (c *recordingClient) PopulateInfoSchema(ctx context.Context, is *boilerplate.InfoSchema, resourcePaths [][]string) error {
	return nil
}
func (c *recordingClient) DeleteTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}
func (c *recordingClient) AlterTable(ctx context.Context, ta TableAlter) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}
func (c *recordingClient) TruncateTable(ctx context.Context, path []string) (string, boilerplate.ActionApplyFn, error) {
	return "", nil, nil
}
func (c *recordingClient) MustRecreateResource(req *pm.Request_Apply, lastBinding, newBinding *pf.MaterializationSpec_Binding) (bool, error) {
	return false, nil
}
func (c *recordingClient) ListCheckpointsEntries(ctx context.Context) ([]string, error) {
	return nil, nil
}
func (c *recordingClient) DeleteCheckpointsEntry(ctx context.Context, taskName string) error {
	return nil
}
func (c *recordingClient) SnapshotTestTable(ctx context.Context, path []string) ([]string, [][]any, error) {
	return nil, nil, nil
}
func (c *recordingClient) Close() {}

// A connector's CreateTable may need the state key of the binding the table is
// being created for, to record in the destination which generation of the
// binding owns the table. The transactor path has always had it; this asserts
// that the Apply path does too.
func TestCreateResourceCarriesStateKey(t *testing.T) {
	var client = new(recordingClient)

	var s = &sqlMaterialization[boilerplate.EndpointConfiger, Resource]{
		materializationName: "a/materialization",
		endpoint: &Endpoint[boilerplate.EndpointConfiger]{
			Config:              stateKeyTestConfig{},
			Dialect:             newTestDialect(),
			CreateTableTemplate: template.Must(template.New("createTargetTable").Parse(`CREATE TABLE {{$.Identifier}};`)),
		},
		client: client,
	}

	var binding = boilerplate.MappedBinding[boilerplate.EndpointConfiger, Resource, MappedType]{
		MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
			StateKey: "a_table.v3",
			Collection: pf.CollectionSpec{
				Name: "a/collection",
				Projections: []pf.Projection{{
					Field: "id",
					Inference: pf.Inference{
						Types:  []string{"integer"},
						Exists: pf.Inference_MUST,
					},
				}},
			},
			FieldSelection: pf.FieldSelection{Keys: []string{"id"}},
		},
		Config: stateKeyTestResource{},
	}

	_, apply, err := s.CreateResource(context.Background(), binding)
	require.NoError(t, err)
	require.NoError(t, apply(context.Background()))

	require.Equal(t, "a_table.v3", client.created.StateKey)
}
