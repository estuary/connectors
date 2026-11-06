package sql

import (
	"context"
	"github.com/estuary/connectors/go/common"
	"slices"
	"testing"
	"text/template"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

type stateKeyTestConfig struct{}

func (stateKeyTestConfig) Validate() error                                       { return nil }
func (stateKeyTestConfig) DefaultNamespace() string                              { return "public" }
func (stateKeyTestConfig) FeatureFlags() (string, map[string]common.FlagDefault) { return "", nil }

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

func TestColumnLimit(t *testing.T) {
	var dialect = newTestDialect()
	dialect.MaxColumns = 3

	var s = &sqlMaterialization[boilerplate.EndpointConfiger, Resource]{
		materializationName: "a/materialization",
		endpoint: &Endpoint[boilerplate.EndpointConfiger]{
			Config:              stateKeyTestConfig{},
			Dialect:             dialect,
			CreateTableTemplate: template.Must(template.New("createTargetTable").Parse(`CREATE TABLE {{$.Identifier}};`)),
		},
		client: new(recordingClient),
	}

	var binding = func(values ...string) boilerplate.MappedBinding[boilerplate.EndpointConfiger, Resource, MappedType] {
		var projections []pf.Projection
		for _, f := range slices.Sorted(slices.Values(append([]string{"id"}, values...))) {
			projections = append(projections, pf.Projection{
				Field:     f,
				Ptr:       "/" + f,
				Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
			})
		}
		return boilerplate.MappedBinding[boilerplate.EndpointConfiger, Resource, MappedType]{
			MaterializationSpec_Binding: pf.MaterializationSpec_Binding{
				Collection:     pf.CollectionSpec{Name: "a/collection", Projections: projections},
				FieldSelection: pf.FieldSelection{Keys: []string{"id"}, Values: values},
			},
			Config: stateKeyTestResource{},
		}
	}

	var existing = func(fields ...string) boilerplate.ExistingResource {
		var r = boilerplate.NewInfoSchema(nil, nil, func(f string) string { return f }, false, false).PushResource("public", "a_table")
		for _, f := range fields {
			r.PushField(boilerplate.ExistingField{Name: f})
		}
		return *r
	}

	var update = func(b boilerplate.MappedBinding[boilerplate.EndpointConfiger, Resource, MappedType], newFields ...string) boilerplate.BindingUpdate[boilerplate.EndpointConfiger, Resource, MappedType] {
		var u = boilerplate.BindingUpdate[boilerplate.EndpointConfiger, Resource, MappedType]{Binding: b}
		for _, f := range newFields {
			u.NewProjections = append(u.NewProjections, boilerplate.MappedProjection[MappedType]{
				Projection: boilerplate.Projection{Projection: pf.Projection{Field: f}},
			})
		}
		return u
	}

	var ctx = context.Background()
	var path = []string{"public", "a_table"}

	_, _, err := s.CreateResource(ctx, binding("a", "b"))
	require.NoError(t, err)

	_, _, err = s.CreateResource(ctx, binding("a", "b", "c"))
	require.EqualError(t, err, `table a_database.public.a_table for collection "a/collection" would have 4 columns, which exceeds the destination's limit of 3 columns per table: exclude fields from this binding to reduce the number of columns`)

	_, _, err = s.UpdateResource(ctx, path, existing("id", "a"), update(binding("a", "b"), "b"))
	require.NoError(t, err)

	// A column that is no longer selected still counts toward the limit.
	_, _, err = s.UpdateResource(ctx, path, existing("id", "a", "old"), update(binding("a", "b"), "b"))
	require.EqualError(t, err, `table a_database.public.a_table for collection "a/collection" would have 4 columns (3 existing and 1 new), which exceeds the destination's limit of 3 columns per table: exclude fields from this binding to reduce the number of columns`)

	// A table already at the limit can still be altered if no columns are added.
	_, _, err = s.UpdateResource(ctx, path, existing("id", "a", "old"), boilerplate.BindingUpdate[boilerplate.EndpointConfiger, Resource, MappedType]{
		Binding:             binding("a"),
		NewlyNullableFields: []boilerplate.ExistingField{{Name: "old"}},
	})
	require.NoError(t, err)
}
