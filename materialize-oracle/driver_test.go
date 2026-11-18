package connector

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestExistingTable(t *testing.T) {
	for _, tc := range []struct {
		name      string
		flags     string
		idType    string
		wantError bool
	}{
		{name: "default", idType: "NUMBER", wantError: true},
		{name: "explicit override", flags: "allow_existing_tables_for_new_bindings", idType: "NUMBER"},
		{name: "incompatible key", flags: "allow_existing_tables_for_new_bindings", idType: "CLOB"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.Advanced.FeatureFlags = tc.flags
			spec := testMaterialization(t, cfg)
			d := testDriver(t, &existingTableClient{t: t, idType: tc.idType})

			// A new preview has no last specification, but its destination table
			// and checkpoint already exist from an earlier session.
			validated, err := d.Validate(t.Context(), &pm.Request_Validate{
				Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType,
				Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: spec.Bindings[0].ResourceConfigJson, Collection: spec.Bindings[0].Collection}},
			})
			if tc.wantError {
				require.ErrorContains(t, err, "already exists for new binding")
			} else {
				require.NoError(t, err)
				require.Len(t, validated.Bindings, 1)
				require.Equal(t, []string{"test", "orders"}, validated.Bindings[0].ResourcePath)
				var incompatible bool
				for _, constraint := range validated.Bindings[0].ProjectionConstraints {
					if constraint.Field == "id" {
						require.Equal(t, "ID", constraint.Constraint.FoldedField)
						incompatible = incompatible || constraint.Constraint.Type == pm.Response_Validated_Constraint_INCOMPATIBLE
					}
				}
				require.Equal(t, tc.idType != "NUMBER", incompatible)
				if incompatible {
					return
				}
				applied, err := d.Apply(t.Context(), &pm.Request_Apply{Materialization: spec, Version: "test"})
				require.NoError(t, err)
				require.Empty(t, applied.ActionDescription)
			}

			transactor, opened, _, err := d.NewTransactor(t.Context(), pm.Request_Open{
				Materialization: spec, Version: "test", Range: &pf.RangeSpec{KeyEnd: 0xffffffff, RClockEnd: 0xffffffff},
			}, nil)
			require.NoError(t, err)
			defer transactor.Destroy()
			require.Equal(t, !tc.wantError, opened.DisableLoadOptimization)
			require.NotNil(t, opened.RuntimeCheckpoint)
		})
	}
}

// testDriver is the connector with client in place of a database, and a
// transactor that only returns its fence.
func testDriver(t *testing.T, client sql.Client) *oracleDriver {
	t.Helper()
	d := NewDriver()
	newEndpoint := d.NewEndpoint
	d.PreReqs = func(context.Context, config, map[string]bool) *cerrors.PrereqErr { return nil }
	d.NewEndpoint = func(ctx context.Context, cfg config, flags map[string]bool) (*sql.Endpoint[config], error) {
		ep, err := newEndpoint(ctx, cfg, flags)
		require.NoError(t, err)
		ep.NewClient = func(context.Context, string, *sql.Endpoint[config]) (sql.Client, error) {
			return client, nil
		}
		ep.NewTransactor = func(_ context.Context, _ string, _ map[string]bool, _ *sql.Endpoint[config], fence sql.Fence, _ []sql.Table, _ pm.Request_Open, _ *boilerplate.InfoSchema, _ *m.BindingEvents) (m.Transactor, error) {
			return &transactor{fence: fence}, nil
		}
		return ep, nil
	}
	return d
}

func testMaterialization(t *testing.T, cfg config) *pf.MaterializationSpec {
	t.Helper()
	raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(raw))
	spec.Name = "test/materialization"
	spec.ConfigJson, err = json.Marshal(cfg)
	require.NoError(t, err)
	spec.Bindings = spec.Bindings[:1]
	b := spec.Bindings[0]
	b.ResourceConfigJson = json.RawMessage(`{"table":"orders"}`)
	b.ResourcePath = []string{"test", "orders"}
	b.FieldSelection = pf.FieldSelection{Keys: []string{"id"}, Values: []string{"status"}, Document: "flow_document"}
	b.Collection.Key = []string{"/id"}
	b.Collection.Projections = []pf.Projection{
		{Field: "flow_document", Inference: pf.Inference{Types: []string{"object"}, Exists: pf.Inference_MUST}},
		{Field: "id", Ptr: "/id", IsPrimaryKey: true, Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST}},
		{Field: "status", Ptr: "/status", Inference: pf.Inference{Types: []string{"string"}, Exists: pf.Inference_MAY}},
	}
	b.Collection.WriteSchemaJson = json.RawMessage(`{"type":"object","properties":{"id":{"type":"integer"},"status":{"type":"string"}},"required":["id"]}`)
	require.NoError(t, spec.Validate())
	return &spec
}

type existingTableClient struct {
	sql.Client
	t           *testing.T
	idType      string
	checkpoint  []byte
	statements  []string
	populateErr error
	execErr     error
}

func (c *existingTableClient) PopulateInfoSchema(_ context.Context, is *boilerplate.InfoSchema, _ [][]string) error {
	if c.populateErr != nil {
		return c.populateErr
	}
	is.PushNamespace("TEST")
	resource := is.PushResource("TEST", "ORDERS")
	resource.PushField(boilerplate.ExistingField{Name: "ID", Type: c.idType})
	resource.PushField(boilerplate.ExistingField{Name: "STATUS", Type: "CLOB", Nullable: true})
	resource.PushField(boilerplate.ExistingField{Name: "FLOW_DOCUMENT", Type: "CLOB"})
	is.PushResource("TEST", "FLOW_CHECKPOINTS_V1")
	return nil
}

func (c *existingTableClient) AlterTable(_ context.Context, alter sql.TableAlter) (string, boilerplate.ActionApplyFn, error) {
	require.Empty(c.t, alter.AddColumns)
	require.Empty(c.t, alter.DropNotNulls)
	require.Empty(c.t, alter.ColumnTypeChanges)
	return "", nil, nil
}

func (c *existingTableClient) InstallFence(_ context.Context, _ sql.Table, fence sql.Fence) (sql.Fence, error) {
	if c.checkpoint != nil {
		fence.Checkpoint = c.checkpoint
	}
	return fence, nil
}

func (c *existingTableClient) ExecStatements(_ context.Context, statements []string) error {
	c.statements = append(c.statements, statements...)
	return c.execErr
}

func (c *existingTableClient) Close() {}

func TestCheckCharacterSet(t *testing.T) {
	for _, charset := range []string{"AL32UTF8", "UTF8"} {
		require.NoError(t, checkCharacterSet(charset), charset)
	}
	for _, charset := range []string{"WE8MSWIN1252", "WE8ISO8859P1", "US7ASCII"} {
		require.ErrorContains(t, checkCharacterSet(charset), "the database character set is "+charset, charset)
	}
}

func TestPrereqs(t *testing.T) {
	if !integrationTestsEnabled() {
		t.Skip("skipping Oracle integration test: set TEST_DATABASE=yes or CONNECTOR=materialize-oracle")
	}
	var cfg = integrationConfig()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, cfg.Validate())
	require.Empty(t, preReqs(ctx, cfg, nil).Unwrap())
	db, err := openDB(cfg)
	require.NoError(t, err)
	defer db.Close()
	var one int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&one))
	require.Equal(t, 1, one)
}
