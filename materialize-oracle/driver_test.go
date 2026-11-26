package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

func startDatabase(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}
	if os.Getenv("ORACLE_ADDRESS") != "" {
		return
	}
	if err := exec.Command("docker", "network", "inspect", "flow-test").Run(); err != nil {
		out, err := exec.Command("docker", "network", "create", "flow-test").CombinedOutput()
		require.NoError(t, err, "%s", out)
	}
	t.Cleanup(func() {
		out, err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").CombinedOutput()
		require.NoError(t, err, "%s", out)
	})
	out, err := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait", "--wait-timeout", "600").CombinedOutput()
	if err != nil {
		logs, _ := exec.Command("docker", "compose", "-f", "docker-compose.yaml", "logs").CombinedOutput()
		t.Logf("%s", logs)
	}
	require.NoError(t, err, "%s", out)
	out, err = exec.Command("docker", "compose", "-f", "docker-compose.yaml", "port", "db", "1521").Output()
	require.NoError(t, err)
	address := strings.TrimSpace(string(out))
	require.NotEmpty(t, address)
	t.Setenv("ORACLE_ADDRESS", address)
}

func TestIntegration(t *testing.T) {
	startDatabase(t)
	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{Table: table, Delta: delta}
	}

	for _, tc := range []struct {
		name           string
		noFlowDocument bool
	}{
		{"materialize", false},
		{"materialize-no-flow-document", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := integrationConfig()
			cfg.Advanced.NoFlowDocument = tc.noFlowDocument
			source := integrationSource(t, "testdata/materialize.flow.yaml", cfg)
			sql.RunMaterializationTest(t, NewDriver().Driver, source, makeResourceFn, nil,
				sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
		})
	}

	t.Run("apply", func(t *testing.T) {
		testutil.SkipRuntimeV1(t)
		driver := NewDriver()
		source := integrationSource(t, "testdata/apply.flow.yaml", integrationConfig())
		// Use the shared implementation directly so Apply tests retain Oracle's
		// validation of identifiers, including forbidden double quotes.
		testutil.RunApplyTest(t, driver, driver.NewMaterializer, source, func(table string, delta bool) tableConfig {
			// The challenging-names fixture has two unbounded text keys, which
			// exceed Oracle's primary-key budget for standard updates.
			return tableConfig{Table: table, Delta: delta || strings.HasPrefix(table, "challengingnames_")}
		}, testutil.RuntimeConfig{})
	})

	t.Run("migrate", func(t *testing.T) {
		testutil.SkipRuntimeV1(t)
		source := integrationSource(t, "testdata/migrate.flow.yaml", integrationConfig())
		sql.RunMigrationTest(t, NewDriver().Driver, source, makeResourceFn, nil)
	})

	t.Run("fence", func(t *testing.T) {
		testutil.SkipRuntimeV1(t)
		cfg := integrationConfig()
		driver := NewDriver()
		newEndpoint := driver.NewEndpoint
		driver.NewEndpoint = func(ctx context.Context, cfg config, flags map[string]bool) (*sql.Endpoint[config], error) {
			ep, err := newEndpoint(ctx, cfg, flags)
			if err != nil {
				return nil, err
			}
			ep.NewClient = func(ctx context.Context, name string, ep *sql.Endpoint[config]) (sql.Client, error) {
				c, err := newClient(ctx, name, ep)
				if err != nil {
					return nil, err
				}
				return &fencingTestClient{c.(*client)}, nil
			}
			return ep, nil
		}
		templates := renderTemplates(createOracleDialect(cfg.DefaultNamespace()))
		sql.RunFencingTest(t, driver.Driver, integrationSource(t, "testdata/fence.flow.yaml", cfg), makeResourceFn,
			templates.createTargetTable, func(ctx context.Context, c sql.Client, fence sql.Fence) error {
				var update strings.Builder
				if err := templates.updateFence.Execute(&update, fence); err != nil {
					return err
				}
				result, err := c.(*fencingTestClient).db.ExecContext(ctx, update.String(),
					go_ora.Clob{String: base64.StdEncoding.EncodeToString(fence.Checkpoint), Valid: true},
					string(fence.Materialization), fence.KeyBegin, fence.KeyEnd, fence.Fence)
				if err != nil {
					return err
				} else if rows, err := result.RowsAffected(); err != nil {
					return err
				} else if rows != 1 {
					return fmt.Errorf("this instance was fenced off by another")
				}
				return nil
			})
	})

	t.Run("truncate", testTruncateAfterBackfill)
	t.Run("delta-updates", testLiveDeltaUpdatesTransition)
	t.Run("oracle", runOracleMaterializationTests)
}

// The shared fencing snapshots use lowercase column names and numeric key order.
type fencingTestClient struct{ *client }

func (c *fencingTestClient) SnapshotTestTable(ctx context.Context, path []string) ([]string, [][]any, error) {
	columns, rows, err := c.client.SnapshotTestTable(ctx, path)
	if err != nil {
		return nil, nil, err
	}
	for i := range columns {
		columns[i] = strings.ToLower(columns[i])
		if columns[i] == "key_begin" || columns[i] == "key_end" || columns[i] == "fence" {
			for _, row := range rows {
				// go-ora returns NUMBER(38) as text, which the snapshot helper
				// preserves as JSON numbers. These metadata values fit in int64.
				if value, ok := row[i].(json.RawMessage); ok {
					number, err := strconv.ParseInt(string(value), 10, 64)
					if err != nil {
						return nil, nil, fmt.Errorf("parsing %s: %w", columns[i], err)
					}
					row[i] = number
				}
			}
		}
	}
	sql.SortRows(rows)
	return columns, rows, nil
}

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

func TestDeltaUpdatesTransition(t *testing.T) {
	// Switching to append-only stores must remove the standard table's primary key.
	for _, tc := range []struct {
		name      string
		lastDelta bool
		nextDelta bool
		backfill  uint32
		wantError bool
	}{
		{name: "standard to delta requires backfill", nextDelta: true, wantError: true},
		{name: "standard to delta with backfill", nextDelta: true, backfill: 1},
		{name: "delta to standard with backfill", lastDelta: true, backfill: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recording := new(recordingSQL)
			db := stdsql.OpenDB(recording)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			c := &client{db: db, ep: &sql.Endpoint[config]{Dialect: testDialect}}
			d := testDriver(t, &validationTestClient{
				Client: c,
				tables: map[string][]boilerplate.ExistingField{
					"ORDERS": {
						{Name: "ID", Type: "NUMBER(38)"},
						{Name: "STATUS", Type: "CLOB", Nullable: true},
						{Name: "FLOW_DOCUMENT", Type: "CLOB"},
					},
					"FLOW_CHECKPOINTS_V1": {},
				},
			})
			last := testMaterialization(t, testConfig())
			last.Bindings[0].DeltaUpdates = tc.lastDelta
			var err error
			last.Bindings[0].ResourceConfigJson, err = json.Marshal(tableConfig{Table: "orders", Delta: tc.lastDelta})
			require.NoError(t, err)
			next := testMaterialization(t, testConfig())
			b := next.Bindings[0]
			b.DeltaUpdates, b.Backfill = tc.nextDelta, tc.backfill
			b.ResourceConfigJson, err = json.Marshal(tableConfig{Table: "orders", Delta: tc.nextDelta})
			require.NoError(t, err)
			_, err = d.Validate(t.Context(), &pm.Request_Validate{
				Name: next.Name, ConfigJson: next.ConfigJson, ConnectorType: next.ConnectorType, LastMaterialization: last,
				Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: b.ResourceConfigJson, Collection: b.Collection, Backfill: b.Backfill}},
			})
			if tc.wantError {
				require.ErrorContains(t, err, "changing delta_updates requires a backfill")
				require.Empty(t, recording.queries())
				return
			}
			require.NoError(t, err)
			req := &pm.Request_Apply{Materialization: next, LastMaterialization: last, Version: "next", LastVersion: "previous"}
			recreate, err := c.MustRecreateResource(req, last.Bindings[0], b)
			require.NoError(t, err)
			require.True(t, recreate)
			_, err = d.Apply(t.Context(), req)
			require.NoError(t, err)
			queries := recording.queries()
			require.Contains(t, queries, "DROP TABLE TEST.ORDERS")
			require.NotContains(t, queries, "TRUNCATE TABLE TEST.ORDERS")
			var created bool
			for _, query := range queries {
				if strings.HasPrefix(strings.TrimSpace(query), "CREATE TABLE TEST.ORDERS") {
					created = true
					require.Equal(t, !tc.nextDelta, strings.Contains(query, "PRIMARY KEY (ID)"))
				}
			}
			require.True(t, created)
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
	startDatabase(t)
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
