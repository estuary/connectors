package connector

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"regexp"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func mustGetCfg(t *testing.T) config {
	if testing.Short() {
		t.Skip("skipping test in short mode")
		return config{}
	}

	jsonBytes, err := exec.Command("sops", "--decrypt", "--output-type", "json", "testdata/config.local.yaml").Output()
	require.NoError(t, err)

	var out config
	require.NoError(t, json.Unmarshal(jsonBytes, &out))
	require.NoError(t, out.Validate())

	return out
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
			Delta: delta,
		}
	}

	// Staged manifest keys in the connector state carry per-transaction UUIDs.
	sanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}`).
				ReplaceAllString(s, "<uuid>")
		},
	}

	// Two shards exercise the primary shard applying a peer's staged files.
	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, sanitizers,
			sql.RuntimeConfig{Shards: 2})
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("apply-drain", func(t *testing.T) {
		testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			ctx := context.Background()
			tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
			res := makeResourceFn(tableName, false).WithDefaults(cfg)

			// Values for the fields of the testutil drain fixture specs.
			values := map[string]any{
				"key":                  "k1",
				"flow_published_at":    "2024-01-01T00:00:00Z",
				"_meta/flow_truncated": false,
				"optionalBoolean":      true,
				"requiredBoolean":      true,
				"optionalInteger":      2,
				"requiredInteger":      1,
				"optionalString":       "opt",
				"requiredString":       "req",
				"optionalObject":       json.RawMessage(`{}`),
				"requiredObject":       json.RawMessage(`{}`),
				"second_root":          json.RawMessage(`{}`),
				"flow_document":        json.RawMessage(`{}`),
			}

			seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
				b := appliedSpec.Bindings[0]
				fields := append(append([]string{}, b.FieldSelection.Keys...), b.FieldSelection.Values...)
				if b.FieldSelection.Document != "" {
					fields = append(fields, b.FieldSelection.Document)
				}
				var row []any
				for _, f := range fields {
					v, ok := values[f]
					require.True(t, ok, "no seed value for selected field %q", f)
					row = append(row, v)
				}

				entry := stageRows(t, cfg, fields, [][]any{row})
				return mustMarshal(t, pendingState{rangeKeyOf(0, math.MaxUint32): {b.StateKey: entry}})
			}

			verifyDrained := func(t *testing.T, _ *pf.MaterializationSpec, _ []string, rows [][]any) {
				require.Len(t, rows, 1, "the staged transaction's row must have been committed")
			}

			// The drain applies under the fixture spec's own name and leaves
			// its tokens row behind.
			t.Cleanup(func() {
				materializer, err := NewDriver().NewMaterializer(ctx, taskName, cfg, boilerplate.ParseFlags(cfg))
				require.NoError(t, err)
				defer materializer.Close(ctx)
				require.NoError(t, materializer.CleanupTestTask(ctx, "test/sqlite"))
			})

			sql.RunApplyDrainTest(t, NewDriver(), cfg, res, seedPending, verifyDrained)
		})
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	t.Run("idempotency", func(t *testing.T) {
		runIdempotencyTest(t, makeResourceFn)
	})
}

// stageRows stages rows to S3 as a transaction of a binding having the given
// column fields would, returning the entry describing them.
func stageRows(t *testing.T, cfg config, fields []string, rows [][]any) *stagedTransaction {
	t.Helper()
	ctx := context.Background()

	client, err := cfg.toS3Client(ctx, boilerplate.ParseFlags(cfg))
	require.NoError(t, err)

	f := newStagedFile(newS3Store(client, cfg.Bucket), cfg.effectiveBucketPath(), fields)
	f.start()
	for _, row := range rows {
		require.NoError(t, f.writeRow(ctx, row))
	}
	files, err := f.flush()
	require.NoError(t, err)

	return &stagedTransaction{ID: uuid.NewString(), StoreFiles: files}
}

// runIdempotencyTest covers what the runtime-driven tests cannot, because they
// cannot interrupt an apply: a recovered entry whose token is already
// committed is skipped, and a task crossing over from the old checkpoints row
// format hands back its runtime checkpoint exactly once.
func runIdempotencyTest(t *testing.T, makeResourceFn func(string, bool) tableConfig) {
	ctx := context.Background()
	cfg := mustGetCfg(t)
	flags := boilerplate.ParseFlags(cfg)
	suffix := fmt.Sprintf("%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())

	driver := NewDriver()
	ep, err := driver.NewEndpoint(ctx, cfg, flags)
	require.NoError(t, err)
	materializer, err := driver.NewMaterializer(ctx, "acmeCo/tests/idempotency_"+suffix, cfg, flags)
	require.NoError(t, err)
	t.Cleanup(func() { materializer.Close(ctx) })
	client, err := ep.NewClient(ctx, "acmeCo/tests/idempotency_"+suffix, ep)
	require.NoError(t, err)
	t.Cleanup(client.Close)

	// A delta-updates table: with no MERGE the token is all that stands
	// between a re-applied entry and duplicated rows.
	intProjection := func(field string) sql.Projection {
		return sql.Projection{Projection: pf.Projection{
			Field:     field,
			Ptr:       "/" + field,
			Inference: pf.Inference{Types: []string{"integer"}, Exists: pf.Inference_MUST},
		}}
	}
	res := makeResourceFn("idempotency"+suffix, true).WithDefaults(cfg)
	path, _, err := res.Parameters()
	require.NoError(t, err)
	table, err := sql.ResolveTable(sql.TableShape{
		Path:         path,
		Binding:      0,
		DeltaUpdates: true,
		Keys:         []sql.Projection{intProjection("id")},
		Values:       []sql.Projection{intProjection("val")},
	}, ep.Dialect)
	require.NoError(t, err)
	table.StateKey = "idempotency.v1"

	createSQL, err := sql.RenderTableTemplate(table, ep.CreateTableTemplate)
	require.NoError(t, err)
	require.NoError(t, client.ExecStatements(ctx, []string{createSQL}))
	t.Cleanup(func() {
		if _, err := testutil.DropResource(ctx, materializer, path); err != nil {
			t.Log("failed to clean up resource:", err)
		}
	})

	is := boilerplate.InitInfoSchema(materializer.Config())
	require.NoError(t, materializer.PopulateInfoSchema(ctx, is, [][]string{path}))
	_, err = materializer.Setup(ctx, is)
	require.NoError(t, err)

	checkpointsTable := ep.Dialect.Identifier(ep.MetaCheckpoints.Path...)
	newTransactor := func(t *testing.T, taskName string, state json.RawMessage) *transactor {
		t.Helper()
		fence := sql.Fence{
			TablePath:       ep.MetaCheckpoints.Path,
			Materialization: pf.Materialization(taskName),
			KeyBegin:        0,
			KeyEnd:          math.MaxUint32,
		}
		d, err := ep.NewTransactor(ctx, taskName, flags, ep, fence, []sql.Table{table}, pm.Request_Open{StateJson: state}, is, m.NewBindingEvents())
		require.NoError(t, err)
		return d.(*transactor)
	}
	recoverCheckpoint := func(t *testing.T, d *transactor) m.RuntimeCheckpoint {
		t.Helper()
		cp, err := d.RecoverCheckpoint(ctx, pf.MaterializationSpec{}, pf.RangeSpec{})
		require.NoError(t, err)
		return cp
	}
	acknowledge := func(t *testing.T, d *transactor) *pf.ConnectorState {
		t.Helper()
		patch, err := d.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		return patch
	}
	countRows := func(t *testing.T) int {
		t.Helper()
		_, rows, err := materializer.SnapshotTestResource(ctx, path)
		require.NoError(t, err)
		return len(rows)
	}
	stateWith := func(t *testing.T, entry *stagedTransaction) json.RawMessage {
		return mustMarshal(t, pendingState{rangeKeyOf(0, math.MaxUint32): {table.StateKey: entry}})
	}

	t.Run("a committed token settles a recovered entry", func(t *testing.T) {
		taskName := "acmeCo/tests/idempotency_double_apply_" + suffix
		t.Cleanup(func() { require.NoError(t, materializer.CleanupTestTask(ctx, taskName)) })

		before := countRows(t)
		state := stateWith(t, stageRows(t, cfg, table.ColumnNames(), [][]any{{1, 10}, {2, 20}, {3, 30}}))

		// No row yet: the task is new and its first apply commits the entry.
		d := newTransactor(t, taskName, state)
		require.Nil(t, recoverCheckpoint(t, d))
		patch := acknowledge(t, d)
		require.NotNil(t, patch)
		require.JSONEq(t, `{"00000000-ffffffff": {"idempotency.v1": null}}`, string(patch.UpdatedJson))
		require.Equal(t, before+3, countRows(t))

		// The clearing was never persisted: a new session recovers the same
		// entry, whose token is now committed, and skips it.
		d = newTransactor(t, taskName, state)
		patch = acknowledge(t, d)
		require.NotNil(t, patch)
		require.JSONEq(t, `{"00000000-ffffffff": {"idempotency.v1": null}}`, string(patch.UpdatedJson))
		require.Equal(t, before+3, countRows(t))

		// With nothing pending, Acknowledge reports convergence.
		require.Nil(t, acknowledge(t, d))
	})

	t.Run("a task crossing over hands back its checkpoint once", func(t *testing.T) {
		taskName := "acmeCo/tests/idempotency_crossover_" + suffix
		t.Cleanup(func() { require.NoError(t, materializer.CleanupTestTask(ctx, taskName)) })

		checkpoint := []byte("runtime-checkpoint-of-the-old-format")
		compressed, err := compressBytes(checkpoint)
		require.NoError(t, err)
		conn, err := pgx.Connect(ctx, cfg.toURI())
		require.NoError(t, err)
		defer conn.Close(ctx)
		_, err = conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO %s (materialization, key_begin, key_end, fence, checkpoint) VALUES ($1, 0, $2, 7, $3);", checkpointsTable),
			taskName, math.MaxUint32, base64.StdEncoding.EncodeToString(compressed))
		require.NoError(t, err)

		// The row is authoritative while nothing has been staged.
		require.Equal(t, m.RuntimeCheckpoint(checkpoint), recoverCheckpoint(t, newTransactor(t, taskName, nil)))
		require.Equal(t, m.RuntimeCheckpoint(checkpoint), recoverCheckpoint(t, newTransactor(t, taskName, json.RawMessage(`{}`))))

		// Once the runtime has committed a staged transaction, returning the
		// row's checkpoint would rewind it and apply that transaction twice.
		state := stateWith(t, stageRows(t, cfg, table.ColumnNames(), [][]any{{4, 40}}))
		d := newTransactor(t, taskName, state)
		require.Nil(t, recoverCheckpoint(t, d))

		// The first apply rewrites the row, and the checkpoint is gone for good.
		before := countRows(t)
		require.NotNil(t, acknowledge(t, d))
		require.Equal(t, before+1, countRows(t))
		require.Nil(t, recoverCheckpoint(t, newTransactor(t, taskName, nil)))
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	cfg := mustGetCfg(t)

	nonExistentBucket := uuid.NewString()

	tests := []struct {
		name string
		cfg  func(config) config
		want []error
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) config {
				cfg.User = "wrong" + cfg.User
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
		{
			name: "bucket doesn't exist",
			cfg: func(cfg config) config {
				cfg.Bucket = nonExistentBucket
				return cfg
			},
			want: []error{fmt.Errorf("bucket %q does not exist", nonExistentBucket)},
		},
		{
			name: "unauthorized to bucket: access key id",
			cfg: func(cfg config) config {
				cfg.AWSAccessKeyID = "wrong" + cfg.AWSAccessKeyID
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to \"%s%s\"", cfg.Bucket, cfg.BucketPath)},
		},
		{
			name: "unauthorized to bucket: secret access key",
			cfg: func(cfg config) config {
				cfg.AWSSecretAccessKey = "wrong" + cfg.AWSSecretAccessKey
				return cfg
			},
			want: []error{fmt.Errorf("not authorized to write to \"%s%s\"", cfg.Bucket, cfg.BucketPath)},
		},
		{
			name: "database problem and bucket problem",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				cfg.Bucket = nonExistentBucket
				return cfg
			},
			want: []error{
				fmt.Errorf("database %q does not exist", "wrong"+cfg.Database),
				fmt.Errorf("bucket %q does not exist", nonExistentBucket),
			},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(ctx, tt.cfg(cfg)).Unwrap())
		})
	}
}
