package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	cerrors "github.com/estuary/connectors/go/connector-errors"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
)

func testConfig() config {
	return config{
		Address:     "localhost:1522",
		User:        "flow",
		Credentials: credentialsConfig{AuthType: UserPassword, userPasswordConfig: userPasswordConfig{Password: "flowpassword"}},
		Database:    "FREEPDB1",
		SSLMode:     sslDisable,
	}
}

func startDatabase(t *testing.T) {
	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})
}

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	startDatabase(t)

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{Table: table, Delta: delta}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})
	})

	t.Run("truncate", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		testutil.RunTestAllTasks(t, "testdata/truncate.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			var ctx = context.Background()
			var task = sql.PrepareTestTask(t, NewDriver(), bundled, taskName, cfg, makeResourceFn)
			var db = cfg.openDB()
			defer db.Close()

			testutil.RunFlowctl(t, "raw", "preview-next",
				"--name", task.Name,
				"--source", task.SourcePath,
				"--fixture", "testdata/truncate.fixture.json",
				"--shards", "1",
				"--timeout", "5m",
				"--network", "flow-test",
			)

			// The fixture stores ids 1-3, then re-stores only id 1 during a
			// backfill. Only the standard table with a flow_published_at column
			// loses the rows published before the backfill.
			for table, want := range map[string][]int64{
				"truncate_standard":        {1},
				"truncate_delta":           {1, 1, 2, 3},
				"truncate_no_published_at": {1, 2, 3},
			} {
				var rows, err = db.QueryContext(ctx, "SELECT id FROM "+task.Resource(t, table)+" ORDER BY id")
				require.NoError(t, err)
				var ids []int64
				for rows.Next() {
					var id int64
					require.NoError(t, rows.Scan(&id))
					ids = append(ids, id)
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, want, ids, table)
			}
		})
	})

	// Values over 4000 bytes bind as LONG, and must go through both the
	// insert of new keys and the delete and re-insert of existing ones.
	t.Run("large-values", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("fixtures require runtime-next")
		}
		var doc = func(id int, n int) string {
			b, err := json.Marshal(map[string]any{
				"id":     id,
				"small":  strings.Repeat("s", 4001+n),
				"medium": strings.Repeat("m", 40_000+n),
				"large":  strings.Repeat("l", 3<<20+n),
				"bin":    base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{byte(n)}, 50_000+n)),
			})
			require.NoError(t, err)
			return `["tests/large",` + string(b) + "]\n"
		}
		var fixture = filepath.Join(t.TempDir(), "large.fixture.json")
		require.NoError(t, os.WriteFile(fixture, []byte(
			`{"commit": true}`+"\n"+doc(1, 0)+`["tests/large",{"id":2,"small":""}]`+"\n"+`{"commit": true}`+"\n"+
				doc(1, 1)+doc(2, 2)+`{"commit": true}`+"\n"), 0o644))

		testutil.RunTestAllTasks(t, "testdata/large.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			var ctx = context.Background()
			var task = sql.PrepareTestTask(t, NewDriver(), bundled, taskName, cfg, makeResourceFn)
			var db = cfg.openDB()
			defer db.Close()

			testutil.RunFlowctl(t, "raw", "preview-next",
				"--name", task.Name,
				"--source", task.SourcePath,
				"--fixture", fixture,
				"--shards", "1",
				"--timeout", "5m",
				"--network", "flow-test",
			)

			rows, err := db.QueryContext(ctx, "SELECT id, DBMS_LOB.GETLENGTH(small), DBMS_LOB.GETLENGTH(medium), DBMS_LOB.GETLENGTH(large), DBMS_LOB.GETLENGTH(bin), DBMS_LOB.GETLENGTH(flow_document) FROM "+
				task.Resource(t, "large_values")+" ORDER BY id")
			require.NoError(t, err)
			defer rows.Close()
			var got [][]int64
			for rows.Next() {
				var r = make([]int64, 6)
				require.NoError(t, rows.Scan(&r[0], &r[1], &r[2], &r[3], &r[4], &r[5]))
				require.Greater(t, r[5], int64(3<<20), "flow_document of id %d", r[0])
				got = append(got, r[:5])
			}
			require.NoError(t, rows.Err())
			require.Equal(t, [][]int64{
				{1, 4002, 40_001, 3<<20 + 1, 50_001},
				{2, 4003, 40_002, 3<<20 + 2, 50_002},
			}, got)
		})
	})

	// Array inserts whose LOB columns mix values of different bind types keep
	// every value in its own column (see insertRows).
	t.Run("lob-binds", func(t *testing.T) {
		var ctx = context.Background()
		var db = testConfig().openDB()
		defer db.Close()

		var small, mid, long = "x", strings.Repeat("m", 5000), strings.Repeat("l", 3<<20)
		var columns = [][]any{
			{"1", "2", "3", "4", "5"},
			{[]byte(long), nil, []byte(mid), []byte(small), []byte(long)},
			{mid, long, nil, small, long},
			{long, small, mid, nil, ""},
		}
		var table = fmt.Sprintf("lob_binds_%d", time.Now().UnixNano())
		_, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id NUMBER PRIMARY KEY, b BLOB, c0 CLOB, c1 CLOB)")
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP TABLE "+table+" PURGE")

		txn, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		n, err := insertRows(ctx, txn, "INSERT INTO "+table+" (id, b, c0, c1) VALUES (TO_NUMBER(:1), :2, :3, :4)", columns, 1)
		require.NoError(t, err)
		require.NoError(t, txn.Commit())
		require.Equal(t, int64(5), n)

		rows, err := db.QueryContext(ctx, "SELECT b, c0, c1 FROM "+table+" ORDER BY id")
		require.NoError(t, err)
		defer rows.Close()
		for r := 0; rows.Next(); r++ {
			var b []byte
			var c0, c1 *string
			require.NoError(t, rows.Scan(&b, &c0, &c1))
			for i, got := range []any{b, c0, c1} {
				var want = columns[i+1][r]
				if want == nil || want == "" {
					require.Empty(t, got, "row %d column %d", r, i)
				} else if w, ok := want.([]byte); ok {
					require.Equal(t, w, got, "row %d column %d", r, i)
				} else {
					require.Equal(t, want, *(got.(*string)), "row %d column %d", r, i)
				}
			}
		}
		require.NoError(t, rows.Err())
	})

	// CARDINALITY is an undocumented hint, so the plan it gets Loads is pinned
	// here: each key looked up in the primary key, never a full scan.
	t.Run("load-plan", func(t *testing.T) {
		var ctx = context.Background()
		var db = testConfig().openDB()
		defer db.Close()

		var name = fmt.Sprintf("load_plan_%d", time.Now().UnixNano())
		_, err := db.ExecContext(ctx, "CREATE TABLE "+name+" (id NUMBER(38,0) NOT NULL, name VARCHAR2(1000 CHAR) NOT NULL, flow_document CLOB, PRIMARY KEY (id, name))")
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP TABLE "+name+" PURGE")
		_, err = db.ExecContext(ctx, "INSERT INTO "+name+" SELECT level, 'name-' || level, '{}' FROM dual CONNECT BY level <= 100000")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, :1); END;", strings.ToUpper(name))
		require.NoError(t, err)

		var table = sql.Table{
			Identifier: name,
			Keys: []sql.Column{
				{Identifier: "id", MappedType: sql.MappedType{BareDDL: "NUMBER(38,0)"}},
				{Identifier: "name", MappedType: sql.MappedType{BareDDL: "VARCHAR2(1000 CHAR)"}},
			},
			Document: &sql.Column{Identifier: "flow_document"},
		}
		_, err = db.ExecContext(ctx, "EXPLAIN PLAN SET STATEMENT_ID = '"+name+"' FOR "+loadSQL(&table))
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx, "SELECT operation || ' ' || options || ' ' || object_name FROM plan_table WHERE statement_id = :1 ORDER BY id", name)
		require.NoError(t, err)
		var ops []string
		for rows.Next() {
			var op string
			require.NoError(t, rows.Scan(&op))
			ops = append(ops, strings.TrimSpace(op))
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		var plan = strings.Join(ops, "\n")
		require.Contains(t, plan, "NESTED LOOPS", plan)
		require.Contains(t, plan, "INDEX UNIQUE SCAN", plan)
		require.NotContains(t, plan, "TABLE ACCESS FULL", plan)
	})

	// The boundary keeps its fraction, and rounds like stored values into
	// columns of lesser precision.
	t.Run("truncation-boundary", func(t *testing.T) {
		var ctx = context.Background()
		var db = testConfig().openDB()
		defer db.Close()

		var boundary = time.Date(2026, 11, 13, 10, 0, 0, 123_456_789, time.UTC)
		for _, ddl := range []string{"TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP(6)"} {
			var table = fmt.Sprintf("truncation_boundary_%d", time.Now().UnixNano())
			_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id NUMBER, published %s)", table, ddl))
			require.NoError(t, err)

			var offsets = []time.Duration{-time.Second, -time.Microsecond, 0, time.Nanosecond, time.Second}
			for i, off := range offsets {
				_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (:1, TO_TIMESTAMP_TZ(:2, '%s'))", table, timestampFormat),
					i, boundary.Add(off).Format(goTimestampFormat))
				require.NoError(t, err)
			}
			_, err = db.ExecContext(ctx, truncateSQL(table, "published", ddl), boundary.Format(goTimestampFormat))
			require.NoError(t, err)

			var kept []int
			rows, err := db.QueryContext(ctx, "SELECT id FROM "+table+" ORDER BY id")
			require.NoError(t, err)
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				kept = append(kept, id)
			}
			require.NoError(t, rows.Close())
			_, err = db.ExecContext(ctx, "DROP TABLE "+table+" PURGE")
			require.NoError(t, err)

			require.Equal(t, []int{2, 3, 4}, kept, ddl)
		}
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			NewDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			renderTemplates(newOracleDialect("FLOW")).createTargetTable,
			func(ctx context.Context, c sql.Client, fence sql.Fence) error {
				var cl = c.(*client)
				_, err := cl.db.ExecContext(ctx, updateFenceSQL(cl.ep.Dialect, fence),
					go_ora.Clob{String: encodeCheckpoint(fence.Checkpoint), Valid: true})
				return err
			},
		)
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	startDatabase(t)

	for _, tt := range []struct {
		name string
		cfg  func(config) config
		want []string
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Credentials.Password = "wrong" + cfg.Credentials.Password
				return cfg
			},
			want: []string{"ORA-01017"},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []string{"ORA-12514"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var actual = preReqs(context.Background(), tt.cfg(testConfig()), common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{})).Unwrap()
			require.Equal(t, len(tt.want), len(actual), "%v", actual)
			for i := range tt.want {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}

// NewEndpoint runs before the prerequisite checks, so a login failure must be
// reported as a connection failure rather than as the first query it runs.
func TestNewEndpointConnectError(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	startDatabase(t)

	var cfg = testConfig()
	cfg.Credentials.Password = "wrong"

	_, err := NewDriver().NewEndpoint(context.Background(), cfg, common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{}))
	require.ErrorContains(t, err, "unable to connect to database: ")
	require.ErrorContains(t, err, "ORA-01017")

	var userErr *cerrors.UserError
	require.ErrorAs(t, err, &userErr)
}
