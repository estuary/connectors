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
	"slices"
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
			// backfill. Only a standard table with a flow_published_at column
			// loses the rows published before the backfill, or with soft
			// deletes has them marked deleted ("d").
			var tables = map[string][]string{
				"truncate_standard":        {"1"},
				"truncate_delta":           {"1", "1", "2", "3"},
				"truncate_no_published_at": {"1", "2", "3"},
			}
			if !cfg.HardDelete {
				tables = map[string][]string{"truncate_soft": {"1", "2d", "3d"}}
			}
			for table, want := range tables {
				var rows, err = db.QueryContext(ctx, `SELECT id, "_meta/op" FROM `+task.Resource(t, table)+" ORDER BY id")
				require.NoError(t, err)
				var got []string
				for rows.Next() {
					var id int64
					var op *string
					require.NoError(t, rows.Scan(&id, &op))
					if op != nil && *op == "d" {
						got = append(got, fmt.Sprint(id)+"d")
					} else {
						got = append(got, fmt.Sprint(id))
					}
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, want, got, table)
			}
		})
	})

	// A string without a maxLength is a VARCHAR2 until a value doesn't fit,
	// which converts its column to a CLOB and fails the transaction for the
	// runtime to retry. Values over 4000 bytes bind as LONGs, both when new
	// keys are inserted and when existing ones are updated.
	t.Run("large-values", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("fixtures require runtime-next")
		}
		var maxStringBytes int
		var db = testConfig().openDB()
		require.NoError(t, db.QueryRow(maxStringBytesQuery).Scan(&maxStringBytes))
		require.NoError(t, db.Close())

		// A small value has too many bytes, but not characters, for a VARCHAR2.
		var small = func(n int) int { return maxStringBytes/2 + 1 + n }
		var doc = func(id int, n int) string {
			b, err := json.Marshal(map[string]any{
				"id":     id,
				"tiny":   "t",
				"small":  strings.Repeat("é", small(n)),
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

			var args = []string{"raw", "preview-next",
				"--name", task.Name,
				"--source", task.SourcePath,
				"--fixture", fixture,
				"--shards", "1",
				"--timeout", "5m",
				"--network", "flow-test",
			}
			out, err := exec.Command("flowctl", args...).CombinedOutput()
			require.Error(t, err, "%s", out)
			require.Contains(t, string(out), "were converted from VARCHAR2 to CLOB")
			// A new preview starts over, from no checkpoint.
			require.NoError(t, sql.DeleteCheckpointsEntry(ctx, db, "flow_checkpoints_v1", task.Name))
			testutil.RunFlowctl(t, args...)

			var types = make(map[string]string)
			rows, err := db.QueryContext(ctx, "SELECT column_name, data_type FROM user_tab_columns WHERE table_name = UPPER(:1)", task.Resource(t, "large_values"))
			require.NoError(t, err)
			for rows.Next() {
				var name, typ string
				require.NoError(t, rows.Scan(&name, &typ))
				types[name] = typ
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			for col, want := range map[string]string{"TINY": "VARCHAR2", "SMALL": "CLOB", "MEDIUM": "CLOB", "LARGE": "CLOB"} {
				require.Equal(t, want, types[col], col)
			}

			rows, err = db.QueryContext(ctx, "SELECT id, DBMS_LOB.GETLENGTH(small), DBMS_LOB.GETLENGTH(medium), DBMS_LOB.GETLENGTH(large), DBMS_LOB.GETLENGTH(bin), DBMS_LOB.GETLENGTH(flow_document) FROM "+
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
				{1, int64(small(1)), 40_001, 3<<20 + 1, 50_001},
				{2, int64(small(2)), 40_002, 3<<20 + 2, 50_002},
			}, got)
		})
	})

	// Rows mixing LONG binds and others, NULLs and empty strings keep each
	// value in its own column (see writeRows).
	t.Run("lob-binds", func(t *testing.T) {
		var ctx = context.Background()
		var db = testConfig().openDB()
		defer db.Close()

		var small, mid, long = "x", strings.Repeat("m", 5000), strings.Repeat("l", 3<<20)
		// Each row is an id and the values of b, c0 and c1.
		var inserted = [][]any{
			{"1", []byte(long), mid, long},
			{"2", nil, long, small},
			{"3", []byte(mid), nil, mid},
			{"4", []byte(small), small, nil},
			{"5", []byte(long), long, ""},
		}
		// The update moves every value to another row.
		var updated = [][]any{
			{"1", nil, long, ""},
			{"2", []byte(mid), mid, long},
			{"3", []byte(small), long, small},
			{"4", []byte(long), nil, mid},
			{"5", []byte(long), small, nil},
		}
		var maxInlineBytes int
		require.NoError(t, db.QueryRowContext(ctx, maxStringBytesQuery).Scan(&maxInlineBytes))
		require.Contains(t, []int{4000, 32767}, maxInlineBytes)

		var table = fmt.Sprintf("lob_binds_%d", time.Now().UnixNano())
		_, err := db.ExecContext(ctx, "CREATE TABLE "+table+" (id NUMBER PRIMARY KEY, b BLOB, c0 CLOB, c1 CLOB)")
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP TABLE "+table+" PURGE")

		var updateRows [][]any
		for _, row := range updated {
			updateRows = append(updateRows, append(slices.Clone(row[1:]), row[0]))
		}
		for _, tc := range []struct {
			query string
			rows  [][]any
			want  [][]any
		}{
			{"INSERT INTO " + table + " (id, b, c0, c1) VALUES (TO_NUMBER(:1), :2, :3, :4)", inserted, inserted},
			{"UPDATE " + table + " SET b = :1, c0 = :2, c1 = :3 WHERE id = TO_NUMBER(:4)", updateRows, updated},
		} {
			txn, err := db.BeginTx(ctx, nil)
			require.NoError(t, err)
			n, err := writeRows(ctx, txn, tc.query, tc.rows, maxInlineBytes)
			require.NoError(t, err)
			require.NoError(t, txn.Commit())
			require.Equal(t, int64(5), n)

			rows, err := db.QueryContext(ctx, "SELECT b, c0, c1 FROM "+table+" ORDER BY id")
			require.NoError(t, err)
			for r := 0; rows.Next(); r++ {
				var b []byte
				var c0, c1 *string
				require.NoError(t, rows.Scan(&b, &c0, &c1))
				for i, got := range []any{b, c0, c1} {
					var want = tc.want[r][i+1]
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
			require.NoError(t, rows.Close())
		}
	})

	// Loads find the rows of keys of every type, and look up each key in the
	// primary key without scanning the table in full.
	t.Run("load-plan", func(t *testing.T) {
		var ctx = context.Background()
		var db = testConfig().openDB()
		defer db.Close()
		// The load table and the plan are private to a session.
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		var name = fmt.Sprintf("load_plan_%d", time.Now().UnixNano())
		_, err = conn.ExecContext(ctx, "CREATE TABLE "+name+" (id NUMBER(38,0) NOT NULL, flag BOOLEAN NOT NULL, bin RAW(2000) NOT NULL, name VARCHAR2(1000 CHAR) NOT NULL, flow_document CLOB, PRIMARY KEY (id, flag, bin, name))")
		require.NoError(t, err)
		defer conn.ExecContext(ctx, "DROP TABLE "+name+" PURGE")
		_, err = conn.ExecContext(ctx, "INSERT INTO "+name+` SELECT level, TO_BOOLEAN(MOD(level, 2)), HEXTORAW(TO_CHAR(level, 'FM0000000X')), 'name-' || level, '{"id":' || level || '}' FROM dual CONNECT BY level <= 100000`)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, "BEGIN DBMS_STATS.GATHER_TABLE_STATS(USER, :1); END;", strings.ToUpper(name))
		require.NoError(t, err)

		var table = sql.Table{
			Identifier: name,
			Keys: []sql.Column{
				{Identifier: "id", MappedType: sql.MappedType{BareDDL: "NUMBER(38,0)"}},
				{Identifier: "flag", MappedType: sql.MappedType{BareDDL: "BOOLEAN"}},
				{Identifier: "bin", MappedType: sql.MappedType{BareDDL: "RAW(2000)"}},
				{Identifier: "name", MappedType: sql.MappedType{BareDDL: "VARCHAR2(1000 CHAR)"}},
			},
			Document: &sql.Column{Identifier: "flow_document"},
		}
		createLoadTable, err := sql.RenderTableTemplate(table, testTemplates.createLoadTable)
		require.NoError(t, err)
		loadInsert, err := sql.RenderTableTemplate(table, testTemplates.loadInsert)
		require.NoError(t, err)
		loadQuery, err := sql.RenderTableTemplate(table, testTemplates.loadQuery)
		require.NoError(t, err)

		// The keys of rows 7 and 254, and one that has no row.
		_, err = conn.ExecContext(ctx, createLoadTable)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, loadInsert,
			[]any{"7", "254", "100001"},
			[]any{true, false, true},
			[]any{[]byte{0, 0, 0, 7}, []byte{0, 0, 0, 254}, []byte{0, 1, 0x86, 0xA1}},
			[]any{"name-7", "name-254", "name-100001"},
		)
		require.NoError(t, err)

		rows, err := conn.QueryContext(ctx, loadQuery)
		require.NoError(t, err)
		var docs []string
		for rows.Next() {
			var doc string
			require.NoError(t, rows.Scan(&doc))
			docs = append(docs, doc)
		}
		require.NoError(t, rows.Err())
		require.ElementsMatch(t, []string{`{"id":7}`, `{"id":254}`}, docs)

		_, err = conn.ExecContext(ctx, "EXPLAIN PLAN SET STATEMENT_ID = '"+name+"' FOR "+loadQuery)
		require.NoError(t, err)

		rows, err = conn.QueryContext(ctx, "SELECT operation || ' ' || options || ' ' || object_name FROM plan_table WHERE statement_id = :1 ORDER BY id", name)
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
		require.NotContains(t, plan, "TABLE ACCESS FULL "+strings.ToUpper(name), plan)
	})

	// The boundary keeps its fraction.
	t.Run("truncation-boundary", func(t *testing.T) {
		var ctx = context.Background()
		var db = testConfig().openDB()
		defer db.Close()

		var boundary = time.Date(2026, 11, 13, 10, 0, 0, 123_456_789, time.UTC)
		var table = fmt.Sprintf("truncation_boundary_%d", time.Now().UnixNano())
		_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id NUMBER, published %s)", table, timestampDDL))
		require.NoError(t, err)

		var offsets = []time.Duration{-time.Second, -time.Nanosecond, 0, time.Nanosecond, time.Second}
		for i, off := range offsets {
			_, err := db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s VALUES (:1, TO_TIMESTAMP_TZ(:2, '%s'))", table, timestampFormat),
				i, boundary.Add(off).Format(goTimestampFormat))
			require.NoError(t, err)
		}
		_, err = db.ExecContext(ctx, truncateSQL(table, "published", ""), boundary.Format(goTimestampFormat))
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

		require.Equal(t, []int{2, 3, 4}, kept)
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
			testTemplates.createTargetTable,
			func(ctx context.Context, c sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				_, err := c.(*client).db.ExecContext(ctx, fenceUpdate.String(),
					go_ora.Clob{String: base64.StdEncoding.EncodeToString(fence.Checkpoint), Valid: true})
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
		{
			name: "TLS on a plain TCP port",
			cfg: func(cfg config) config {
				cfg.SSLMode = sslVerifyFull
				return cfg
			},
			want: []string{"the TLS handshake failed, so the port of address"},
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
