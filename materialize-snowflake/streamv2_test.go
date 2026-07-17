package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

// testSidecarPython returns a python interpreter from a venv with the real
// sidecar package (and the snowpipe-streaming SDK) installed, building the
// venv on first use. The venv is shared across test runs; the sidecar package
// itself is installed editable so source changes are always picked up.
func testSidecarPython(t *testing.T) string {
	t.Helper()
	systemPython, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	var venv = filepath.Join(os.TempDir(), "snowpipe-sidecar-test-venv")
	var python = filepath.Join(venv, "bin", "python")
	if _, err := os.Stat(python); err != nil {
		t.Logf("building sidecar test venv at %s", venv)
		if out, err := exec.Command(systemPython, "-m", "venv", venv).CombinedOutput(); err != nil {
			t.Fatalf("creating venv: %v: %s", err, out)
		}
		if out, err := exec.Command(filepath.Join(venv, "bin", "pip"), "install", "--quiet", "-e", "./sidecar").CombinedOutput(); err != nil {
			os.RemoveAll(venv)
			t.Fatalf("installing sidecar package: %v: %s", err, out)
		}
	}
	return python
}

func TestStreamV2Manager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	var python = testSidecarPython(t)
	t.Setenv("SNOWPIPE_SIDECAR_PYTHON", python)

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	_, err = db.ExecContext(ctx, createStageSQL)
	require.NoError(t, err)

	var tableName = "STREAMV2_TEST"
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (key TEXT, intcol NUMBER, doc VARIANT);", tableName))
	require.NoError(t, err)
	defer cleanup()

	var table = sql.Table{
		TableShape: sql.TableShape{Binding: 0},
		Identifier: tableName,
		Keys:       []sql.Column{{Identifier: `KEY`}},
		Values:     []sql.Column{{Identifier: `INTCOL`}, {Identifier: `DOC`}},
		Document:   nil,
	}

	var newManager = func() *streamV2Manager {
		var m = newStreamV2Manager(ctx, &cfg, "test/streamV2Materialization", accountName, 0)
		t.Cleanup(m.stop)
		return m
	}

	var countRows = func() int {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", tableName)).Scan(&count))
		return count
	}

	var writeRows = func(m *streamV2Manager, lo, hi int) {
		for i := lo; i < hi; i++ {
			require.NoError(t, m.writeRow(ctx, db, 0, []any{
				fmt.Sprintf("key-%d", i),
				i,
				json.RawMessage(fmt.Sprintf(`{"nested":{"i":%d},"arr":[1,2]}`, i)),
			}))
		}
	}

	t.Run("happy path", func(t *testing.T) {
		var m = newManager()
		require.NoError(t, m.addBinding(ctx, cfg.Database, cfg.Schema, tableName, table))

		writeRows(m, 0, 100)
		items, err := m.flush("basetok0000000001")
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.NoError(t, m.apply(ctx, db, items[0], false))
		require.Equal(t, 100, countRows())

		// VARIANT columns must round-trip as real JSON objects, not strings.
		var docType string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT TYPEOF(doc) FROM %s WHERE key = 'key-7';`, tableName)).Scan(&docType))
		require.Equal(t, "OBJECT", docType)

		// Re-applying the same item on a recovery pass must be a no-op: a
		// fresh manager (fresh sidecar and channel) reads the committed token
		// and skips every batch. The local spill is removed first so the
		// replay must GET the spilled files back from the internal stage.
		m.stop()
		require.NoError(t, os.RemoveAll(items[0].LocalDir))
		var m2 = newManager()
		require.NoError(t, m2.apply(ctx, db, items[0], true))
		require.Equal(t, 100, countRows())
	})

	t.Run("partial recovery skips committed batches", func(t *testing.T) {
		_, err = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
		require.NoError(t, err)

		var m = newManager()
		require.NoError(t, m.addBinding(ctx, cfg.Database, cfg.Schema, tableName, table))

		writeRows(m, 0, 3)
		items, err := m.flush("recovtok000000001")
		require.NoError(t, err)
		var item = items[0]
		item.BatchRows = 1 // three rows -> batches 0, 1, 2

		// Simulate a crash partway through Acknowledge: append and commit
		// only batch 0, then kill the sidecar without cleanup.
		var reader = newSpillV2Reader(db, item)
		row0, err := reader.next(ctx)
		require.NoError(t, err)
		reader.close()
		require.NoError(t, m.client.Append(ctx, item.Channel, item.BaseToken+":0", []json.RawMessage{row0}))
		require.NoError(t, m.client.WaitCommit(ctx, item.Channel, item.BaseToken+":0"))
		require.Equal(t, 1, countRows())
		m.sup.kill()

		// Recovery: a fresh manager replays the whole item. Batch 0 is
		// skipped per the committed token; batches 1 and 2 are appended.
		// The local spill is gone, exercising the stage GET path.
		require.NoError(t, os.RemoveAll(item.LocalDir))
		var m2 = newManager()
		require.NoError(t, m2.apply(ctx, db, item, true))
		require.Equal(t, 3, countRows())
	})

	t.Run("unsupported table fails loud", func(t *testing.T) {
		// Streaming into a view is not possible. The SDK validates lazily:
		// opening a channel against a view succeeds, and the failure only
		// surfaces when data is ingested. This documents that incompatible
		// tables cannot be detected at addBinding time (so there is no silent
		// fall-through for them), and that the eventual failure is a clean,
		// classified error rather than a hang.
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW STREAMV2_TEST_VIEW AS SELECT * FROM %s;", tableName))
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP VIEW IF EXISTS STREAMV2_TEST_VIEW;")

		var viewTable = table
		viewTable.Identifier = "STREAMV2_TEST_VIEW"

		var m = newManager()
		require.NoError(t, m.addBinding(ctx, cfg.Database, cfg.Schema, "STREAMV2_TEST_VIEW", viewTable))

		require.NoError(t, m.writeRow(ctx, db, 0, []any{"k", 1, json.RawMessage(`{}`)}))
		items, err := m.flush("viewtok0000000001")
		require.NoError(t, err)

		err = m.apply(ctx, db, items[0], false)
		require.Error(t, err)
		var scErr *sidecarError
		require.ErrorAs(t, err, &scErr)
		t.Logf("view ingestion error code: %s message: %s", scErr.Code, scErr.Message)
	})
}
