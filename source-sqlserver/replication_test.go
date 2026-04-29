package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLookupTranEndTimesPageEmpty verifies the empty-range fast path: a
// zero-width LSN range returns a non-nil empty map with pageEnd set to the
// requested toLSN.
func TestLookupTranEndTimesPageEmpty(t *testing.T) {
	var db, _ = blackboxTestSetup(t)
	var ctx = context.Background()

	var anchor LSN
	require.NoError(t, db.conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&anchor))

	var rs = &sqlserverReplicationStream{conn: db.conn}
	got, pageEnd, err := rs.lookupTranEndTimesPage(ctx, anchor, anchor, tranEndTimesPageSize)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got)
	require.True(t, bytes.Equal(pageEnd, anchor))
}

// TestLookupTranEndTimesPageBoundary verifies the function's pagination
// contract on single calls: when the database has more transactions in the
// range than fit in a page, a single call must return exactly pageSize
// entries with pageEnd set to the last entry's start_lsn (so the caller can
// resume from there); when the page is not filled, pageEnd must be the
// caller-supplied toLSN.
func TestLookupTranEndTimesPageBoundary(t *testing.T) {
	var db, _ = blackboxTestSetup(t)
	var ctx = context.Background()

	db.CreateTable(t, `<NAME>`, `(id INTEGER PRIMARY KEY)`)

	var fromLSN LSN
	require.NoError(t, db.conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&fromLSN))

	// Insert as separate single-row transactions so each gets a distinct
	// start_lsn in cdc.lsn_time_mapping.
	const numTransactions = 100
	for i := 0; i < numTransactions; i++ {
		db.QuietExec(t, fmt.Sprintf(`INSERT INTO <NAME> VALUES (%d)`, i))
	}

	// Wait for the SQL Server Agent to scan our inserts into the change
	// tables and populate cdc.lsn_time_mapping. The test docker-compose
	// configures @pollinginterval = 1, so a few seconds is plenty.
	var toLSN LSN
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, db.conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&toLSN))
		if bytes.Compare(toLSN, fromLSN) > 0 {
			// Confirm at least the expected number of transactions are visible.
			var n int
			require.NoError(t, db.conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM cdc.lsn_time_mapping WHERE start_lsn > @p1 AND start_lsn <= @p2", fromLSN, toLSN).Scan(&n))
			if n >= numTransactions {
				break
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.Equalf(t, 1, bytes.Compare(toLSN, fromLSN), "agent never advanced past fromLSN; check that the SQL Server Agent is running")

	var rs = &sqlserverReplicationStream{conn: db.conn}

	// Page is full: pageSize < transactions in range. The function must
	// return exactly pageSize entries and signal "more available" by
	// returning pageEnd != toLSN.
	const smallPageSize = 25
	page, pageEnd, err := rs.lookupTranEndTimesPage(ctx, fromLSN, toLSN, smallPageSize)
	require.NoError(t, err)
	require.Lenf(t, page, smallPageSize, "full page must return exactly pageSize entries")
	require.Falsef(t, bytes.Equal(pageEnd, toLSN), "full page must return pageEnd advanced past fromLSN, not at toLSN")
	require.Equalf(t, 1, bytes.Compare(pageEnd, fromLSN), "pageEnd must strictly advance past fromLSN")
	_, ok := page[string(pageEnd)]
	require.Truef(t, ok, "pageEnd must be the start_lsn of an entry in the returned page")

	// Page not filled: pageSize > transactions in range. The function must
	// return all entries and signal "consumed" by returning pageEnd == toLSN.
	const largePageSize = numTransactions * 10
	page, pageEnd, err = rs.lookupTranEndTimesPage(ctx, fromLSN, toLSN, largePageSize)
	require.NoError(t, err)
	require.GreaterOrEqualf(t, len(page), numTransactions, "non-full page must return all entries in range")
	require.Truef(t, bytes.Equal(pageEnd, toLSN), "non-full page must return pageEnd == toLSN")
}
