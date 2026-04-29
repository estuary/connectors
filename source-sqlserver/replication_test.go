package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLookupTranEndTimesPageEmpty verifies the empty-range fast path:
// querying a zero-width range returns an empty (non-nil) map and the
// requested toLSN as the page end.
func TestLookupTranEndTimesPageEmpty(t *testing.T) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	var ctx = context.Background()
	var controlURI = (&Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	conn, err := sql.Open("sqlserver", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.PingContext(ctx))

	var anchor LSN
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&anchor))
	require.NotEmpty(t, anchor)

	var rs = &sqlserverReplicationStream{conn: conn}
	got, pageEnd, err := rs.lookupTranEndTimesPage(ctx, anchor, anchor, tranEndTimesPageSize)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Empty(t, got)
	require.True(t, bytes.Equal(pageEnd, anchor), "empty range must return toLSN as pageEnd")
}

// TestLookupTranEndTimesPagePagination verifies the TOP N pagination boundary:
// inserts more single-row transactions than the page size and walks them in
// pages, asserting the entire range is consumed exactly once with no gaps or
// duplicates.
func TestLookupTranEndTimesPagePagination(t *testing.T) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
	}

	const numTransactions = 1100
	const pageSize = 500 // forces at least 2 pages

	var ctx = context.Background()
	var controlURI = (&Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	conn, err := sql.Open("sqlserver", controlURI)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.PingContext(ctx))

	var probeID = uniqueTableID(t)
	var tableName = fmt.Sprintf("dbo.tsms_page_%s", probeID)
	var captureInstance = fmt.Sprintf("dbo_tsms_page_%s", probeID)

	runSQL := func(query string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, query)
		require.NoErrorf(t, err, "executing %s", query)
	}

	runSQL(fmt.Sprintf(`IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'%s') EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'tsms_page_%s', @capture_instance = N'%s'`, captureInstance, probeID, captureInstance))
	runSQL(fmt.Sprintf(`IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s`, tableName, tableName))
	runSQL(fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY)`, tableName))
	runSQL(fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'tsms_page_%s', @role_name = NULL, @capture_instance = N'%s'`, probeID, captureInstance))
	t.Cleanup(func() {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'%s') EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'tsms_page_%s', @capture_instance = N'%s'`, captureInstance, probeID, captureInstance))
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s`, tableName, tableName))
	})

	var fromLSN LSN
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&fromLSN))

	runSQL(fmt.Sprintf(`DECLARE @i INT = 0; WHILE @i < %d BEGIN INSERT INTO %s (id) VALUES (@i); SET @i = @i + 1; END`, numTransactions, tableName))

	if _, err := conn.ExecContext(ctx, "EXEC sys.sp_cdc_scan @maxtrans = 5000, @maxscans = 10"); err != nil {
		t.Logf("sys.sp_cdc_scan returned: %v (continuing)", err)
	}

	var toLSN LSN
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&toLSN))

	// Read every distinct LSN our test workload produced.
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SELECT DISTINCT __$start_lsn FROM cdc.%s_CT`, captureInstance))
	require.NoError(t, err)
	expectedLSNs := make(map[string]bool)
	for rows.Next() {
		var lsn []byte
		require.NoError(t, rows.Scan(&lsn))
		expectedLSNs[string(lsn)] = true
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equalf(t, numTransactions, len(expectedLSNs), "expected %d distinct LSNs in change table; got %d", numTransactions, len(expectedLSNs))

	// Walk the range in pages exactly the way pollTable does.
	var rs = &sqlserverReplicationStream{conn: conn}
	resolved := make(map[string]time.Time)
	var current = fromLSN
	var pages int
	for bytes.Compare(current, toLSN) < 0 {
		pages++
		page, pageEnd, err := rs.lookupTranEndTimesPage(ctx, current, toLSN, pageSize)
		require.NoError(t, err)
		// pageEnd must strictly advance until we hit toLSN.
		if !bytes.Equal(pageEnd, toLSN) {
			require.Equal(t, 1, bytes.Compare(pageEnd, current), "pageEnd must strictly advance past current")
			require.Lenf(t, page, pageSize, "non-final pages must be full (page %d had %d entries)", pages, len(page))
		}
		for k, v := range page {
			_, dup := resolved[k]
			require.Falsef(t, dup, "LSN %X appeared in multiple pages", []byte(k))
			resolved[k] = v
		}
		current = pageEnd
	}
	require.GreaterOrEqualf(t, pages, 2, "test workload should have produced at least 2 pages; got %d", pages)

	for k := range expectedLSNs {
		_, ok := resolved[k]
		require.Truef(t, ok, "missing tran_end_time for change-table LSN %X (likely a pagination boundary bug)", []byte(k))
	}

	t.Logf("walked %d distinct LSNs across %d pages of size %d", len(resolved), pages, pageSize)
}

// BenchmarkLookupTranEndTimes is a directional A/B comparison for the feature
// itself. The "FeatureOff" sub-benchmarks just iterate the buffered events
// (no DB round-trip) and stand in for the existing streaming-emit path; the
// "FeatureOn" sub-benchmarks issue a single range prefetch against a real
// database and apply the result to each event. The delta between the two is
// the per-pollTable cost the feature adds at a given LSN density in the
// (fromLSN, toLSN] range.
//
// Run with:
//
//	GODEBUG=x509negativeserial=1 TEST_DATABASE=yes \
//	    go test -run='^$' -bench=BenchmarkLookupTranEndTimes -benchmem ./source-sqlserver/...
func BenchmarkLookupTranEndTimes(b *testing.B) {
	if os.Getenv("TEST_DATABASE") != "yes" {
		b.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", b.Name())
	}

	var ctx = context.Background()
	var controlURI = (&Config{
		Address:  *dbControlAddress,
		User:     *dbControlUser,
		Password: *dbControlPass,
		Database: *dbName,
	}).ToURI()
	conn, err := sql.Open("sqlserver", controlURI)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { conn.Close() })
	if err := conn.PingContext(ctx); err != nil {
		b.Fatal(err)
	}

	var probeID = uniqueTableID(b)
	var tableName = fmt.Sprintf("dbo.tsms_bench_%s", probeID)
	var captureInstance = fmt.Sprintf("dbo_tsms_bench_%s", probeID)

	mustExec := func(query string) {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			b.Fatalf("executing %s: %v", query, err)
		}
	}

	mustExec(fmt.Sprintf(`IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'%s') EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'tsms_bench_%s', @capture_instance = N'%s'`, captureInstance, probeID, captureInstance))
	mustExec(fmt.Sprintf(`IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s`, tableName, tableName))
	mustExec(fmt.Sprintf(`CREATE TABLE %s (id INT PRIMARY KEY)`, tableName))
	mustExec(fmt.Sprintf(`EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'tsms_bench_%s', @role_name = NULL, @capture_instance = N'%s'`, probeID, captureInstance))
	b.Cleanup(func() {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`IF EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = N'%s') EXEC sys.sp_cdc_disable_table @source_schema = 'dbo', @source_name = 'tsms_bench_%s', @capture_instance = N'%s'`, captureInstance, probeID, captureInstance))
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(`IF OBJECT_ID(N'%s', 'U') IS NOT NULL DROP TABLE %s`, tableName, tableName))
	})

	var fromLSN LSN
	if err := conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&fromLSN); err != nil {
		b.Fatal(err)
	}

	const totalRows = 50000
	mustExec(fmt.Sprintf(`DECLARE @i INT = 0; WHILE @i < %d BEGIN INSERT INTO %s (id) VALUES (@i); SET @i = @i + 1; END`, totalRows, tableName))
	if _, err := conn.ExecContext(ctx, "EXEC sys.sp_cdc_scan @maxtrans = 100000, @maxscans = 50"); err != nil {
		b.Logf("sys.sp_cdc_scan returned: %v", err)
	}

	var toLSN LSN
	if err := conn.QueryRowContext(ctx, "SELECT sys.fn_cdc_get_max_lsn()").Scan(&toLSN); err != nil {
		b.Fatal(err)
	}

	// Fetch every change-table LSN once so FeatureOff has a realistic event
	// list to iterate (it doesn't query the DB; it just walks the events).
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SELECT __$start_lsn FROM cdc.%s_CT`, captureInstance))
	if err != nil {
		b.Fatal(err)
	}
	var allLSNs []LSN
	for rows.Next() {
		var lsn []byte
		if err := rows.Scan(&lsn); err != nil {
			b.Fatal(err)
		}
		allLSNs = append(allLSNs, lsn)
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	rows.Close()
	if len(allLSNs) < totalRows {
		b.Fatalf("expected at least %d change-table LSNs; got %d", totalRows, len(allLSNs))
	}

	var events = make([]*sqlserverChangeEvent, len(allLSNs))
	for i, lsn := range allLSNs {
		events[i] = &sqlserverChangeEvent{
			Meta: sqlserverChangeMetadataCDC{
				Source: sqlserverSourceInfoCDC{LSN: lsn},
			},
		}
	}

	var rs = &sqlserverReplicationStream{conn: conn}

	for _, n := range []int{100, 1000, 5000, 25000, 50000} {
		if n > len(allLSNs) {
			continue
		}
		var subset = events[:n]
		var pageBound = allLSNs[n-1] // upper LSN that bounds exactly N transactions

		b.Run(fmt.Sprintf("FeatureOff/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, ev := range subset {
					ev.Meta.Source.Millis = 0
				}
			}
		})
		b.Run(fmt.Sprintf("FeatureOn/N=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tranEndTimes, _, err := rs.lookupTranEndTimesPage(ctx, fromLSN, pageBound, tranEndTimesPageSize)
				if err != nil {
					b.Fatal(err)
				}
				for _, ev := range subset {
					if t, ok := tranEndTimes[string(ev.Meta.Source.LSN)]; ok {
						ev.Meta.Source.Millis = t.UnixMilli()
					}
				}
			}
		})
	}
}
