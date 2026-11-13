package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	"github.com/stretchr/testify/require"
)

// partitionedRow is the (a, b) primary key of a row in a partitioned test table.
type partitionedRow struct{ A, B int }

// partitionedRows generates 16 rows whose primary key (a, b) is partitioned on b. Row a
// goes in band a%4, so every partition holds keys interleaved with every other one. A
// global ORDER BY a, b therefore visits the partitions round-robin and never finishes one
// before starting the next.
func partitionedRows(bands [4]int) []partitionedRow {
	var rows []partitionedRow
	for a := 1; a <= 16; a++ {
		rows = append(rows, partitionedRow{A: a, B: bands[a%4] + ((a*7)%10)*100})
	}
	return rows
}

// createPartitionScheme creates a partition function over a parameter of the given type
// with the given boundaries, and a partition scheme on it for <NAME> to be created on.
// rangeDir is LEFT or RIGHT. Both names end in suffix so a test can create more than one
// scheme. It returns the names of the function and the scheme.
func createPartitionScheme(t *testing.T, db *sqlserverTestDatabase, suffix, paramType, rangeDir, boundaries string) (string, string) {
	t.Helper()
	var function, scheme = db.Expand("pf_<ID>" + suffix), db.Expand("ps_<ID>" + suffix)

	// Drop leftovers of interrupted earlier runs
	db.QuietExec(t, `IF OBJECT_ID('<NAME>', 'U') IS NOT NULL DROP TABLE <NAME>`)
	var dropScheme = fmt.Sprintf(`IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '%[1]s') DROP PARTITION SCHEME [%[1]s]`, scheme)
	var dropFunction = fmt.Sprintf(`IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '%[1]s') DROP PARTITION FUNCTION [%[1]s]`, function)
	db.QuietExec(t, dropScheme)
	db.QuietExec(t, dropFunction)

	db.Exec(t, fmt.Sprintf(`CREATE PARTITION FUNCTION [%s] (%s) AS RANGE %s FOR VALUES (%s)`, function, paramType, rangeDir, boundaries))
	t.Cleanup(func() { db.QuietExec(t, dropFunction) })
	db.Exec(t, fmt.Sprintf(`CREATE PARTITION SCHEME [%s] AS PARTITION [%s] ALL TO ([PRIMARY])`, scheme, function))
	t.Cleanup(func() { db.QuietExec(t, dropScheme) })
	return function, scheme
}

// createPartitionedTable creates <NAME> with clustered primary key (a, b) on a partition
// scheme over b with the given range direction and boundaries, fills it with rows, and
// returns the partition function's name.
func createPartitionedTable(t *testing.T, db *sqlserverTestDatabase, rangeDir, boundaries string, rows []partitionedRow) string {
	t.Helper()
	var function, scheme = createPartitionScheme(t, db, "", "INT", rangeDir, boundaries)
	db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(a INT NOT NULL, b INT NOT NULL, data VARCHAR(32), PRIMARY KEY CLUSTERED (a, b)) ON [%s](b)`, scheme))

	var values []string
	for _, row := range rows {
		values = append(values, fmt.Sprintf("(%d, %d, 'row %d/%d')", row.A, row.B, row.A, row.B))
	}
	db.Exec(t, `INSERT INTO <NAME> VALUES `+strings.Join(values, ", "))
	return function
}

// backfilledDocsJSON returns the backfill documents in the output of a capture run, in the
// order they were emitted.
func backfilledDocsJSON(t *testing.T, output []byte) []json.RawMessage {
	t.Helper()
	var docs []json.RawMessage
	for line := range bytes.SplitSeq(output, []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var message []json.RawMessage
		require.NoError(t, json.Unmarshal(line, &message))
		require.Len(t, message, 2)
		var doc struct {
			Meta struct {
				Source struct {
					Snapshot bool `json:"snapshot"`
				} `json:"source"`
			} `json:"_meta"`
		}
		require.NoError(t, json.Unmarshal(message[1], &doc))
		if doc.Meta.Source.Snapshot {
			docs = append(docs, message[1])
		}
	}
	return docs
}

// backfilledRows returns the keys of the backfill documents in the output of a capture
// run, in the order they were emitted.
func backfilledRows(t *testing.T, output []byte) []partitionedRow {
	t.Helper()
	var rows []partitionedRow
	for _, doc := range backfilledDocsJSON(t, output) {
		var row struct {
			A int `json:"a"`
			B int `json:"b"`
		}
		require.NoError(t, json.Unmarshal(doc, &row))
		rows = append(rows, partitionedRow{A: row.A, B: row.B})
	}
	return rows
}

// keyOrder sorts rows by primary key (a, b).
func keyOrder(rows []partitionedRow) []partitionedRow {
	rows = slices.Clone(rows)
	slices.SortFunc(rows, func(x, y partitionedRow) int {
		if x.A != y.A {
			return x.A - y.A
		}
		return x.B - y.B
	})
	return rows
}

// partitionOrder sorts rows partition by partition, ascending, and by primary key within
// each, where boundaries are those of a RANGE RIGHT or RANGE LEFT partition function. A
// row on a boundary belongs to the partition above it under RANGE RIGHT, and to the one
// below it under RANGE LEFT.
func partitionOrder(rows []partitionedRow, boundaries []int, rangeRight bool) []partitionedRow {
	var partitionOf = func(b int) int {
		var n = 1
		for _, boundary := range boundaries {
			if b > boundary || (rangeRight && b == boundary) {
				n++
			}
		}
		return n
	}
	rows = keyOrder(rows)
	slices.SortStableFunc(rows, func(x, y partitionedRow) int { return partitionOf(x.B) - partitionOf(y.B) })
	return rows
}

// discoverPartitionedTable sets the capture's feature flags, discovers the test table, and
// sets a backfill chunk size of 2 rows.
func discoverPartitionedTable(t *testing.T, tc *blackbox.TranscriptCapture, featureFlags string) {
	t.Helper()
	require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", featureFlags))
	tc.Discover("Discover Tables")
	require.NoError(t, tc.Capture.EditConfig("advanced.backfill_chunk_size", 2))
}

// TestPartitionedBackfill checks that a table whose clustered primary key is partitioned
// on its second column is backfilled one partition at a time, and that the
// no_partition_aware_backfill flag restores the key-ordered scan.
func TestPartitionedBackfill(t *testing.T) {
	// The fourth partition, [3000, 4000), is empty, so the backfill has to step over it.
	var boundaries = []int{1000, 2000, 3000, 4000}
	var rows = partitionedRows([4]int{0, 1000, 2000, 4000})

	t.Run("PartitionOrder", func(t *testing.T) {
		t.Parallel()
		var db, tc = blackboxTestSetup(t)
		createPartitionedTable(t, db, "RIGHT", "1000, 2000, 3000, 4000", rows)
		discoverPartitionedTable(t, tc, "partition_aware_backfill")
		var output = tc.Run("Backfill", -1)
		require.Equal(t, partitionOrder(rows, boundaries, true), backfilledRows(t, output))
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})

	t.Run("FeatureFlagDisabled", func(t *testing.T) {
		t.Parallel()
		var db, tc = blackboxTestSetup(t)
		createPartitionedTable(t, db, "RIGHT", "1000, 2000, 3000, 4000", rows)
		discoverPartitionedTable(t, tc, "no_partition_aware_backfill")
		var output = tc.Run("Backfill", -1)
		require.Equal(t, keyOrder(rows), backfilledRows(t, output))
		cupaloy.SnapshotT(t, tc.Transcript.String())
	})

	// A leading NULL boundary adds an empty partition below every other one, and SQL
	// Server's partition numbers shift up by one. A backfill which ignored it would never
	// read the last partition. Row (10, 2000) is on a boundary, so the two range
	// directions put it in different partitions.
	for _, rangeDir := range []string{"RIGHT", "LEFT"} {
		t.Run("NullBoundary"+rangeDir, func(t *testing.T) {
			t.Parallel()
			var db, tc = blackboxTestSetup(t)
			createPartitionedTable(t, db, rangeDir, "NULL, 1000, 2000, 3000, 4000", rows)
			discoverPartitionedTable(t, tc, "partition_aware_backfill")
			var output = tc.Run("Backfill", -1)
			require.Equal(t, partitionOrder(rows, boundaries, rangeDir == "RIGHT"), backfilledRows(t, output))
			cupaloy.SnapshotT(t, tc.Transcript.String())
		})
	}
}

// rewindToBackfill edits the checkpoint of the only binding back into an unfiltered
// backfill resuming after the row key (a, b), and with the given metadata if non-nil.
func rewindToBackfill(t *testing.T, db *sqlserverTestDatabase, capture *blackbox.Capture, cursor partitionedRow, metadata any) {
	t.Helper()
	var key []tuple.TupleElement
	for _, v := range []int{cursor.A, cursor.B} {
		var encoded, err = datatypes.EncodeKeyFDB(int64(v), "int")
		require.NoError(t, err)
		key = append(key, encoded)
	}
	var packed, err = sqlcapture.PackTuple(key)
	require.NoError(t, err)

	var tableName = db.Expand(`<NAME>`)
	var stateKey = "bindingStateV1." + strings.Replace(tableName, ".", "%2F", 1)
	require.NoError(t, capture.EditCheckpoint(stateKey+".mode", sqlcapture.TableStateUnfilteredBackfill))
	require.NoError(t, capture.EditCheckpoint(stateKey+".scanned", base64.StdEncoding.EncodeToString(packed)))
	if metadata != nil {
		require.NoError(t, capture.EditCheckpoint(stateKey+".metadata", metadata))
	}
}

// partitionMetadata is the backfill metadata of a partition-ordered backfill reading
// partition [2000, 3000) of the given RANGE RIGHT partition function over b.
func partitionMetadata(function string) map[string]any {
	return map[string]any{
		"partition": map[string]any{"function": function, "column": "b", "lo": "2000", "hi": "3000", "range_right": true},
	}
}

// TestPartitionedBackfillDDL resumes a partition-ordered backfill after the partition
// function has been altered, and checks that no row which was still ahead of the backfill
// is lost. Rows may be emitted more than once.
//
// The backfill is rewound to resume in partition 3, [2000, 3000), after key (6, 2200).
// Rows of partitions 1 and 2 and the rows of partition 3 up to that key count as already
// backfilled; the rest of partition 3 and all of partition 4 are still to come.
func TestPartitionedBackfillDDL(t *testing.T) {
	var boundaries = []int{1000, 2000, 3000}
	var rows = partitionedRows([4]int{0, 1000, 2000, 3000})
	var cursor = partitionedRow{A: 6, B: 2200}

	var pending []partitionedRow
	for _, row := range rows {
		var afterCursor = row.A > cursor.A || (row.A == cursor.A && row.B > cursor.B)
		if row.B >= 3000 || (row.B >= 2000 && afterCursor) {
			pending = append(pending, row)
		}
	}
	// The cases below rely on these rows: (10, 2000) sits in partition 3 below the
	// cursor's b but after it by key, and (3, 3100) in partition 4 before it by key.
	require.Contains(t, pending, partitionedRow{A: 10, B: 2000})
	require.Contains(t, pending, partitionedRow{A: 3, B: 3100})

	for _, tc := range []struct {
		name string
		ddl  string
		from int // The backfill must not go back to rows whose b is below this.
	}{
		// Splitting the current partition at or below the cursor's b moves the cursor
		// into the upper half, leaving (10, 2000) behind in the lower one.
		{"SplitInside", `ALTER PARTITION FUNCTION [<FUNCTION>]() SPLIT RANGE (2200)`, 2000},
		// Merging the next partition into the current one puts (3, 3100) behind the
		// cursor by key.
		{"MergeNext", `ALTER PARTITION FUNCTION [<FUNCTION>]() MERGE RANGE (3000)`, 2000},
		// Merging the current partition into the one before it removes the lower bound
		// the backfill tracks it by, so the backfill re-reads the merged partition,
		// [1000, 3000), and not the first one.
		{"MergePrevious", `ALTER PARTITION FUNCTION [<FUNCTION>]() MERGE RANGE (2000)`, 1000},
		// These two are controls. Neither touches the current partition.
		{"SplitAbove", `ALTER PARTITION FUNCTION [<FUNCTION>]() SPLIT RANGE (3500)`, 2000},
		{"MergeBelow", `ALTER PARTITION FUNCTION [<FUNCTION>]() MERGE RANGE (1000)`, 2000},
		// Adding a NULL boundary moves no rows, but SQL Server's partition numbers shift
		// up by one. A query by the old number would go back to [1000, 2000) instead of
		// continuing in [2000, 3000).
		{"SplitNull", `ALTER PARTITION FUNCTION [<FUNCTION>]() SPLIT RANGE (NULL)`, 2000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var db, capture = blackboxTestSetup(t)
			var function = createPartitionedTable(t, db, "RIGHT", "1000, 2000, 3000", rows)
			discoverPartitionedTable(t, capture, "partition_aware_backfill")
			var initial = capture.Run("Initial Backfill", -1)
			require.Equal(t, partitionOrder(rows, boundaries, true), backfilledRows(t, initial))

			rewindToBackfill(t, db, capture.Capture, cursor, partitionMetadata(function))
			db.Exec(t, strings.ReplaceAll(tc.ddl, "<FUNCTION>", function))
			var resumed = backfilledRows(t, capture.Run("Resumed Backfill", -1))
			for _, row := range pending {
				require.Contains(t, resumed, row, "row %v was not backfilled", row)
			}
			for _, row := range resumed {
				require.GreaterOrEqual(t, row.B, tc.from, "row %v was backfilled again", row)
			}
			cupaloy.SnapshotT(t, capture.Transcript.String())
		})
	}
}

// TestPartitionedBackfillRepartitioned resumes a partition-ordered backfill after its table
// was moved to a RANGE LEFT partition function with the same boundaries. The stored range
// [2000, 3000) has the same lower bound in the new layout, but row (10, 2000), which comes
// after the cursor, is now in (1000, 2000], below it. Only a restart from the first
// partition reads that row again.
func TestPartitionedBackfillRepartitioned(t *testing.T) {
	t.Parallel()
	var boundaries = []int{1000, 2000, 3000}
	var rows = partitionedRows([4]int{0, 1000, 2000, 3000})
	var cursor = partitionedRow{A: 6, B: 2200}

	var db, tc = blackboxTestSetup(t)
	// Created before the table so that the cleanups drop the table first.
	var _, leftScheme = createPartitionScheme(t, db, "_left", "INT", "LEFT", "1000, 2000, 3000")
	var function = createPartitionedTable(t, db, "RIGHT", "1000, 2000, 3000", rows)
	discoverPartitionedTable(t, tc, "partition_aware_backfill")
	require.Equal(t, partitionOrder(rows, boundaries, true), backfilledRows(t, tc.Run("Initial Backfill", -1)))

	rewindToBackfill(t, db, tc.Capture, cursor, partitionMetadata(function))
	db.Exec(t, fmt.Sprintf(`DECLARE @pk sysname = (SELECT name FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID('<NAME>') AND type = 'PK');
		DECLARE @sql nvarchar(max) = 'CREATE UNIQUE CLUSTERED INDEX ' + QUOTENAME(@pk) + ' ON <NAME> (a, b) WITH (DROP_EXISTING = ON) ON [%s](b)';
		EXEC(@sql)`, leftScheme))
	require.Equal(t, partitionOrder(rows, boundaries, false), backfilledRows(t, tc.Run("Resumed Backfill", -1)))
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestPartitionedBackfillDatetime checks a partition-ordered backfill of a table
// partitioned on a DATETIME column. The connector binds DATETIME resume keys as strings
// truncated to milliseconds (see retypeResumeKey in go/capture/sqlserver/backfill), which
// is safe here because it finds the current partition by its stored boundary range and
// not from the resume key.
func TestPartitionedBackfillDatetime(t *testing.T) {
	t.Parallel()
	var db, tc = blackboxTestSetup(t)
	var _, scheme = createPartitionScheme(t, db, "", "DATETIME", "RIGHT", `'2024-02-01', '2024-03-01'`)
	db.CreateTable(t, `<NAME>`, fmt.Sprintf(`(a INT NOT NULL, b DATETIME NOT NULL, PRIMARY KEY CLUSTERED (a, b)) ON [%s](b)`, scheme))
	db.Exec(t, `INSERT INTO <NAME> VALUES
		(1, '2024-03-05T10:00:00.003'), (2, '2024-01-15T08:30:00.007'), (3, '2024-02-29T23:59:59.997'),
		(4, '2024-01-31T23:59:59.997'), (5, '2024-02-01T00:00:00'), (6, '2024-03-01T00:00:00'),
		(7, '2024-01-01T00:00:00.003'), (8, '2024-02-10T12:00:00.333')`)
	discoverPartitionedTable(t, tc, "partition_aware_backfill")

	var order []int
	for _, doc := range backfilledDocsJSON(t, tc.Run("Backfill", -1)) {
		var row struct {
			A int `json:"a"`
		}
		require.NoError(t, json.Unmarshal(doc, &row))
		order = append(order, row.A)
	}
	// January, then February, then March onward, each in key order.
	require.Equal(t, []int{2, 4, 7, 3, 5, 8, 1, 6}, order)
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestPartitionedBackfillLegacyState checks that a backfill which is already in progress
// in key order, recognizable by a resume key without partition metadata, finishes in key
// order even though the table could be backfilled by partition. That happens when
// partition-aware backfills are enabled while a backfill is underway. A key-ordered
// cursor has passed rows in every partition, so switching it to partition order would
// lose rows.
func TestPartitionedBackfillLegacyState(t *testing.T) {
	t.Parallel()
	var rows = partitionedRows([4]int{0, 1000, 2000, 3000})
	var cursor = partitionedRow{A: 6, B: 2200}

	var db, tc = blackboxTestSetup(t)
	createPartitionedTable(t, db, "RIGHT", "1000, 2000, 3000", rows)
	discoverPartitionedTable(t, tc, "partition_aware_backfill")
	tc.Run("Initial Backfill", -1)

	rewindToBackfill(t, db, tc.Capture, cursor, nil)
	var want []partitionedRow
	for _, row := range keyOrder(rows) {
		if row.A > cursor.A || (row.A == cursor.A && row.B > cursor.B) {
			want = append(want, row)
		}
	}
	require.Equal(t, want, backfilledRows(t, tc.Run("Resumed Backfill", -1)))
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestPartitionedBackfillDisabledMidway checks that a partition-ordered backfill which
// can no longer continue by partition, here because no_partition_aware_backfill was set
// while it was in progress, restarts from the beginning in key order. Resuming its cursor in
// key order would skip the rows of later partitions which sort below it.
func TestPartitionedBackfillDisabledMidway(t *testing.T) {
	t.Parallel()
	var rows = partitionedRows([4]int{0, 1000, 2000, 3000})
	var cursor = partitionedRow{A: 6, B: 2200}

	var db, tc = blackboxTestSetup(t)
	var function = createPartitionedTable(t, db, "RIGHT", "1000, 2000, 3000", rows)
	discoverPartitionedTable(t, tc, "partition_aware_backfill")
	tc.Run("Initial Backfill", -1)

	rewindToBackfill(t, db, tc.Capture, cursor, partitionMetadata(function))
	require.NoError(t, tc.Capture.EditConfig("advanced.feature_flags", "no_partition_aware_backfill"))
	require.Equal(t, keyOrder(rows), backfilledRows(t, tc.Run("Resumed Backfill", -1)))
	cupaloy.SnapshotT(t, tc.Transcript.String())
}

// TestPartitionLayoutRepartitioned checks that a stored partition range is only located in
// a layout of the partition function, column and range direction it was recorded under.
// For any other layout the backfill starts over from the first partition.
func TestPartitionLayoutRepartitioned(t *testing.T) {
	var lo, hi = "2000", "3000"
	var layout = &partitionLayout{Function: "pf", Column: "b", Boundaries: []string{"1000", "2000", "3000"}, RangeRight: true}

	for _, tc := range []struct {
		name             string
		function, column string
		rangeRight       bool
		partition        int
		unchanged        bool
	}{
		{"Unchanged", "pf", "b", true, 3, true},
		{"OtherFunction", "pf_old", "b", true, 1, false},
		{"OtherColumn", "pf", "c", true, 1, false},
		{"OtherRangeDirection", "pf", "b", false, 1, false},
		{"NoIdentity", "", "", true, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stored = partitionRange{Function: tc.function, Column: tc.column, Lo: &lo, Hi: &hi, RangeRight: tc.rangeRight}
			n, unchanged, ok := layout.locatePartitionNumber(stored)
			require.True(t, ok)
			require.Equal(t, tc.partition, n)
			require.Equal(t, tc.unchanged, unchanged)
		})
	}
}

// TestPartitionLayoutNumbering checks partition numbers and ranges against the numbers
// SQL Server 2022 gives for functions over INT with boundaries (NULL, 1000, 2000) and
// (1000, 2000). With RANGE RIGHT, SQL Server puts 999 in partition 2 and 1000 in partition
// 3 when the NULL boundary is present, and in partitions 1 and 2 without it.
func TestPartitionLayoutNumbering(t *testing.T) {
	var str = func(s string) *string { return &s }

	for _, tc := range []struct {
		name        string
		leadingNull bool
		first, last int
	}{
		{"WithoutNullBoundary", false, 1, 3},
		{"WithNullBoundary", true, 2, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var layout = &partitionLayout{Boundaries: []string{"1000", "2000"}, RangeRight: true, LeadingNull: tc.leadingNull}
			require.Equal(t, tc.first, layout.First())
			require.Equal(t, tc.last, layout.Count())

			var ranges = []partitionRange{
				{Hi: str("1000"), RangeRight: true},
				{Lo: str("1000"), Hi: str("2000"), RangeRight: true},
				{Lo: str("2000"), RangeRight: true},
			}
			for i, expected := range ranges {
				var n = tc.first + i
				require.Equal(t, expected.String(), layout.Range(n).String(), "partition %d", n)

				located, unchanged, ok := layout.locatePartitionNumber(expected)
				require.True(t, ok)
				require.True(t, unchanged)
				require.Equal(t, n, located)
			}
		})
	}

	// A stored range keeps its meaning when a NULL boundary is added, only its number changes.
	var stored = partitionRange{Lo: str("1000"), Hi: str("2000"), RangeRight: true}
	var withNull = &partitionLayout{Boundaries: []string{"1000", "2000"}, RangeRight: true, LeadingNull: true}
	n, unchanged, ok := withNull.locatePartitionNumber(stored)
	require.True(t, ok)
	require.True(t, unchanged)
	require.Equal(t, 3, n)
}
