package main

import (
	"regexp"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/estuary/connectors/go/capture/blackbox"
	"github.com/stretchr/testify/require"
)

// chunkedTestSetup is blackboxTestSetup but using the chunk-size-1 capture config, so that
// multi-row tables exercise chunked backfills and the resume-key path.
func chunkedTestSetup(t testing.TB) (*cockroachdbTestDatabase, *blackbox.TranscriptCapture) {
	t.Helper()
	var db, _ = blackboxTestSetup(t)

	tc, err := blackbox.NewWithTranscript("testdata/flow_chunk1.yaml")
	require.NoError(t, err)
	tc.Capture.Logger = t.Log
	tc.Capture.DiscoveryFilter = regexp.MustCompile(db.vars["<ID>"])
	tc.DocumentSanitizers = documentSanitizers
	tc.CheckpointSanitizers = checkpointSanitizers
	db.transcript = tc.Transcript
	return db, tc
}

// TestConcurrentChangesDuringBackfill verifies that rows modified or deleted while their chunk
// hasn't been scanned yet are never superseded by older snapshot state. The capture is stopped
// mid-backfill, rows beyond the scan cursor are mutated, and the capture is resumed: because
// chunks read at the latest resolved timestamp, the remaining chunks must observe the mutations
// rather than resurrecting the pre-mutation row states after their change events.
//
// Unlike most capture tests this one asserts on event ordering rather than a cupaloy snapshot:
// how far the time-bounded "Partial Backfill" session progresses varies with fence timing, so
// the exact transcript is not deterministic.
func TestConcurrentChangesDuringBackfill(t *testing.T) {
	var db, tc = chunkedTestSetup(t)

	db.CreateTable(t, "<NAME>", "(id INTEGER PRIMARY KEY, v TEXT)")
	tc.Discover("Discover")

	db.Exec(t, "INSERT INTO <NAME> VALUES (1,'a1'), (2,'b1'), (3,'c1'), (4,'d1'), (5,'e1')")
	// Stop after a few Flow transactions: with chunk size 1 the backfill of 5 rows can't be done.
	tc.Run("Partial Backfill", 3)

	// Mutate rows the backfill hasn't scanned yet.
	db.Exec(t, "UPDATE <NAME> SET v = 'e2' WHERE id = 5")
	db.Exec(t, "DELETE FROM <NAME> WHERE id = 4")

	tc.Run("Resume Backfill", runSessions)

	var transcript = tc.Transcript.String()
	require.Contains(t, transcript, `"v": "e2"`, "updated row 5 missing from capture")
	require.Contains(t, transcript, `"op": "d"`, "delete event for row 4 missing from capture")
	if i := strings.LastIndex(transcript, `"v": "e1"`); i >= 0 && i > strings.Index(transcript, `"v": "e2"`) {
		t.Errorf("stale snapshot of row 5 published after its streamed update")
	}
	if i := strings.LastIndex(transcript, `"v": "d1"`); i >= 0 && i > strings.Index(transcript, `"op": "d"`) {
		t.Errorf("deleted row 4 resurrected by a later backfill chunk")
	}
}

// TestExoticKeyResume exercises the chunked-backfill resume path (chunk size 1) over a composite
// primary key of types whose pgx-decoded Go values don't round-trip textually: the `::STRING` key
// projection must yield resume predicates CockroachDB can parse back. Also covers a composite
// delete on the same key.
func TestExoticKeyResume(t *testing.T) {
	var db, tc = chunkedTestSetup(t)

	db.CreateTable(t, "<NAME>", `(u UUID, ts TIMESTAMPTZ, plain TIMESTAMP, d DATE, by BYTES, bl BOOL, ip INET, payload TEXT,
	  PRIMARY KEY (u, ts, plain, d, by, bl, ip))`)
	tc.Discover("Discover")
	db.Exec(t, `INSERT INTO <NAME> VALUES
	 ('11111111-1111-1111-1111-111111111111', '2026-01-01 00:00:00+00', '2026-01-01 00:00:00', '2026-01-01', b'aa', true, '10.0.0.1', 'row1'),
	 ('22222222-2222-2222-2222-222222222222', '2026-02-02 12:00:00.5+00', '2026-02-02 12:00:00.5', '2026-02-02', b'bb', false, '10.0.0.2', 'row2'),
	 ('33333333-3333-3333-3333-333333333333', '2026-03-03 03:03:03.123456+00', '2026-03-03 03:03:03.123456', '2026-03-03', b'cc', true, '::1', 'row3'),
	 ('44444444-4444-4444-4444-444444444444', '2026-04-04 04:04:04+00', '2026-04-04 04:04:04', '2026-04-04', b'dd', false, '192.168.0.1/24', 'row4')`)
	tc.Run("Backfill Chunked", runSessions)
	db.Exec(t, "DELETE FROM <NAME> WHERE u = '22222222-2222-2222-2222-222222222222'")
	tc.Run("Composite Delete", runSessions)
	cupaloy.SnapshotT(t, tc.Transcript.String())

	for _, payload := range []string{"row1", "row2", "row3", "row4"} {
		require.Contains(t, tc.Transcript.String(), `"payload": "`+payload+`"`, "backfill resume lost a row")
	}
}

// TestIntegerKeyResume proves that integer keys still resume correctly now that key columns are
// projected as `::STRING`: the text bind argument must coerce back against the INT8 column.
func TestIntegerKeyResume(t *testing.T) {
	var db, tc = chunkedTestSetup(t)
	db.CreateTable(t, "<NAME>", "(id INTEGER PRIMARY KEY, v TEXT)")
	tc.Discover("Discover")
	db.Exec(t, "INSERT INTO <NAME> VALUES (-5,'neg'), (0,'zero'), (7,'seven'), (9223372036854775807,'max')")
	tc.Run("Backfill Chunked", runSessions)
	cupaloy.SnapshotT(t, tc.Transcript.String())

	for _, v := range []string{"neg", "zero", "seven", "max"} {
		require.Contains(t, tc.Transcript.String(), `"v": "`+v+`"`, "integer-key resume lost a row")
	}
}
