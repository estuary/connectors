package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeUnistr(t *testing.T) {
	var cases = [][]string{
		{"\\2764\\FE0F", "❤️"},
		{"\\D83D\\DD25\\FE0F", "🔥️"},
		{"\\D83D\\DD25\\FE0F\\2764\\FE0F", "🔥️❤️"},
		{"\\005C \\\\ \\2764\\FE0F \\\\", "\\ \\ ❤️ \\"},
	}

	for _, c := range cases {
		out, err := decodeUnistr(c[0])
		require.NoError(t, err)
		require.Equal(t, c[1], out)
	}
}

func TestValidateLogFileCoverage(t *testing.T) {
	// rf is a terse constructor for a redo log file. All test files share a
	// dictionary state ("NO"/"NO") and a non-zero size, which are irrelevant to
	// coverage validation.
	var rf = func(thread, sequence int, firstChange, nextChange SCN, resetlogsID int) redoFile {
		return redoFile{
			Status: "ARCHIVED", Name: "log", Thread: thread, Sequence: sequence,
			FirstChange: firstChange, NextChange: nextChange, ResetlogsID: resetlogsID, Bytes: 1,
		}
	}

	var cases = []struct {
		name        string
		files       []redoFile
		startSCN    SCN
		endSCN      SCN
		errContains string // empty means no error expected
	}{
		{
			name:     "contiguous chain is valid",
			files:    []redoFile{rf(1, 1, 100, 200, 1), rf(1, 2, 200, 300, 1)},
			startSCN: 100, endSCN: 300,
		},
		{
			// Server restart leaves an SCN gap between two
			// consecutively-numbered archive logs of the same incarnation.
			name:     "restart gap across consecutive sequences is tolerated",
			files:    []redoFile{rf(1, 1, 100, 200, 1), rf(1, 2, 250, 300, 1)},
			startSCN: 100, endSCN: 300,
		},
		{
			name:        "missing log (non-consecutive sequence) fails",
			files:       []redoFile{rf(1, 1, 100, 200, 1), rf(1, 3, 250, 300, 1)},
			startSCN:    100,
			endSCN:      300,
			errContains: "redo log SCN gap in thread 1",
		},
		{
			// Same consecutive-sequence forward gap, but a RESETLOGS forked the
			// timeline between the two files: real data loss, must fail.
			name:        "incarnation change (RESETLOGS) is not tolerated",
			files:       []redoFile{rf(1, 1, 100, 200, 1), rf(1, 2, 250, 300, 2)},
			startSCN:    100,
			endSCN:      300,
			errContains: "redo log SCN gap in thread 1",
		},
		{
			// A backward discontinuity (overlap) is not the restart signature and
			// must still fail even with consecutive sequences.
			name:        "backward overlap is not tolerated",
			files:       []redoFile{rf(1, 1, 100, 200, 1), rf(1, 2, 150, 300, 1)},
			startSCN:    100,
			endSCN:      300,
			errContains: "redo log SCN gap in thread 1",
		},
		{
			name:        "gap at start of range fails",
			files:       []redoFile{rf(1, 1, 150, 300, 1)},
			startSCN:    100,
			endSCN:      300,
			errContains: "gap at start",
		},
		{
			name:        "gap at end of range fails",
			files:       []redoFile{rf(1, 1, 100, 200, 1), rf(1, 2, 200, 300, 1)},
			startSCN:    100,
			endSCN:      400,
			errContains: "gap at end",
		},
		{
			name:        "empty file set fails",
			files:       nil,
			startSCN:    100,
			endSCN:      300,
			errContains: "no redo log files available",
		},
		{
			name:        "file following a CURRENT log fails",
			files:       []redoFile{rf(1, 1, 100, 0, 1), rf(1, 2, 200, 300, 1)},
			startSCN:    100,
			endSCN:      300,
			errContains: "has no NEXT_CHANGE#",
		},
		{
			// RAC: each thread is validated independently. A benign restart gap in
			// thread 1 is tolerated while thread 2 stays contiguous.
			name:     "per-thread validation tolerates a benign gap in one thread",
			files:    []redoFile{rf(1, 1, 100, 200, 1), rf(1, 2, 250, 300, 1), rf(2, 1, 100, 300, 1)},
			startSCN: 100, endSCN: 300,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var err = validateLogFileCoverage(c.files, c.startSCN, c.endSCN)
			if c.errContains == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, c.errContains)
			}
		})
	}
}
