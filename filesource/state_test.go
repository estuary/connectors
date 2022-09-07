package filesource

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNoFiles(t *testing.T) {
	var s State

	s.startSweep(*ts(10))
	require.NoError(t, s.Validate())
	verify(t, State{MaxBound: ts(10)}, s)

	// File is too new.
	var skip, reason = s.shouldSkip("aaa", *ts(11))
	require.True(t, skip)
	require.Equal(t, "!MaxBound.After(modTime)", reason)

	s.finishSweep(true)
	verify(t, State{}, s)
}

func TestSuccessfulSweeps(t *testing.T) {
	var s1 State

	s1.startSweep(*ts(10))
	require.NoError(t, s1.Validate())

	// Process aaa @8.
	var skip, _ = s1.shouldSkip("aaa", *ts(8))
	require.False(t, skip)

	require.True(t, s1.startPath("aaa", *ts(8)))
	require.Len(t, s1.nextLines(lines(5)), 5)

	verify(t, State{
		MaxBound: ts(10),
		MaxMod:   ts(8),
		Path:     "aaa",
		Records:  5,
	}, s1)

	s1.finishPath()
	verify(t, State{
		MaxBound: ts(10),
		MaxMod:   ts(8),
		Path:     "aaa",
		Complete: true,
	}, s1)

	// Process bbb @6.
	require.True(t, s1.startPath("bbb", *ts(7)))
	require.Len(t, s1.nextLines(lines(5)), 5)
	require.Len(t, s1.nextLines(lines(7)), 7)

	verify(t, State{
		MaxBound: ts(10),
		MaxMod:   ts(8),
		Path:     "bbb",
		Records:  12,
	}, s1)
	s1.finishPath()

	var s2 = s1
	s1.finishSweep(false)
	s2.finishSweep(true)

	// Non-monotonic: next sweep starts @8.
	verify(t, State{
		MinBound: *ts(8),
	}, s1)
	// Monotonic version: mark for continuation from "bbb".
	verify(t, State{
		MaxMod:   ts(8),
		Path:     "bbb",
		Complete: true,
	}, s2)

	// Start next sweeps (+/- monotonic)
	s1.startSweep(*ts(20))
	s2.startSweep(*ts(20))

	verify(t, State{
		MinBound: *ts(8),
		MaxBound: ts(20),
	}, s1)
	verify(t, State{
		MaxBound: ts(20),
		MaxMod:   ts(8),
		Path:     "bbb",
		Complete: true,
	}, s2)

	// Non-monotonic: This time, aaa @8 is too old to be processed.
	skip, reason := s1.shouldSkip("aaa", *ts(8))
	require.True(t, skip)
	require.Equal(t, reason, "!modTime.After(MinBound)")

	// bbb @9 has been modified, and should be.
	// We detect this even though it's before our previous sweep start,
	// because we increment MinBound using the previous MaxMod.
	skip, _ = s1.shouldSkip("bbb", *ts(9))
	require.False(t, skip)

	// Monotonic version: bbb is skipped because its key is too low.
	skip, reason = s2.shouldSkip("bbb", *ts(9))
	require.True(t, skip)
	require.Equal(t, reason, "state.Path == obj.Path && Complete")

	// ccc is processed by both.
	skip, _ = s1.shouldSkip("ccc", *ts(11))
	require.False(t, skip)
	skip, _ = s2.shouldSkip("ccc", *ts(11))
	require.False(t, skip)

	// Turns out it's empty. That's okay.
	require.True(t, s1.startPath("ccc", *ts(11)))
	require.True(t, s2.startPath("ccc", *ts(11)))
	s1.finishPath()
	s2.finishPath()

	verify(t, State{
		MinBound: *ts(8),
		MaxMod:   ts(11),
		MaxBound: ts(20),
		Path:     "ccc",
		Complete: true,
	}, s1)
	verify(t, State{
		MaxBound: ts(20),
		MaxMod:   ts(11),
		Path:     "ccc",
		Complete: true,
	}, s2)

	// Finish the sweep.
	s1.finishSweep(false)
	s2.finishSweep(true)

	verify(t, State{MinBound: *ts(11)}, s1)
	verify(t, State{
		MaxMod:   ts(11),
		Path:     "ccc",
		Complete: true,
	}, s2)

	// Next sweep processes no files.
	s1.startSweep(*ts(30))
	s2.startSweep(*ts(30))

	// At first we think ddd @25 is in bounds.
	skip, _ = s1.shouldSkip("ddd", *ts(25))
	require.False(t, skip)

	// But after opening it, the modTime is restated.
	require.False(t, s1.startPath("ddd", *ts(35)))

	s1.finishSweep(false)
	s2.finishSweep(true)

	// Both states are unchanged.
	verify(t, State{MinBound: *ts(11)}, s1)
	verify(t, State{
		MaxMod:   ts(11),
		Path:     "ccc",
		Complete: true,
	}, s2)

	// Next sweep fails part-way through a file.
	s1.startSweep(*ts(40))
	s2.startSweep(*ts(40))

	require.True(t, s1.startPath("ddd", *ts(35)))
	require.True(t, s2.startPath("ddd", *ts(35)))
	require.Len(t, s1.nextLines(lines(5)), 5)
	require.Len(t, s2.nextLines(lines(5)), 5)

	// CRASH!

	s1.startSweep(*ts(50))
	s2.startSweep(*ts(50))

	// Both states are configured for recovery.
	verify(t, State{
		MinBound: *ts(11),
		MaxBound: ts(40),
		MaxMod:   ts(35),
		Path:     "ddd",
		Records:  5,
	}, s1)
	verify(t, State{
		MaxBound: ts(40),
		MaxMod:   ts(35),
		Path:     "ddd",
		Records:  5,
	}, s2)

	// It skips a file that's already been walked.
	skip, reason = s1.shouldSkip("aaa", *ts(20))
	require.True(t, skip)
	require.Equal(t, reason, "state.Path > obj.Path")

	// It skips a modTime that's < 50, but > the recovered MaxBound.
	skip, reason = s1.shouldSkip("zzz", *ts(45))
	require.True(t, skip)
	require.Equal(t, reason, "!MaxBound.After(modTime)")

	// s1 recovers by re-reading ddd @ 35.
	require.True(t, s1.startPath("ddd", *ts(35)))
	require.Len(t, s1.nextLines(lines(3)), 0) // Consumes 5 lines.
	require.Len(t, s1.nextLines(lines(6)), 4)
	s1.finishPath()

	// s2 sees that ddd was modified and is now out-of-bounds.
	require.False(t, s2.startPath("ddd", *ts(45)))

	// It moves on to eee @ 37.
	require.True(t, s2.startPath("eee", *ts(37)))
	require.Len(t, s2.nextLines(lines(4)), 4) // ddd's skip is reset.
	s2.finishPath()

	s1.finishSweep(false)
	s2.finishSweep(true)

	verify(t, State{MinBound: *ts(35)}, s1)
	verify(t, State{
		MaxMod:   ts(37),
		Path:     "eee",
		Complete: true,
	}, s2)
}

// TestMonotonicChanges shows that sweeps initially run in monotonic mode and later run in
// non-monotonic mode will not re-process previously processed files.
func TestMonotonicChanges(t *testing.T) {
	var s State

	s.startSweep(*ts(10))
	require.NoError(t, s.Validate())

	// Process some lexicographic files. The path will be stored in the state if running as
	// monotonic.
	require.True(t, s.startPath("aaa", *ts(5)))
	require.True(t, s.startPath("bbb", *ts(6)))
	s.finishPath()
	verify(t, State{
		MaxBound: ts(10),
		MaxMod:   ts(6),
		Path:     "bbb",
		Complete: true,
	}, s)

	// Running in monotonic mode to begin with
	s.finishSweep(true)
	verify(t, State{
		MinBound: time.Time{}, // Min bound is never incremented.
		MaxBound: nil,
		MaxMod:   ts(6),
		Path:     "bbb",
		Complete: true,
	}, s)

	// Process a couple more files.
	s.startSweep(*ts(20))
	require.NoError(t, s.Validate())
	require.True(t, s.startPath("ccc", *ts(12)))
	require.True(t, s.startPath("ddd", *ts(13)))
	s.finishPath()
	verify(t, State{
		MaxBound: ts(20),
		MaxMod:   ts(13),
		Path:     "ddd",
		Complete: true,
	}, s)

	// Still in monotonic mode.
	s.finishSweep(true)
	verify(t, State{
		MinBound: time.Time{}, // Min bound is never incremented.
		MaxBound: nil,
		MaxMod:   ts(13),
		Path:     "ddd",
		Complete: true,
	}, s)

	// Next sweep will finish NOT in monotonic mode.
	s.startSweep(*ts(30))

	// Doesn't re-process files, because the state records the "highest" seen path.
	skip, reason := s.shouldSkip("bbb", *ts(5))
	require.True(t, skip)
	require.Equal(t, "state.Path > obj.Path", reason)
	skip, reason = s.shouldSkip("ccc", *ts(13))
	require.True(t, skip)
	require.Equal(t, "state.Path > obj.Path", reason)

	require.True(t, s.startPath("eee", *ts(23)))
	s.finishPath()
	verify(t, State{
		MaxBound: ts(30),
		MaxMod:   ts(23),
		Path:     "eee",
		Complete: true,
	}, s)

	// Finish this sweep in non-monotonic mode.
	s.finishSweep(false)
	verify(t, State{
		MinBound: *ts(23), // Min bound is now updated
		MaxBound: nil,
		MaxMod:   nil,
		Path:     "",
		Complete: false,
	}, s)

	s.startSweep(*ts(40))

	// Still does not re-process files, but now its based on the MinBound.
	skip, reason = s.shouldSkip("aaa", *ts(5))
	require.True(t, skip)
	require.Equal(t, "!modTime.After(MinBound)", reason)
	skip, reason = s.shouldSkip("ccc", *ts(12))
	require.True(t, skip)
	require.Equal(t, "!modTime.After(MinBound)", reason)
	skip, reason = s.shouldSkip("eee", *ts(23))
	require.True(t, skip)
	require.Equal(t, "!modTime.After(MinBound)", reason)

	// Will process newer files.
	skip, _ = s.shouldSkip("fff", *ts(33))
	require.False(t, skip)

	require.True(t, s.startPath("fff", *ts(33)))
	s.finishPath()

	// Switch back to monotonic
	s.finishSweep(true)

	// Continues to not re-process files.
	skip, reason = s.shouldSkip("aaa", *ts(5))
	require.True(t, skip)
	require.Equal(t, "state.Path > obj.Path", reason)
	skip, reason = s.shouldSkip("ccc", *ts(12))
	require.True(t, skip)
	require.Equal(t, "state.Path > obj.Path", reason)
	skip, reason = s.shouldSkip("fff", *ts(33))
	require.True(t, skip)
	require.Equal(t, "state.Path == obj.Path && Complete", reason)
}

func ts(i int64) *time.Time {
	var out = time.Unix(i, 0)
	return &out
}

func lines(i int) []json.RawMessage {
	return make([]json.RawMessage, i)
}

func verify(t *testing.T, expect, actual State) {
	require.Equal(t, expect, actual)
	require.NoError(t, actual.Validate())
}
