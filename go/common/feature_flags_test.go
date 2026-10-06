package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var gatedFlag = FlagEnabledForTasksCreatedAfter("2026-06-10")

// futureFlag's cutoff is far enough out to stay in the future for the life of
// this test, so it exercises resolution of a not-yet-reached cutoff.
var futureFlag = FlagEnabledForTasksCreatedAfter("2099-01-01")

// mustCreatedAt is a test helper for dates known to be well-formed.
func mustCreatedAt(t *testing.T, date string) CreatedAt {
	t.Helper()
	createdAt, err := ParseCreatedAt(date)
	require.NoError(t, err)
	return createdAt
}

func TestFlagDefaultResolve(t *testing.T) {
	for _, tt := range []struct {
		name      string
		def       FlagDefault
		createdAt string
		want      bool
	}{
		{"fixed true for old task", FlagEnabled, "2020-01-01", true},
		{"fixed true for brand new task", FlagEnabled, "", true},
		{"fixed false for old task", FlagDisabled, "2020-01-01", false},
		{"fixed false for brand new task", FlagDisabled, "", false},
		{"cutoff: created well before", gatedFlag, "2026-01-01", false},
		{"cutoff: created day before", gatedFlag, "2026-06-09", false},
		{"cutoff: created on the cutoff date", gatedFlag, "2026-06-10", true},
		{"cutoff: created day after", gatedFlag, "2026-06-11", true},
		{"cutoff: created in a later year", gatedFlag, "2027-01-01", true},
		// An unstamped creation date means the task's creation isn't committed
		// yet, i.e. it is brand new, so it resolves as created today.
		{"cutoff: brand new task", gatedFlag, "", true},
		// A cutoff that hasn't been reached yet is off for every task, including
		// a brand-new one — which is what keeps a brand-new task's resolution
		// stable across the publication that stamps its date.
		{"future cutoff: brand new task", futureFlag, "", false},
		{"future cutoff: created today", futureFlag, "2026-08-01", false},
		{"future cutoff: created on the cutoff date", futureFlag, "2099-01-01", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.def.Resolve(mustCreatedAt(t, tt.createdAt)))
		})
	}
}

func TestParseCreatedAt(t *testing.T) {
	t.Run("a well-formed date is carried through", func(t *testing.T) {
		createdAt, err := ParseCreatedAt("2026-06-10")
		require.NoError(t, err)
		require.False(t, createdAt.IsBrandNew())
		require.Equal(t, "2026-06-10", createdAt.String())
	})

	t.Run("an empty date is a brand new task, not an error", func(t *testing.T) {
		createdAt, err := ParseCreatedAt("")
		require.NoError(t, err)
		require.True(t, createdAt.IsBrandNew())
		require.Equal(t, CreatedAt{}, createdAt)
	})

	// A date we cannot place relative to a cutoff is a hard error: resolving
	// flags around it either way would silently give the task the behavior of
	// the wrong era.
	t.Run("a malformed date is an error", func(t *testing.T) {
		for _, date := range []string{
			"not-a-date",
			"2026-6-10",            // not zero-padded
			"10-06-2026",           // wrong field order
			"2026-06-10T00:00:00Z", // a timestamp: specs record only a date
			"2026-13-01",           // no such month
		} {
			_, err := ParseCreatedAt(date)
			require.ErrorContains(t, err, date)
			require.ErrorContains(t, err, "task creation date")
		}
	})
}

func TestFlagEnabledForTasksCreatedAfterRejectsBadDates(t *testing.T) {
	for _, date := range []string{
		"",
		"2026-6-10",            // not zero-padded
		"10-06-2026",           // wrong field order
		"2026-06-10T00:00:00Z", // a timestamp: specs record only a date
		"tomorrow",
	} {
		require.Panics(t, func() { FlagEnabledForTasksCreatedAfter(date) }, date)
	}
}

var resolveDefaults = map[string]FlagDefault{
	"always_on":  FlagEnabled,
	"always_off": FlagDisabled,
	"gated":      gatedFlag,
}

func TestPendingFlagPins(t *testing.T) {
	defaults := map[string]FlagDefault{
		"always_on":   FlagEnabled,
		"always_off":  FlagDisabled,
		"gated":       gatedFlag,
		"later_gated": FlagEnabledForTasksCreatedAfter("2026-08-01"),
	}

	t.Run("only cutoff-gated flags are pinned, at their resolved value", func(t *testing.T) {
		// Fixed defaults are left out: pinning them would freeze a value we may
		// want to change fleet-wide later. Each cutoff applies independently.
		require.Equal(t, map[string]bool{"gated": false, "later_gated": false},
			PendingFlagPins("", defaults, mustCreatedAt(t, "2026-01-01")))
		require.Equal(t, map[string]bool{"gated": true, "later_gated": false},
			PendingFlagPins("", defaults, mustCreatedAt(t, "2026-07-01")))
		require.Equal(t, map[string]bool{"gated": true, "later_gated": true},
			PendingFlagPins("", defaults, mustCreatedAt(t, "2026-09-01")))
	})

	t.Run("brand new task pins the enabled form", func(t *testing.T) {
		require.Equal(t, map[string]bool{"gated": true, "later_gated": true},
			PendingFlagPins("", defaults, mustCreatedAt(t, "")))
	})

	t.Run("flags already mentioned in the config are left alone", func(t *testing.T) {
		require.Equal(t, map[string]bool{"later_gated": false},
			PendingFlagPins("gated", defaults, mustCreatedAt(t, "2026-07-01")))
		require.Equal(t, map[string]bool{"later_gated": false},
			PendingFlagPins("no_gated", defaults, mustCreatedAt(t, "2026-07-01")))
		require.Empty(t, PendingFlagPins("gated,no_later_gated", defaults, mustCreatedAt(t, "2026-07-01")))
	})

	t.Run("pinned value always matches what Resolve produces", func(t *testing.T) {
		for _, createdAt := range []string{"", "2026-01-01", "2026-06-09", "2026-06-10", "2026-07-01", "2026-09-01"} {
			pins := PendingFlagPins("", defaults, mustCreatedAt(t, createdAt))
			resolved := ResolveFlags("", defaults, mustCreatedAt(t, createdAt))
			for flag, pinned := range pins {
				require.Equal(t, resolved[flag], pinned, "flag %s at %q", flag, createdAt)
				// And re-resolving with the pin applied is a no-op.
				withPin := ResolveFlags(PinnedFlagsString("", pins), defaults, mustCreatedAt(t, createdAt))
				require.Equal(t, resolved[flag], withPin[flag], "flag %s at %q after pinning", flag, createdAt)
			}
		}
	})
}

func TestPinnedFlagsString(t *testing.T) {
	require.Equal(t, "gated_one,no_gated_two",
		PinnedFlagsString("", map[string]bool{"gated_one": true, "gated_two": false}))
	require.Equal(t, "existing,gated,no_other",
		PinnedFlagsString("existing", map[string]bool{"gated": true, "other": false}))
	require.Equal(t, "existing", PinnedFlagsString("existing", nil))
}

func TestResolveFlags(t *testing.T) {
	for _, tt := range []struct {
		name      string
		raw       string
		createdAt string
		wantGated bool
	}{
		{"task created before the cutoff", "", "2026-01-01", false},
		{"task created after the cutoff", "", "2026-07-01", true},
		{"brand new task", "", "", true},
		{"user disables for a new task", "no_gated", "2026-07-01", false},
		{"user enables for an old task", "gated", "2026-01-01", true},
		{"unrelated user flags don't disturb the cutoff", "other_flag", "2026-01-01", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flags := ResolveFlags(tt.raw, resolveDefaults, mustCreatedAt(t, tt.createdAt))
			require.Equal(t, tt.wantGated, flags["gated"])
			require.True(t, flags["always_on"])
			require.False(t, flags["always_off"])
		})
	}
}
