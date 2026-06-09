package common

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlagDefaultResolve(t *testing.T) {
	for _, tt := range []struct {
		name      string
		def       FlagDefault
		isNewTask bool
		want      bool
	}{
		{"fixed true for existing task", FlagEnabled, false, true},
		{"fixed true for new task", FlagEnabled, true, true},
		{"fixed false for existing task", FlagDisabled, false, false},
		{"fixed false for new task", FlagDisabled, true, false},
		{"new-task gate for existing task", FlagEnabledForNewTasks, false, false},
		{"new-task gate for new task", FlagEnabledForNewTasks, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.def.Resolve(tt.isNewTask))
		})
	}
}

var resolveDefaults = map[string]FlagDefault{
	"always_on":  FlagEnabled,
	"always_off": FlagDisabled,
	"gated":      FlagEnabledForNewTasks,
}

func TestResolveFlags(t *testing.T) {
	for _, tt := range []struct {
		name        string
		raw         string
		isNewTask   bool
		stateRecord map[string]bool
		wantGated   bool
	}{
		{"existing task, no state", "", false, nil, false},
		{"new task, no state", "", true, nil, true},
		{"existing task, state says enabled", "", false, map[string]bool{"gated": true}, true},
		{"new task, state says disabled", "", true, map[string]bool{"gated": false}, false},
		{"config pin beats state", "no_gated", false, map[string]bool{"gated": true}, false},
		{"config pin beats newness", "gated", false, nil, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flags := ResolveFlags(tt.raw, resolveDefaults, tt.isNewTask, tt.stateRecord)
			require.Equal(t, tt.wantGated, flags["gated"])
			require.True(t, flags["always_on"])
			require.False(t, flags["always_off"])
		})
	}
}

func TestPendingFeatureFlags(t *testing.T) {
	defaults := map[string]FlagDefault{
		"always_on": FlagEnabled,
		"gated_one": FlagEnabledForNewTasks,
		"gated_two": FlagEnabledForNewTasks,
	}

	t.Run("new task with no state decides and records both", func(t *testing.T) {
		toPin, recordNew := PendingFeatureFlags("", defaults, true, nil)
		require.Equal(t, map[string]bool{"gated_one": true, "gated_two": true}, toPin)
		require.Equal(t, map[string]bool{"gated_one": true, "gated_two": true}, recordNew)
	})

	t.Run("existing task with no state pins disabled and records", func(t *testing.T) {
		toPin, recordNew := PendingFeatureFlags("", defaults, false, nil)
		require.Equal(t, map[string]bool{"gated_one": false, "gated_two": false}, toPin)
		require.Equal(t, map[string]bool{"gated_one": false, "gated_two": false}, recordNew)
	})

	t.Run("recorded value is pinned but not re-recorded", func(t *testing.T) {
		// State already decided gated_one; only gated_two is newly decided.
		toPin, recordNew := PendingFeatureFlags("", defaults, true, map[string]bool{"gated_one": true})
		require.Equal(t, map[string]bool{"gated_one": true, "gated_two": true}, toPin)
		require.Equal(t, map[string]bool{"gated_two": true}, recordNew)
	})

	t.Run("flag already pinned in config is skipped", func(t *testing.T) {
		toPin, recordNew := PendingFeatureFlags("gated_one,no_gated_two", defaults, true, nil)
		require.Empty(t, toPin)
		require.Empty(t, recordNew)
	})
}

func TestPinnedFlagsString(t *testing.T) {
	require.Equal(t, "gated_one,no_gated_two",
		PinnedFlagsString("", map[string]bool{"gated_one": true, "gated_two": false}))
	require.Equal(t, "existing,gated,no_other",
		PinnedFlagsString("existing", map[string]bool{"gated": true, "other": false}))
	require.Equal(t, "existing", PinnedFlagsString("existing", nil))
}

func TestFeatureFlagStateRoundTrip(t *testing.T) {
	// An empty record produces no patch.
	require.Nil(t, FeatureFlagStatePatch(nil))
	require.Nil(t, FeatureFlagStatePatch(map[string]bool{}))

	patch := FeatureFlagStatePatch(map[string]bool{"jsonb": true})
	require.JSONEq(t, `{"_runtimeFeatureFlags":{"jsonb":true}}`, string(patch))

	// The patch reads back via FeatureFlagStateRecord.
	require.Equal(t, map[string]bool{"jsonb": true}, FeatureFlagStateRecord(patch))

	// Absent / empty / unrelated states yield no record.
	require.Nil(t, FeatureFlagStateRecord(nil))
	require.Nil(t, FeatureFlagStateRecord(json.RawMessage(`{}`)))
	require.Nil(t, FeatureFlagStateRecord(json.RawMessage(`{"checkpoint":"abc"}`)))
}

func TestStripFeatureFlagState(t *testing.T) {
	// The reserved key is removed, transactor-owned keys are preserved.
	stripped, err := StripFeatureFlagState(json.RawMessage(`{"_runtimeFeatureFlags":{"jsonb":true},"checkpoint":"abc"}`))
	require.NoError(t, err)
	require.JSONEq(t, `{"checkpoint":"abc"}`, string(stripped))

	// Absent key: input returned unchanged (byte-for-byte).
	original := json.RawMessage(`{"checkpoint":"abc"}`)
	stripped, err = StripFeatureFlagState(original)
	require.NoError(t, err)
	require.Equal(t, string(original), string(stripped))

	// Empty/nil state is passed through.
	stripped, err = StripFeatureFlagState(nil)
	require.NoError(t, err)
	require.Empty(t, stripped)
}
