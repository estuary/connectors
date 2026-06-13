package boilerplate

import (
	"encoding/json"
	"testing"

	"github.com/estuary/connectors/go/common"
	"github.com/stretchr/testify/require"
)

type flagsTestConfig struct {
	raw string
}

func (c flagsTestConfig) Validate() error          { return nil }
func (c flagsTestConfig) DefaultNamespace() string { return "" }
func (c flagsTestConfig) FeatureFlags() (string, map[string]common.FlagDefault) {
	return c.raw, map[string]common.FlagDefault{
		"fixed_on":  common.FlagEnabled,
		"fixed_off": common.FlagDisabled,
		"gated":     common.FlagEnabledForNewTasks,
	}
}

func TestResolveFlags(t *testing.T) {
	statePatch := func(gated bool) json.RawMessage {
		return common.FeatureFlagStatePatch(map[string]bool{"gated": gated})
	}

	for _, tt := range []struct {
		name      string
		cfg       flagsTestConfig
		isNewTask bool
		stateJson json.RawMessage
		wantGated bool
	}{
		{
			name:      "existing task with no state resolves gated flag off",
			isNewTask: false,
			wantGated: false,
		},
		{
			name:      "new task with no state resolves gated flag on",
			isNewTask: true,
			wantGated: true,
		},
		{
			// The race fix: a restart re-classifies as existing (isNewTask
			// false), but the recorded state keeps the flag enabled.
			name:      "state record overrides re-classified newness",
			isNewTask: false,
			stateJson: statePatch(true),
			wantGated: true,
		},
		{
			name:      "config pin wins over state record",
			cfg:       flagsTestConfig{raw: "no_gated"},
			isNewTask: true,
			stateJson: statePatch(true),
			wantGated: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			flags := resolveFlags(tt.cfg, tt.isNewTask, tt.stateJson)
			require.Equal(t, tt.wantGated, flags["gated"])
			require.True(t, flags["fixed_on"])
			require.False(t, flags["fixed_off"])
		})
	}

	t.Run("ParseFlags treats the task as pre-existing with no state", func(t *testing.T) {
		flags := ParseFlags(flagsTestConfig{})
		require.False(t, flags["gated"])
	})
}
