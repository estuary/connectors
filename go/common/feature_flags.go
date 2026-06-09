package common

import (
	"encoding/json"
	"sort"
	"strings"
)

// StateKeyRuntimeFeatureFlags is the reserved key, within a materialization's
// connector state, under which the runtime-resolved values of new-task-gated
// feature flags are recorded. It is owned by the boilerplate, not the
// connector's transactor, and is stripped from connector state before the
// transactor observes it (see StripFeatureFlagState).
const StateKeyRuntimeFeatureFlags = "_runtimeFeatureFlags"

// FlagDefault describes the default value of a feature flag: either a fixed
// boolean, or enabled only for tasks created after the flag was released.
// Users can always override the default by listing the flag (or its 'no_'
// negation) in their feature flags string.
type FlagDefault struct {
	value        bool
	newTasksOnly bool
}

// FlagEnabled is a flag that defaults to true for all tasks.
var FlagEnabled = FlagDefault{value: true}

// FlagDisabled is a flag that defaults to false for all tasks.
var FlagDisabled = FlagDefault{value: false}

// FlagEnabledForNewTasks is a flag that defaults to true for brand-new tasks,
// and false for tasks that were already running when the flag was released.
// The resolved value is recorded in connector state on the task's first Apply
// and pinned into the endpoint config; once pinned, the explicit config value
// applies instead of the default.
var FlagEnabledForNewTasks = FlagDefault{newTasksOnly: true}

// NewTasksOnly reports whether the flag's default depends on whether the task
// is brand new rather than being a fixed boolean.
func (d FlagDefault) NewTasksOnly() bool {
	return d.newTasksOnly
}

// Resolve returns the effective boolean default of the flag. isNewTask
// reports whether the task is brand new, i.e. has never been published
// before.
func (d FlagDefault) Resolve(isNewTask bool) bool {
	if d.newTasksOnly {
		return isNewTask
	}
	return d.value
}

// ResolveFlagDefaults resolves a map of flag defaults into concrete booleans
// purely from task newness, suitable for layering user-provided flags on top
// via ParseFeatureFlags. It ignores any recorded connector state; use
// ResolveFlags when state is available.
func ResolveFlagDefaults(defaults map[string]FlagDefault, isNewTask bool) map[string]bool {
	var resolved = make(map[string]bool, len(defaults))
	for k, v := range defaults {
		resolved[k] = v.Resolve(isNewTask)
	}
	return resolved
}

// ResolveFlags computes the effective feature flags for a task. Precedence for
// each flag is: an explicit entry in the raw config string, then a value
// recorded in connector state for a new-task-gated flag, then the newness
// resolved default.
func ResolveFlags(raw string, defaults map[string]FlagDefault, isNewTask bool, stateRecord map[string]bool) map[string]bool {
	resolved := ResolveFlagDefaults(defaults, isNewTask)
	// A recorded value overrides the newness default for gated flags whose
	// decision was already made and persisted on a prior Apply.
	for flag, d := range defaults {
		if !d.NewTasksOnly() {
			continue
		}
		if v, ok := stateRecord[flag]; ok {
			resolved[flag] = v
		}
	}
	// Explicit config entries always win.
	return ParseFeatureFlags(raw, resolved)
}

// PendingFeatureFlags reports the work needed to durably fix the values of
// new-task-gated flags that are not yet explicitly present in the raw config
// string. toPin is every such flag mapped to the value to write into the
// config (its recorded value if a prior Apply already decided it, otherwise
// the newness resolved default) and drives a configUpdate that is retried each
// session until the config catches up. recordNew is the subset that has no
// prior recorded value and must be persisted to connector state, so the
// decision is made exactly once regardless of the eventually-consistent
// configUpdate.
func PendingFeatureFlags(raw string, defaults map[string]FlagDefault, isNewTask bool, stateRecord map[string]bool) (toPin, recordNew map[string]bool) {
	mentioned := mentionedFlags(raw)
	toPin = map[string]bool{}
	recordNew = map[string]bool{}
	for flag, d := range defaults {
		if !d.NewTasksOnly() || mentioned[flag] {
			continue
		}
		if v, ok := stateRecord[flag]; ok {
			toPin[flag] = v
		} else {
			v := d.Resolve(isNewTask)
			toPin[flag] = v
			recordNew[flag] = v
		}
	}
	return toPin, recordNew
}

// PinnedFlagsString appends an explicit entry to the raw feature flags string
// for each flag in toPin (which must not already be mentioned in raw),
// recording its resolved value as a fixed per-task setting: true flags are
// added by name, false flags with the 'no_' prefix.
func PinnedFlagsString(raw string, toPin map[string]bool) string {
	var pins []string
	for flag, value := range toPin {
		if value {
			pins = append(pins, flag)
		} else {
			pins = append(pins, "no_"+flag)
		}
	}
	if len(pins) == 0 {
		return raw
	}
	sort.Strings(pins)

	if strings.TrimSpace(raw) == "" {
		return strings.Join(pins, ",")
	}
	return raw + "," + strings.Join(pins, ",")
}

// FeatureFlagStatePatch returns an RFC 7396 merge patch that records the given
// flag values under StateKeyRuntimeFeatureFlags, to be returned as a connector
// state update from an Apply RPC. Returns nil if record is empty.
func FeatureFlagStatePatch(record map[string]bool) json.RawMessage {
	if len(record) == 0 {
		return nil
	}
	patch, _ := json.Marshal(map[string]map[string]bool{StateKeyRuntimeFeatureFlags: record})
	return patch
}

// FeatureFlagStateRecord extracts the recorded new-task-gated flag values from
// a connector state document, or nil if none are present.
func FeatureFlagStateRecord(stateJson json.RawMessage) map[string]bool {
	if len(stateJson) == 0 {
		return nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(stateJson, &envelope); err != nil {
		return nil
	}
	raw, ok := envelope[StateKeyRuntimeFeatureFlags]
	if !ok {
		return nil
	}
	var record map[string]bool
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil
	}
	return record
}

// StripFeatureFlagState removes the boilerplate-owned StateKeyRuntimeFeatureFlags
// key from a connector state document, returning the state that the connector's
// transactor should observe. The input is returned unchanged when the key is
// absent, so connector-managed state is never reserialized needlessly.
func StripFeatureFlagState(stateJson json.RawMessage) (json.RawMessage, error) {
	if len(stateJson) == 0 {
		return stateJson, nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(stateJson, &envelope); err != nil {
		// Not a JSON object we can introspect; leave it untouched.
		return stateJson, nil
	}
	if _, ok := envelope[StateKeyRuntimeFeatureFlags]; !ok {
		return stateJson, nil
	}
	delete(envelope, StateKeyRuntimeFeatureFlags)
	return json.Marshal(envelope)
}

// mentionedFlags returns the set of flag names mentioned in a raw feature flags
// string, with any 'no_' prefix stripped.
func mentionedFlags(raw string) map[string]bool {
	mentioned := make(map[string]bool)
	for _, flagName := range strings.Split(raw, ",") {
		flagName = strings.TrimPrefix(strings.TrimSpace(flagName), "no_")
		if flagName != "" {
			mentioned[flagName] = true
		}
	}
	return mentioned
}

// ParseFeatureFlags parses a comma-separated list of flag names and combines that with a
// map describing default flag settings in the absence of any flags. A flag name can be
// prefixed with 'no_' to explicitly set it to a false value, in case the default is (or
// might soon become) true.
func ParseFeatureFlags(flags string, defaults map[string]bool) map[string]bool {
	var settings = make(map[string]bool)
	for k, v := range defaults {
		settings[k] = v
	}
	for _, flagName := range strings.Split(flags, ",") {
		flagName = strings.TrimSpace(flagName)
		var flagValue = true
		if strings.HasPrefix(flagName, "no_") {
			flagName = strings.TrimPrefix(flagName, "no_")
			flagValue = false
		}
		if flagName != "" {
			settings[flagName] = flagValue
		}
	}
	return settings
}
