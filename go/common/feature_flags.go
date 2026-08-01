package common

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// CreatedAtLayout is the format of the task creation date carried by built
// capture and materialization specs: a UTC date in RFC 3339 "full-date" form.
const CreatedAtLayout = "2006-01-02"

// CreatedAt is a task's validated creation date. Parse one with ParseCreatedAt
// before resolving feature flags, so that a date the connector cannot reason
// about fails the RPC rather than silently resolving flags from a value nobody
// understood.
//
// The zero value means the task is brand new: the control plane stamps no date
// during a task's first build, because its creation is not yet committed.
type CreatedAt struct {
	date string
}

// ParseCreatedAt validates the creation date of a task spec, which is a UTC date
// in CreatedAtLayout form, or empty for a brand-new task whose creation is not
// yet committed. Any other value is an error: it is a date the connector cannot
// place relative to a cutoff, and guessing either way would silently give the
// task the wrong behavior.
func ParseCreatedAt(createdAt string) (CreatedAt, error) {
	if createdAt == "" {
		return CreatedAt{}, nil
	} else if _, err := time.Parse(CreatedAtLayout, createdAt); err != nil {
		return CreatedAt{}, fmt.Errorf("task creation date %q is not a %s date: %w", createdAt, CreatedAtLayout, err)
	}
	return CreatedAt{date: createdAt}, nil
}

// IsBrandNew reports whether the task's creation is not yet committed, and so
// carries no date.
func (c CreatedAt) IsBrandNew() bool { return c.date == "" }

func (c CreatedAt) String() string { return c.date }

// FlagDefault describes the default value of a feature flag: either a fixed
// boolean, or one gated on when a task was created. Users can always override
// the default by listing the flag (or its 'no_' negation) in their feature flags
// string.
type FlagDefault struct {
	value bool
	// enabledFrom, when non-empty, is the cutoff date in CreatedAtLayout form:
	// the flag defaults to true for tasks created on or after it, and false for
	// tasks created before it.
	enabledFrom string
}

// FlagEnabled is a flag that defaults to true for all tasks.
var FlagEnabled = FlagDefault{value: true}

// FlagDisabled is a flag that defaults to false for all tasks.
var FlagDisabled = FlagDefault{value: false}

// FlagEnabledForTasksCreatedAfter is a flag that defaults to true for tasks
// created on or after the given cutoff date, and false for tasks created before
// it. Use it to introduce a behavior change that must not disturb existing tasks:
// set the cutoff to the date the flag is released, so every task in existence at
// that point keeps the old behavior for the rest of its life while new tasks get
// the new one.
//
// The cutoff may be in the future, which is usually the easier thing to do: pick a
// date comfortably after the change is expected to merge, and every task created
// before it keeps the old behavior even though the code is already deployed.
//
// The cutoff is a date in CreatedAtLayout form ("2026-06-10"), matching the
// creation date stamped onto the task's built spec by the control plane, which is
// all a spec records: it carries no time of day, so a finer-grained cutoff could
// not be honored. The two dates are compared directly, and resolution is a pure
// function of the spec, yielding the same answer in every RPC, forever, with
// nothing to persist.
//
// The date is derived from the task's control-plane ID rather than recorded at
// build time, so every spec built since the field was introduced carries it,
// including those of long-running tasks. A spec built before the field existed
// has no date at all and resolves as brand new (see Resolve) — but a task can
// only reach a connector version that knows about this flag by being
// republished, which rebuilds its spec and stamps the date.
//
// Panics if date is not a valid date in CreatedAtLayout form, which is a
// connector programming error.
func FlagEnabledForTasksCreatedAfter(date string) FlagDefault {
	if _, err := time.Parse(CreatedAtLayout, date); err != nil {
		panic(fmt.Sprintf("invalid feature flag cutoff date %q: %s", date, err))
	}
	return FlagDefault{enabledFrom: date}
}

// Resolve returns the effective boolean default of the flag for a task created
// on createdAt.
//
// A brand-new task has no date yet, because its creation is only being committed
// now — so it resolves against today's date, which is the date it is about to be
// stamped with. That is what makes a cutoff in the future safe to set: such a task
// resolves the same way before and after its date is stamped, rather than flipping
// once the control plane fills the field in.
func (d FlagDefault) Resolve(createdAt CreatedAt) bool {
	if d.enabledFrom == "" {
		return d.value
	}

	date := createdAt.date
	if createdAt.IsBrandNew() {
		date = time.Now().UTC().Format(CreatedAtLayout)
	}
	// Both dates are validated, fixed-width and zero-padded, so they order
	// lexicographically and compare directly.
	return date >= d.enabledFrom
}

// ResolveFlagDefaults resolves a map of flag defaults into concrete booleans
// for a task created on createdAt, suitable for layering user-provided flags on
// top via ParseFeatureFlags.
func ResolveFlagDefaults(defaults map[string]FlagDefault, createdAt CreatedAt) map[string]bool {
	var resolved = make(map[string]bool, len(defaults))
	for k, v := range defaults {
		resolved[k] = v.Resolve(createdAt)
	}
	return resolved
}

// ResolveFlags computes the effective feature flags for a task created on
// createdAt. An explicit entry in the raw config string always wins over the
// resolved default.
func ResolveFlags(raw string, defaults map[string]FlagDefault, createdAt CreatedAt) map[string]bool {
	return ParseFeatureFlags(raw, ResolveFlagDefaults(defaults, createdAt))
}

// PendingFlagPins returns the cutoff-gated flags that are not yet explicitly
// present in the raw config string, mapped to the value they resolve to for a
// task created on createdAt. Writing these into the config leaves every task
// with an explicit record of the flags its behavior depends on, rather than one
// implied by its creation date.
//
// The pinned value is by construction the same value Resolve produces, so the
// config can never contradict the connector's behavior and the write is purely a
// record. That is what makes it safe to apply it lazily, out of band: a task
// behaves identically before and after its config catches up.
func PendingFlagPins(raw string, defaults map[string]FlagDefault, createdAt CreatedAt) map[string]bool {
	mentioned := mentionedFlags(raw)
	pins := map[string]bool{}
	for flag, d := range defaults {
		if d.enabledFrom == "" || mentioned[flag] {
			continue
		}
		pins[flag] = d.Resolve(createdAt)
	}
	return pins
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
