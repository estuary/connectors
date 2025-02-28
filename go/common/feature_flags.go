package common

import "strings"

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
