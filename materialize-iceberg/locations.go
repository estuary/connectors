package connector

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// LocationStyle determines the location of the Iceberg table data.
type LocationStyle int

const (
	FlatLocationStyle LocationStyle = iota
	NestedDotHashLocationStyle
)

// Iceberg catalogs are generally case-insensitive and do not allow dots in
// namespace or table names. AWS Glue is also very picky about anything other
// than alphanumerics or underscores in namespace or table names.
var pathSanitizeRegex = regexp.MustCompile("(?i)[^a-z0-9_]")

func sanitize(name string) string {
	return pathSanitizeRegex.ReplaceAllString(name, "_")
}

func sanitizePath(path ...string) []string {
	out := []string{}
	for _, p := range path {
		out = append(out, strings.ToLower(sanitize(p)))
	}

	return out
}

// sanitizeAndAppendHash adapts an input into a reasonably human-readable
// representation, sanitizing problematic characters and including a hash of the
// "original" value to guarantee a unique (with respect to the input) and
// deterministic output.
func sanitizeAndAppendHash(in ...any) string {
	strs := make([]string, 0, len(in))
	for _, i := range in {
		strs = append(strs, fmt.Sprintf("%v", i))
	}

	joined := strings.Join(strs, "_")
	sanitized := sanitize(joined)
	if len(sanitized) > 64 {
		// Limit the length of the "human readable" part of the table name to
		// something reasonable.
		sanitized = sanitized[:64]
	}

	return fmt.Sprintf("%s_%016X", sanitized, xxhash.Sum64String(joined))
}

func createNestedDotHashLocation(namespace, table string) string {
	sanitized := path.Join(sanitize(namespace), sanitize(table))
	return fmt.Sprintf("%s.%016X", sanitized, xxhash.Sum64String(sanitized))
}

// CreateLocationSuffix returns a path suffix for the namespace and table in
// the selected style.
func createLocationSuffix(namespace, table string, style LocationStyle) string {
	switch style {
	case FlatLocationStyle:
		return sanitizeAndAppendHash(namespace + "_" + table)
	case NestedDotHashLocationStyle:
		return createNestedDotHashLocation(namespace, table)
	default:
		panic("unknown location style")
	}
}
