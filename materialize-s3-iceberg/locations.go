package main

import (
	"fmt"
	"path"
	"regexp"

	"github.com/cespare/xxhash/v2"
)

// LocationStyle determines the location of the Iceberg table data in object
// storage.
//
// In contrast to materialize-iceberg, this connector uses a nested directory
// structure.
type LocationStyle int

const (
	NestedLocationStyle LocationStyle = iota
	NestedDotHashLocationStyle
)

// Iceberg catalogs are generally case-insensitive and do not allow dots in
// namespace or table names.
//
// Note: Unlike materialize-snowflake, this regex allows hyphens.
var identifierSanitizerRegexp = regexp.MustCompile(`[^\-_0-9a-zA-Z]`)

func sanitize(name string) string {
	return identifierSanitizerRegexp.ReplaceAllString(name, "_")
}

// sanitizeTable adapts a table name into a reasonably human-readable
// representation, sanitizing problematic characters from the table name and
// including a hash of the "original" value to guarantee uniqueness.
// Alphanumerics, hyphens, and underscores are left as-is and others are
// converted to underscores. S3 object names are actually very flexible in the
// characters they allow but generally characters outside of these common ones
// can cause issues with clients trying to read the named objects.
func sanitizeTable(table string) string {
	sanitized := sanitize(table)
	if len(sanitized) > 64 {
		// Limit the length of the "human readable" part of the table name to
		// something reasonable.
		sanitized = sanitized[:64]
	}

	hash := xxhash.Sum64String(table)
	return fmt.Sprintf("%s_%016X", sanitized, hash)
}

func createNestedLocation(namespace, table string) string {
	return path.Join(sanitize(namespace), sanitizeTable(table))
}

func createNestedDotHashLocation(namespace, table string) string {
	sanitized := path.Join(sanitize(namespace), sanitize(table))
	return fmt.Sprintf("%s.%016X", sanitized, xxhash.Sum64String(sanitized))
}

// CreateLocationSuffix returns a path suffix for the namespace and table in
// the selected style.
func createLocationSuffix(namespace, table string, style LocationStyle) string {
	switch style {
	case NestedLocationStyle:
		return createNestedLocation(namespace, table)
	case NestedDotHashLocationStyle:
		return createNestedDotHashLocation(namespace, table)
	default:
		panic("unknown location style")
	}
}
