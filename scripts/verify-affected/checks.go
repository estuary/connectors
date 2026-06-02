package main

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
)

// A Check describes a class of collection that is affected by some connector
// feature. The Predicate is a SQL boolean expression evaluated against the
// collection's `live_specs` row (aliased `c` in the query). Within a predicate
// the token `{{COL}}` is substituted with the configured column reference
// (`c.spec` or `c.built_spec`) so a single predicate can target either the
// user-authored spec or the fully-built/inferred spec. The token is delimited
// with braces so it cannot collide with the substring of a SQL identifier or
// string literal (e.g. a LIKE pattern matching "PROTOCOL").
//
// Registry checks are the trusted, snapshot-tested path: their predicates are
// fixed in source and covered by tests, so a developer who selects a registry
// check cannot introduce the kinds of logic errors that plague hand-written
// ad-hoc queries.
type Check struct {
	Name        string
	Description string // Human intent; also handed to the AI reviewer as the stated goal.
	Predicate   string // SQL boolean over alias `c`, using the `{{COL}}` placeholder for the column.
	Column      string // Default column the predicate inspects: "spec" or "built_spec".
}

// registry holds the curated, tested checks. New checks are added here in a PR
// alongside a snapshot test of their rendered SQL.
var registry = []Check{
	{
		Name: "uses-enum",
		Description: "Collections whose schema constrains one or more fields with a JSON Schema " +
			"`enum`. Use this when a materialization feature changes how enum-typed fields are " +
			"handled (column typing, validation, etc.).",
		// The built spec carries the fully-inferred/validated schema the
		// materialization actually sees, so enum detection is most reliable there.
		Predicate: `{{COL}}::text LIKE '%"enum":%'`,
		Column:    "built_spec",
	},
}

func lookupCheck(name string) (Check, bool) {
	for _, c := range registry {
		if c.Name == name {
			return c, true
		}
	}
	return Check{}, false
}

func registryListing() string {
	var b strings.Builder
	b.WriteString("Available registry checks:\n\n")
	for _, c := range registry {
		fmt.Fprintf(&b, "  %s (column: %s)\n    %s\n    predicate: %s\n\n",
			c.Name, c.Column, c.Description, c.Predicate)
	}
	b.WriteString("Or supply an ad-hoc predicate with --check-sql=\"<expr over alias c>\".\n")
	return b.String()
}

// renderQuery produces the complete, fixed verification query. The only varying
// parts are the predicate and the column it inspects; everything else -- the
// reads_from join that links collections to the materializations that actually
// read them, the spec_type filters, and the parameterized connector-image
// filter -- is constant and snapshot-tested. The connector image names are NOT
// interpolated here; they are bound as the $1 query parameter.
func renderQuery(predicate, column string) string {
	pred := strings.ReplaceAll(predicate, "{{COL}}", "c."+column)
	return fmt.Sprintf(`SELECT
    m.catalog_name        AS task,
    m.connector_image_name AS image,
    c.catalog_name        AS collection
FROM live_specs m
JOIN live_specs c ON c.catalog_name = ANY(m.reads_from)
WHERE m.spec_type = 'materialization'
  AND m.connector_image_name = ANY($1)
  AND c.spec_type = 'collection'
  AND (%s)
ORDER BY m.catalog_name, c.catalog_name;`, pred)
}

// listVariants resolves a connector short name (e.g. "materialize-postgres")
// into the full set of `ghcr.io/estuary/*` image names that should be matched,
// including any variants declared in the connector's VARIANTS file. This mirrors
// the resolution used by scripts/list-tasks so that variant tasks are never
// silently missed. A short name without a "/" is expanded; a value that already
// looks like an image URL is used verbatim by the caller.
func listVariants(connectorName string) ([]string, error) {
	var variantsURL = fmt.Sprintf("https://raw.githubusercontent.com/estuary/connectors/refs/heads/main/%s/VARIANTS", connectorName)
	var variants = []string{fmt.Sprintf("ghcr.io/estuary/%s", connectorName)}

	resp, err := http.Get(variantsURL)
	if err != nil {
		return nil, fmt.Errorf("error fetching variants list: %w", err)
	}
	defer resp.Body.Close()

	// The primary image is the only variant when no VARIANTS file exists.
	if resp.StatusCode == http.StatusNotFound {
		return variants, nil
	} else if resp.StatusCode/100 != 2 {
		// http.Get does not error on 4xx/5xx, so guard explicitly: a 403, 5xx,
		// or redirect page must not be silently parsed as a list of variants.
		return nil, fmt.Errorf("error fetching variants list: unexpected status %s", resp.Status)
	}

	bs, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error fetching variants list: %w", err)
	}
	for _, v := range strings.Split(string(bs), "\n") {
		if v = strings.TrimSpace(v); v != "" {
			variants = append(variants, fmt.Sprintf("ghcr.io/estuary/%s", v))
		}
	}
	return variants, nil
}

// resolveImages returns the image-name set to filter on. A connector value
// containing "/" is treated as an explicit image URL; otherwise it is expanded
// into its variant set.
func resolveImages(connector string) ([]string, error) {
	if strings.Contains(connector, "/") {
		return []string{connector}, nil
	}
	return listVariants(connector)
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
