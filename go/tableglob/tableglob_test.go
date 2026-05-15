package tableglob

import (
	"errors"
	"strings"
	"testing"
)

// TestCompileErrors exercises the error paths in Compile, verifying that
// each malformed pattern produces an *InvalidPatternError with a Cause
// message that names the specific syntax problem.
func TestCompileErrors(t *testing.T) {
	var cases = []struct {
		pattern   string
		causeWant string
	}{
		{"", "empty pattern"},
		{".", "empty schema component"},
		{".foo", "empty schema component"},
		{"foo.", "empty table component"},
		{"a.b.c", "more than one unescaped '.'"},
		{`foo\`, "trailing backslash"},
		{`\`, "trailing backslash"},
	}
	for _, tc := range cases {
		t.Run(tc.pattern, func(t *testing.T) {
			var _, err = Compile(tc.pattern)
			if err == nil {
				t.Fatalf("Compile(%q) returned no error, want one wrapping %q", tc.pattern, tc.causeWant)
			}
			var ipe *InvalidPatternError
			if !errors.As(err, &ipe) {
				t.Fatalf("Compile(%q) error = %v, want *InvalidPatternError", tc.pattern, err)
			}
			if ipe.Pattern != tc.pattern {
				t.Errorf("InvalidPatternError.Pattern = %q, want %q", ipe.Pattern, tc.pattern)
			}
			if ipe.Cause == nil || !strings.Contains(ipe.Cause.Error(), tc.causeWant) {
				t.Errorf("InvalidPatternError.Cause = %v, want one containing %q", ipe.Cause, tc.causeWant)
			}
		})
	}
}

// TestMatchTable exercises the runtime matching behavior across the full
// supported syntax, covering wildcards, escapes, qualified patterns, and
// edge cases like wildcards within a single name component.
func TestMatchTable(t *testing.T) {
	var cases = []struct {
		pattern string
		schema  string
		table   string
		want    bool
	}{
		// Literal unqualified
		{"users", "any", "users", true},
		{"users", "any", "users_evt", false},
		{"users", "any", "Users", false}, // case-sensitive

		// Wildcard *
		{"*", "any", "anything", true},
		{"app_*", "any", "app_users", true},
		{"app_*", "any", "users", false},
		{"*_evt", "any", "users_evt", true},
		{"*_evt", "any", "users_log", false},
		{"*foo*", "any", "xfooy", true},
		{"*foo*", "any", "bar", false},

		// Internal stars (between literals, not at the edges).
		{"foo_*_bar", "any", "foo_xyz_bar", true},
		{"foo_*_bar", "any", "foo__bar", true}, // '*' matches zero chars
		{"foo_*_bar", "any", "foo_bar", false},
		{"foo_*_bar", "any", "foo_xyz_baz", false},
		{"a_*_b_*_c", "any", "a_x_b_y_c", true},
		{"a_*_b_*_c", "any", "a__b__c", true},
		{"a_*_b_*_c", "any", "a_x_b_y", false},

		// Wildcard ?
		{"?", "any", "a", true},
		{"?", "any", "ab", false},
		{"a?c", "any", "abc", true},
		{"a?c", "any", "ac", false},

		// Qualified literal
		{"myschema.foo", "myschema", "foo", true},
		{"myschema.foo", "other", "foo", false},
		{"myschema.foo", "myschema", "bar", false},

		// Qualified with wildcards
		{"*.users", "a", "users", true},
		{"*.users", "b", "users", true},
		{"*.users", "a", "users_evt", false},
		{"a.*", "a", "users", true},
		{"a.*", "a", "events", true},
		{"a.*", "b", "users", false},
		{"a.*_evt", "a", "users_evt", true},
		{"a.*_evt", "a", "users", false},
		{"a.*_evt", "b", "users_evt", false},

		// Escapes
		{`\*`, "any", "*", true},
		{`\*`, "any", "x", false},
		{`\?`, "any", "?", true},
		{`\\`, "any", `\`, true},
		{`\.`, "any", ".", true},
		{`\.`, "any", "x", false},

		// Escaped period as literal in name component
		{`foo\.bar`, "any", "foo.bar", true},
		{`foo\.bar`, "any", "foobar", false},
		{`ms.foo\.bar`, "ms", "foo.bar", true},
		{`ms.foo\.bar`, "other", "foo.bar", false},

		// Mixed escapes and wildcards
		{`a\*b*`, "any", "a*b", true},
		{`a\*b*`, "any", "a*bcdef", true},
		{`a\*b*`, "any", "axb", false},

		// Multi-byte UTF-8 in both literals and escape sequences. '?' is
		// rune-oriented (matches one rune, not one byte) because the
		// underlying regex '.' matches one rune by default.
		{"café", "any", "café", true},
		{"café", "any", "cafe", false},
		{`\é`, "any", "é", true},
		{"caf?", "any", "café", true},
	}
	for _, tc := range cases {
		var name = tc.pattern + " | " + tc.schema + "." + tc.table
		t.Run(name, func(t *testing.T) {
			var p, err = Compile(tc.pattern)
			if err != nil {
				t.Fatalf("Compile(%q) unexpected error: %v", tc.pattern, err)
			}
			if got := p.MatchTable(tc.schema, tc.table); got != tc.want {
				t.Errorf("Compile(%q).MatchTable(%q, %q) = %v, want %v",
					tc.pattern, tc.schema, tc.table, got, tc.want)
			}
		})
	}
}
