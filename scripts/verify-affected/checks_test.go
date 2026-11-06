package main

import (
	"strings"
	"testing"
)

// TestRegistryValid guards every curated check: a non-empty predicate, a valid
// column, and a predicate that actually references the {{COL}} placeholder so the
// column setting is meaningful and the rendered SQL targets the intended column.
func TestRegistryValid(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range registry {
		if c.Name == "" {
			t.Errorf("check has empty name")
		}
		if seen[c.Name] {
			t.Errorf("duplicate check name %q", c.Name)
		}
		seen[c.Name] = true
		if strings.TrimSpace(c.Predicate) == "" {
			t.Errorf("check %q has empty predicate", c.Name)
		}
		if c.Column != "spec" && c.Column != "built_spec" {
			t.Errorf("check %q has invalid column %q", c.Name, c.Column)
		}
		if strings.TrimSpace(c.Description) == "" {
			t.Errorf("check %q has empty description (the AI reviewer relies on it)", c.Name)
		}
		if !strings.Contains(c.Predicate, "{{COL}}") {
			t.Errorf("check %q predicate should reference the {{COL}} placeholder, got %q", c.Name, c.Predicate)
		}
		if rendered := renderQuery(c.Predicate, c.Column); !strings.Contains(rendered, "c."+c.Column) {
			t.Errorf("check %q did not render against column %q", c.Name, c.Column)
		}
	}
}

func TestParseVerdict(t *testing.T) {
	cases := []struct {
		name    string
		out     string
		want    string
		wantErr bool
		notes   int
	}{
		{
			name: "clean fenced json",
			out:  "Looks fine.\n```json\n{\"verdict\": \"PASS\", \"notes\": []}\n```",
			want: verdictPass,
		},
		{
			name:  "fenced json with notes and concerns",
			out:   "Analysis...\n```json\n{\"verdict\": \"CONCERNS\", \"notes\": [\"prefix join\", \"wrong column\"]}\n```",
			want:  verdictConcerns,
			notes: 2,
		},
		{
			name: "bare object in prose",
			out:  "My verdict is {\"verdict\":\"PASS\",\"notes\":[\"ok\"]} overall.",
			want: verdictPass,
		},
		{
			name: "verdict token fallback",
			out:  "I could not produce JSON but VERDICT: CONCERNS",
			want: verdictConcerns,
		},
		{
			name:  "lowercase verdict normalized",
			out:   "```json\n{\"verdict\": \"pass\", \"notes\": []}\n```",
			want:  verdictPass,
		},
		{
			name:    "unparseable",
			out:     "I have no opinion.",
			wantErr: true,
		},
		{
			name: "prefers fenced over stray token",
			out:  "VERDICT: PASS but really:\n```json\n{\"verdict\":\"CONCERNS\",\"notes\":[\"a\"]}\n```",
			want: verdictConcerns,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, err := parseVerdict(tc.out)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got verdict %q", v.Verdict)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if v.Verdict != tc.want {
				t.Errorf("verdict = %q, want %q", v.Verdict, tc.want)
			}
			if tc.notes != 0 && len(v.Notes) != tc.notes {
				t.Errorf("notes = %d, want %d", len(v.Notes), tc.notes)
			}
			if v.Raw == "" {
				t.Errorf("Raw should retain the full output for the audit report")
			}
		})
	}
}
