package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

// aiVerdict is the structured outcome of the Claude review.
type aiVerdict struct {
	Verdict string   `json:"verdict"` // "PASS" or "CONCERNS"
	Notes   []string `json:"notes"`
	Raw     string   `json:"-"` // The full model output, for the audit report.
}

const (
	verdictPass     = "PASS"
	verdictConcerns = "CONCERNS"
)

// buildReviewPrompt assembles the logical-review request. The reviewer is asked
// to judge the query against the stated intent: the central failure mode we are
// guarding against is associating a collection with a materialization that does
// not actually read it (e.g. via a tenant-prefix heuristic instead of the
// reads_from dependency).
func buildReviewPrompt(check resolvedCheck, images []string, sql string, sum summary) string {
	var b strings.Builder
	b.WriteString(`You are the first reviewer of an automated impact-verification query for the Estuary control plane.

Your job is to validate the LOGIC of the query below and its results, then return a strict verdict. Be skeptical and specific. This review is a hard gate: a CONCERNS verdict blocks the run.

Check whether the query correctly identifies BOTH:
  (a) the collections that genuinely match the stated intent, and
  (b) ONLY the materialization tasks that actually read those collections.

Common, serious errors to look for:
  - Linking collections to tasks by a tenant/name-prefix heuristic instead of the real read dependency (reads_from). This produces false positives and is the primary error this tool exists to prevent.
  - Predicate false positives/negatives: a substring match that hits unrelated specs, or misses the intended ones.
  - Inspecting the wrong column (spec vs built_spec) for the characteristic in question.
  - Wrong spec_type / connector-image scoping.
  - Result summary that looks implausible given the intent (e.g. zero matches when many were expected, or suspiciously broad matches).

`)
	fmt.Fprintf(&b, "STATED INTENT (what counts as \"affected\"):\n%s\n\n", check.Description)
	fmt.Fprintf(&b, "PREDICATE COLUMN: %s\n", check.Column)
	fmt.Fprintf(&b, "AD-HOC PREDICATE: %v\n\n", check.AdHoc)
	fmt.Fprintf(&b, "CONNECTOR IMAGES MATCHED (the $1 parameter):\n  %s\n\n", strings.Join(images, "\n  "))
	fmt.Fprintf(&b, "RENDERED SQL:\n%s\n\n", sql)
	fmt.Fprintf(&b, "RESULT SUMMARY:\n%s\n", sum.text())
	b.WriteString(sampleRowsText(sum.Rows, 30))
	b.WriteString(`
Respond with a brief analysis, then END your message with a fenced JSON block exactly like:
` + "```json\n" + `{"verdict": "PASS", "notes": ["..."]}` + "\n```" + `
Use "PASS" only if the query soundly identifies the affected collections and the tasks that read them. Use "CONCERNS" if anything above is wrong or doubtful, listing each concern as a note.`)
	return b.String()
}

func sampleRowsText(rows []affectedRow, limit int) string {
	if len(rows) == 0 {
		return "\nSAMPLE ROWS: (none)\n"
	}
	var b strings.Builder
	b.WriteString("\nSAMPLE ROWS (task <- collection):\n")
	for i, r := range rows {
		if i >= limit {
			fmt.Fprintf(&b, "  ... and %d more\n", len(rows)-limit)
			break
		}
		fmt.Fprintf(&b, "  %s  <-  %s\n", r.Task, r.Collection)
	}
	return b.String()
}

// runReview invokes the local Claude CLI in non-interactive print mode and
// parses its verdict.
func runReview(ctx context.Context, model, prompt string) (aiVerdict, error) {
	cmd := exec.CommandContext(ctx, "claude", "-p", prompt, "--model", model)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return aiVerdict{}, fmt.Errorf("claude review failed: %w: %s", err, string(ee.Stderr))
		}
		return aiVerdict{}, fmt.Errorf("claude review failed (is the `claude` CLI installed and authenticated?): %w", err)
	}
	v, perr := parseVerdict(string(out))
	if perr != nil {
		return aiVerdict{Raw: string(out)}, perr
	}
	return v, nil
}

var (
	jsonFenceRe = regexp.MustCompile("(?s)```json\\s*(\\{.*?\\})\\s*```")
	anyObjectRe = regexp.MustCompile(`(?s)\{[^{}]*"verdict"[^{}]*\}`)
	verdictRe   = regexp.MustCompile(`(?i)VERDICT\s*[:=]\s*"?(PASS|CONCERNS)"?`)
)

// parseVerdict extracts a structured verdict from arbitrary model output. It
// tries, in order: a fenced ```json block, any bare object containing
// "verdict", then a loose `VERDICT: X` token. This tolerance keeps the hard
// gate robust against minor formatting drift in the model's reply.
func parseVerdict(out string) (aiVerdict, error) {
	out = strings.TrimSpace(out)

	tryObject := func(s string) (aiVerdict, bool) {
		var v aiVerdict
		if err := json.Unmarshal([]byte(s), &v); err != nil {
			return aiVerdict{}, false
		}
		switch strings.ToUpper(strings.TrimSpace(v.Verdict)) {
		case verdictPass, verdictConcerns:
			v.Verdict = strings.ToUpper(strings.TrimSpace(v.Verdict))
			v.Raw = out
			return v, true
		}
		return aiVerdict{}, false
	}

	if m := jsonFenceRe.FindStringSubmatch(out); m != nil {
		if v, ok := tryObject(m[1]); ok {
			return v, nil
		}
	}
	if m := anyObjectRe.FindString(out); m != "" {
		if v, ok := tryObject(m); ok {
			return v, nil
		}
	}
	if m := verdictRe.FindStringSubmatch(out); m != nil {
		return aiVerdict{Verdict: strings.ToUpper(m[1]), Raw: out}, nil
	}
	return aiVerdict{Raw: out}, fmt.Errorf("could not parse a PASS/CONCERNS verdict from the review output")
}
